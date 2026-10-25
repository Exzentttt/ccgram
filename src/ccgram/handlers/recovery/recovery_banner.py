"""Dead-window recovery banner UX flow.

Owns the banner the user sees in three situations:
  - a tmux window died proactively (``dead`` mode),
  - the user invoked ``/restore`` (``restore`` mode),
  - the user opened the resume picker (``resume`` mode).

Public surface:
  - :class:`RecoveryBanner` / :data:`RecoveryMode`
  - :func:`render_banner`, :func:`build_recovery_keyboard`
  - :func:`_create_and_bind_window` (the window replacement, driven through a
    :class:`~.recovery_ui.RecoveryUi`) and :func:`_replace_from_callback` (its
    button-tap entry, used by :mod:`resume_picker` after a session is picked)
  - the per-button handlers ``_handle_back/_fresh/_continue/_resume/
    _send_empty_state/_handle_browse/_handle_cancel``

The dispatcher in :mod:`recovery_callbacks` routes button taps here. The
sibling cycle with :mod:`resume_picker` is one-way at the top level
(this module imports from the picker for ``scan_sessions_for_cwd``); the
reverse direction lives behind a lazy import inside the picker.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, replace
from functools import partial
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import structlog
from telegram import (
    Bot,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.error import RetryAfter, TelegramError

from ... import window_query
from ...providers import get_provider, get_provider_for_window
from ...session import session_manager
from ...session_map import session_map_sync
from ...telegram_client import PTBTelegramClient
from ...telegram_rate_limiter import retry_after_seconds
from ...thread_router import thread_router
from ...multiplexer import multiplexer as tmux_manager
from ...multiplexer.base import canonical_window_id
from ..telegram_origin import send_telegram_to_window
from ...session_protocol.telegram import TelegramSessionAdapter, adapter_for
from ...session_protocol.models import ProtocolError
from ...window_state_store import CCGRAM_CREATED_WINDOW_ORIGIN
from ..callback_data import (
    CB_RECOVERY_BACK,
    CB_RECOVERY_BROWSE,
    CB_RECOVERY_CANCEL,
    CB_RECOVERY_CONTINUE,
    CB_RECOVERY_FRESH,
    CB_RECOVERY_RESUME,
)
from ..callback_helpers import get_thread_id
from ..callback_tokens import compact_callback_data
from ..messaging_pipeline.message_sender import safe_edit, safe_send
from ..status.topic_emoji import format_topic_name_for_mode
from ..user_state import (
    PENDING_THREAD_ID,
    PENDING_THREAD_TEXT,
    RECOVERY_SESSIONS,
    RECOVERY_WINDOW_ID,
)
from .recovery_callbacks import _clear_recovery_state
from .recovery_ui import RecoveryUi, ReplacementRequest
from .resume_picker import (
    _build_empty_resume_keyboard,
    _build_resume_picker_keyboard,
    scan_sessions_for_cwd,
)

if TYPE_CHECKING:
    from telegram.ext import ContextTypes

    from ...providers.base import AgentProvider

logger = structlog.get_logger()

RecoveryMode = Literal["dead", "restore", "resume"]


@dataclass(frozen=True)
class RecoveryBanner:
    """Inputs for the unified recovery banner.

    The banner is the dead-window notification ccgram shows in three
    situations: a window died proactively (``dead``), the user invoked
    /restore (``restore``), or the user opened the resume picker
    (``resume``). All three flow through ``render_banner`` so the keyboard,
    subtitle, and copy stay consistent across entry points.
    """

    chat_id: int
    thread_id: int
    window_id: str
    mode: RecoveryMode
    provider: str | None = None
    display: str = ""
    cwd: str = ""


def _validate_recovery_state(
    data_suffix: str,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> tuple[int, str] | None:
    """Validate common recovery preconditions.

    Supports two paths:
      1. Text-handler path: PENDING_THREAD_ID and RECOVERY_WINDOW_ID in user_data.
      2. Proactive notification path: no user_data state, validate via binding.

    Returns ``(thread_id, old_window_id)`` on success, or ``None`` on
    failure (caller should return early and call ``query.answer``).
    """
    thread_id = get_thread_id(update)
    if thread_id is None:
        return None

    user_id = update.effective_user.id if update.effective_user else None
    if user_id is None:
        return None

    pending_tid = (
        context.user_data.get(PENDING_THREAD_ID) if context.user_data else None
    )
    stored_wid = (
        context.user_data.get(RECOVERY_WINDOW_ID) if context.user_data else None
    )

    if pending_tid is not None:
        if thread_id != pending_tid or stored_wid != data_suffix:
            return None
    else:
        bound_wid = thread_router.get_window_for_thread(user_id, thread_id)
        if bound_wid != data_suffix:
            return None
        if context.user_data is not None:
            context.user_data[PENDING_THREAD_ID] = thread_id
            context.user_data[RECOVERY_WINDOW_ID] = data_suffix

    return thread_id, data_suffix


def render_banner(banner: RecoveryBanner) -> tuple[str, InlineKeyboardMarkup]:
    """Render the recovery banner text and inline keyboard.

    Returns the message body and a :class:`InlineKeyboardMarkup` ready to
    pass to ``safe_reply`` / ``rate_limit_send_message``. The keyboard is
    the provider-aware action keyboard from :func:`build_recovery_keyboard`
    in every mode — modes only differ in the surrounding copy so the user
    knows whether the banner appeared on its own or in response to a
    request.
    """

    keyboard = build_recovery_keyboard(banner.window_id)
    help_text = _recovery_help_text(banner.window_id)
    cwd_line = f"\n\U0001f4c2 `{banner.cwd}`" if banner.cwd else ""
    label = banner.display or banner.window_id

    if banner.mode == "restore":
        title = f"\U0001f504 Restore `{label}`."
        prompt = f"Choose how to continue.\n{help_text}"
    elif banner.mode == "resume":
        title = f"⏪ Resume `{label}`."
        prompt = f"Pick a session below or use the menu.\n{help_text}"
    else:
        title = f"⚠ Session `{label}` ended."
        prompt = f"Tap a button or send a message to recover.\n{help_text}"

    text = f"{title}{cwd_line}\n\n{prompt}"
    return text, keyboard


def _recovery_help_text(window_id: str) -> str:
    """Return a one-line subtitle explaining the available recovery actions.

    Mirrors the keyboard layout in ``build_recovery_keyboard`` so users can
    read what each button does without trial and error. Buttons hidden by
    the active provider's capabilities are omitted from the subtitle too.
    """

    caps = get_provider_for_window(
        window_id, provider_name=window_query.get_window_provider(window_id)
    ).capabilities
    parts = ["Start fresh"]
    if caps.supports_continue:
        parts.append("Continue last session")
    if caps.supports_resume and caps.supports_resume_picker:
        parts.append("Resume from list")
    return " · ".join(parts)


def build_recovery_keyboard(window_id: str) -> InlineKeyboardMarkup:
    """Build inline keyboard for dead window recovery options.

    Buttons for Continue and Resume are only shown when the active provider
    declares support for those capabilities.
    """

    caps = get_provider_for_window(
        window_id, provider_name=window_query.get_window_provider(window_id)
    ).capabilities
    options: list[InlineKeyboardButton] = [
        InlineKeyboardButton(
            "\U0001f195 Fresh",
            callback_data=compact_callback_data(
                CB_RECOVERY_FRESH, f"{CB_RECOVERY_FRESH}{window_id}", window_id
            ),
        ),
    ]
    if caps.supports_continue:
        options.append(
            InlineKeyboardButton(
                "▶ Continue",
                callback_data=compact_callback_data(
                    CB_RECOVERY_CONTINUE,
                    f"{CB_RECOVERY_CONTINUE}{window_id}",
                    window_id,
                ),
            )
        )
    if caps.supports_resume and caps.supports_resume_picker:
        options.append(
            InlineKeyboardButton(
                "⏪ Resume",
                callback_data=compact_callback_data(
                    CB_RECOVERY_RESUME, f"{CB_RECOVERY_RESUME}{window_id}", window_id
                ),
            )
        )
    return InlineKeyboardMarkup(
        [
            options,
            [InlineKeyboardButton("✖ Cancel", callback_data=CB_RECOVERY_CANCEL)],
        ]
    )


async def _stale_recovery_offer(old_window_id: str) -> str | None:
    """Why this recovery offer must not be acted on, or None to proceed.

    Every Fresh / Continue / Resume route converges on the replacement below,
    and the banner offering them may have been drawn from an ambiguous lookup
    minutes ago. The replacement unbinds before it creates, so a stale offer
    leaves the old terminal running with its routing handed to a new window.

    One confirmed read answers both questions — does it exist, and is what
    holds it still an agent — because a presence check followed by a second
    lookup is a second chance to fail.
    """
    if not old_window_id:
        return None

    # Lazy: importing the reconciliation seam at module load forms a cycle.
    from ...multiplexer.reconciliation import window_snapshot

    # Lazy: same cycle through provider/session initialization.
    from ..telegram_origin import agent_origin_returned_to_shell

    confirmed, window = await window_snapshot(old_window_id)
    if not confirmed:
        return (
            "\u26a0 Could not reach the multiplexer, so nothing was changed. "
            "Try again in a moment."
        )
    if window is None:
        # Confirmed gone: recovery is exactly right.
        return None
    if await agent_origin_returned_to_shell(old_window_id, window):
        # Alive, but the agent exited and left a shell — the case the text
        # handler deliberately offers recovery for.
        return None
    return "That session is running again, so it was left alone."


def _is_definitive_create_failure(message: str) -> bool:
    """Return whether a failed create response proves no target exists."""
    normalized = message.casefold()
    return any(
        marker in normalized
        for marker in (
            "directory does not exist",
            "not a directory",
            "selected workspace no longer exists",
            "requires a sessionful agent",
            "does not create worktrees",
        )
    )


async def _await_creation_result(creation):  # noqa: ANN001
    """Wait for creation while retaining a result delivered with cancellation."""
    task = asyncio.create_task(creation)
    try:
        return await asyncio.shield(task), False
    except asyncio.CancelledError:
        return await task, True


def _mark_provisioning_uncertain(claim_id: str) -> None:
    """Release runtime ownership while retaining durable recovery evidence."""
    if not claim_id:
        return
    try:
        thread_router.mark_provisioning_uncertain(claim_id)
    except KeyError:
        logger.warning("Provisioning claim disappeared while marking uncertain")
        return
    try:
        session_manager.flush_state()
    except BaseException:  # noqa: BLE001 (called from except blocks about to re-raise the original failure: a persistence failure must be logged, never replace that exception)
        logger.warning("Could not persist uncertain provisioning claim")


def _abort_provisioning_claim(claim_id: str) -> None:
    """Release a claim whose backend operation definitely never started."""
    if not claim_id:
        return
    try:
        thread_router.abort_topic_provisioning(
            claim_id,
            target_confirmed_absent=True,
        )
    except KeyError:
        logger.warning("Provisioning claim disappeared while aborting")
        return
    try:
        session_manager.flush_state()
    except BaseException:  # noqa: BLE001 (the claim is already released in memory; a failed flush is logged, never raised: on except paths it would replace the original failure, on the normal failure path it would skip the user-facing failure message)
        logger.warning("Could not persist aborted provisioning claim")


def _clear_failed_target_state(window_id: str) -> None:
    """Clear state scoped only to a failed replacement target."""
    # Lazy: cleanup/state registries import recovery and polling modules.
    from ...session_map import session_map_prefix

    # Lazy: cleanup/state registries import recovery and polling modules.
    from ...topic_state_registry import topic_state

    # Lazy: cleanup/state registries import recovery and polling modules.
    from ..callback_tokens import revoke_window_tokens

    topic_state.clear_window(window_id)
    topic_state.clear_qualified(f"{session_map_prefix()}{window_id}")
    revoke_window_tokens(window_id)


def _follow_provisioning_supersession(window_id: str, *, claim_id: str) -> str:
    """Persist a backend target identity that changed during hook startup."""
    current = window_query.resolve_window_alias(window_id)
    if current == window_id:
        return window_id
    # Lazy: topic orchestration imports polling and topic handlers.
    from ..topics import topic_orchestration

    if canonical_window_id(current) == canonical_window_id(window_id):
        return current
    if claim_id:
        try:
            thread_router.attach_provisioning_target(claim_id, current)
            session_manager.flush_state()
        except BaseException:
            _mark_provisioning_uncertain(claim_id)
            raise
    topic_orchestration.register_pending_creation(current)
    topic_orchestration.clear_pending_creation(window_id)
    logger.info("Recovery target superseded: %s -> %s", window_id, current)
    return current


async def _cleanup_failed_topic(
    user_id: int,
    thread_id: int,
    chat_id: int,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    """Delete an exact topic after a target-for-topic failure."""
    await _cleanup_failed_topic_with(
        user_id, thread_id, chat_id, context.bot, context.user_data
    )


async def _cleanup_failed_topic_with(
    user_id: int,
    thread_id: int,
    chat_id: int,
    bot: Bot,
    user_data: dict[str, Any] | None,
) -> None:
    """Delete an exact topic after a target-for-topic failure."""
    # Lazy: cleanup imports the recovery state registry.
    from ..cleanup import clear_topic_state

    # Lazy: topic deletion imports cleanup and polling state.
    from ..topics.topic_deletion import cleanup_retired_topic

    retired = next(
        (
            topic
            for topic in thread_router.iter_retired_topics()
            if topic.user_id == user_id
            and topic.chat_id == chat_id
            and topic.thread_id == thread_id
        ),
        None,
    )
    if retired is None:
        return

    async def clear_state_before_delete() -> None:
        await clear_topic_state(
            user_id,
            thread_id,
            PTBTelegramClient(bot),
            user_data,
            window_id=retired.target_id,
            chat_id=chat_id,
            window_dead=True,
        )

    await cleanup_retired_topic(
        PTBTelegramClient(bot),
        retired,
        router=thread_router,
        before_delete=clear_state_before_delete,
    )


class CallbackUi:
    """The :class:`RecoveryUi` of a recovery button tap: edits its message."""

    def __init__(
        self, query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        self._query = query
        self._context = context

    @property
    def user_data(self) -> dict[str, Any] | None:
        return self._context.user_data

    async def edit(self, text: str, **kwargs: Any) -> None:
        await safe_edit(self._query, text, **kwargs)

    async def answer(self, text: str) -> None:
        await self._query.answer(text)

    def pending_text(self) -> str | None:
        return self.user_data.get(PENDING_THREAD_TEXT) if self.user_data else None

    def clear_state(self) -> None:
        _clear_recovery_state(self.user_data)

    def note_rate_limit(self, retry_after: float) -> None:
        """A button tap has nobody to stop; the rename failure is already logged."""


class _Replacement:
    """One window replacement for one topic; owns its provisioning claim.

    The claim is created by the first step and must be released, aborted or
    marked uncertain by whichever later step ends the run, so the steps share
    it here instead of threading it through every call.
    """

    def __init__(
        self,
        request: ReplacementRequest,
        ui: RecoveryUi,
        bot: Bot,
        get_adapter: Callable[[], TelegramSessionAdapter],
    ) -> None:
        self.req = request
        self.ui = ui
        self.bot = bot
        self.get_adapter = get_adapter
        self.chat_id: int = (
            request.chat_id
            if isinstance(request.chat_id, int)
            else thread_router.resolve_chat_id(request.user_id, request.thread_id)
        )
        self.claim_id = ""  # "" until the topic claim is begun
        self.preserve_topic = bool(request.old_window_id)

    async def run(self) -> bool:
        """Replace the dead window. Returns True on success, False on failure."""
        if not await self._claim_topic():
            return False
        if not await self._offer_still_valid():
            return False
        provider, approval_mode = self._provider_and_mode()
        launch = await self._launch(provider, approval_mode)
        if not launch.success:
            return await self._launch_failed(launch)
        await self._attach_target(launch)
        wid, found = await self._await_registration(launch.wid, provider)
        if not found:
            return await self._not_registered(wid)
        if not await self._commit(wid, launch.wname, provider, approval_mode):
            return False
        await self._finalize(wid, launch, approval_mode)
        return True

    async def _claim_topic(self) -> bool:
        provisioning_kwargs = {
            "thread_id": self.req.thread_id,
            "kind": "replacement" if self.req.old_window_id else "target_for_topic",
        }
        if self.req.old_window_id:
            provisioning_kwargs["previous_target_id"] = self.req.old_window_id
        try:
            claim = thread_router.begin_topic_provisioning(
                self.req.user_id,
                self.chat_id,
                **provisioning_kwargs,
            )
        except (TypeError, ValueError) as exc:
            await self.ui.edit(f"❌ {exc}", reply_markup=None)
            await self.ui.answer("Failed")
            return False
        self.claim_id = claim.claim_id
        # Persist the exact topic claim before the first multiplexer read.
        try:
            session_manager.flush_state()
        except BaseException:
            _abort_provisioning_claim(self.claim_id)
            raise
        return True

    async def _offer_still_valid(self) -> bool:
        # Every Fresh / Continue / Resume route converges here, and the banner
        # offering them may have been drawn from an ambiguous lookup minutes
        # ago. Keep the old binding in place until a replacement has committed.
        if self.req.offer_verified:
            return True
        try:
            refusal = await _stale_recovery_offer(self.req.old_window_id)
        except BaseException:
            # The replacement request has not started, so release only its
            # claim; the previous binding remains untouched for retry/recovery.
            _abort_provisioning_claim(self.claim_id)
            raise
        if refusal is None:
            return True
        await _finish_failed_provisioning(
            self,
            target_id=None,
            failure_message=refusal,
            target_confirmed_absent=True,
        )
        await self.ui.edit(refusal, reply_markup=None)
        return False

    def _provider_and_mode(self) -> tuple[AgentProvider, Literal["normal", "yolo"]]:
        provider, inherited = self._inherited_provider_and_mode()
        return provider, self.req.approval_mode or inherited

    def _inherited_provider_and_mode(
        self,
    ) -> tuple[AgentProvider, Literal["normal", "yolo"]]:
        old_window_id = self.req.old_window_id
        provider_name = self.req.provider_name
        if old_window_id:
            old_view = window_query.view_window(old_window_id)
            provider = get_provider_for_window(
                old_window_id,
                provider_name=provider_name
                or (old_view.provider_name if old_view else None),
            )
            yolo = old_view is not None and old_view.approval_mode == "yolo"
            return provider, "yolo" if yolo else "normal"
        if provider_name:
            return get_provider_for_window("", provider_name=provider_name), "normal"
        return get_provider(), "normal"

    async def _launch(
        self, provider: AgentProvider, approval_mode: Literal["normal", "yolo"]
    ) -> _Launch:
        cancelled = False
        definitive = False
        try:
            created, cancelled = await _await_creation_result(
                self.get_adapter().create(
                    {"provider": provider.capabilities.name, "directory": self.req.cwd},
                    approval_mode=approval_mode,
                    resume_id=self.req.resume_id,
                    continue_session=self.req.continue_session,
                )
            )
            launch = _Launch(True, f"Created session '{created.title}'")
            launch.wname, launch.wid = created.title, created.target_id
        except ProtocolError as exc:
            if exc.status >= HTTPStatus.INTERNAL_SERVER_ERROR:
                _mark_provisioning_uncertain(self.claim_id)
                raise
            definitive = True
            launch = _Launch(False, exc.message)
        except BaseException:
            _mark_provisioning_uncertain(self.claim_id)
            raise
        if launch.success and not launch.wid:
            launch.success = False
            launch.message = "Backend returned no target ID"
        launch.cancelled = cancelled
        launch.definitive_failure = definitive
        return launch

    async def _launch_failed(self, launch: _Launch) -> bool:
        message = launch.message
        if launch.definitive_failure or _is_definitive_create_failure(message):
            await _finish_failed_provisioning(
                self,
                target_id=None,
                failure_message=message,
                target_confirmed_absent=launch.definitive_failure,
            )
        else:
            _mark_provisioning_uncertain(self.claim_id)
        await self.ui.edit(f"❌ {message}")
        self.ui.clear_state()
        await self.ui.answer("Failed")
        if launch.cancelled:
            raise asyncio.CancelledError
        return False

    async def _attach_target(self, launch: _Launch) -> None:
        try:
            thread_router.attach_provisioning_target(self.claim_id, launch.wid)
            session_manager.flush_state()
        except BaseException:
            _mark_provisioning_uncertain(self.claim_id)
            raise
        # Lazy: topic orchestration imports polling and topic handlers.
        from ..topics import topic_orchestration

        topic_orchestration.register_pending_creation(launch.wid)
        if launch.cancelled:
            await _finish_failed_provisioning(
                self, target_id=launch.wid, failure_message="creation cancelled"
            )
            raise asyncio.CancelledError

    async def _await_registration(
        self, wid: str, provider: AgentProvider
    ) -> tuple[str, bool]:
        """Wait for the hook to register the target; return ``(wid, found)``."""
        found = True
        if provider.capabilities.supports_hook:
            try:
                found = await session_map_sync.wait_for_session_map_entry(
                    wid,
                    resolve_window_id=window_query.resolve_window_alias,
                )
            except BaseException as exc:
                wid = _follow_provisioning_supersession(wid, claim_id=self.claim_id)
                await _finish_failed_provisioning(
                    self, target_id=wid, failure_message=str(exc)
                )
                raise
        return _follow_provisioning_supersession(wid, claim_id=self.claim_id), found

    async def _not_registered(self, wid: str) -> bool:
        # A hook-registration timeout does not prove that the new terminal is
        # dead. Re-read presence before deciding whether the target may be
        # aborted; live or unknown targets remain quarantined for recovery.
        # Lazy: reconciliation imports the active multiplexer backend.
        from ...multiplexer.reconciliation import window_presence

        # Lazy: topic orchestration imports polling and topic handlers.
        from ..topics import topic_orchestration

        try:
            presence = await window_presence(wid, tmux_manager)
        except BaseException as exc:
            _mark_provisioning_uncertain(self.claim_id)
            topic_orchestration.clear_pending_creation(wid)
            if isinstance(exc, asyncio.CancelledError):
                raise
            message = (
                "Could not verify the new session; ccgram kept it quarantined "
                "for recovery."
            )
            await self.ui.edit(f"⚠ {message}")
            await self.ui.answer("Still starting")
            return False
        if presence is False:
            await _finish_failed_provisioning(
                self,
                target_id=wid,
                failure_message="session target is gone",
                target_confirmed_absent=True,
            )
            await self.ui.edit("❌ Session did not register with ccgram and is gone")
            self.ui.clear_state()
            await self.ui.answer("Failed")
            return False

        _mark_provisioning_uncertain(self.claim_id)
        topic_orchestration.clear_pending_creation(wid)
        message = (
            "Session is still starting; ccgram kept it quarantined for recovery. "
            "Try again after it finishes registering."
        )
        await self.ui.edit(f"⚠ {message}")
        await self.ui.answer("Still starting")
        return False

    async def _commit(
        self,
        wid: str,
        wname: str,
        provider: AgentProvider,
        approval_mode: Literal["normal", "yolo"],
    ) -> bool:
        session_manager.set_window_origin(wid, CCGRAM_CREATED_WINDOW_ORIGIN)
        session_manager.set_window_provider(wid, provider.capabilities.name)
        session_manager.set_window_approval_mode(wid, approval_mode)
        # Lazy: topic orchestration imports polling and topic handlers.
        from ..topics import topic_orchestration

        try:
            committed = thread_router.commit_topic_provisioning(
                self.claim_id,
                window_name=wname,
            )
            session_manager.flush_state()
        except BaseException:
            _mark_provisioning_uncertain(self.claim_id)
            topic_orchestration.clear_pending_creation(wid)
            raise
        if not committed:
            _mark_provisioning_uncertain(self.claim_id)
            topic_orchestration.clear_pending_creation(wid)
            await self.ui.edit("❌ Session could not be bound to this topic")
            self.ui.clear_state()
            await self.ui.answer("Failed")
            return False
        return True

    async def _finalize(
        self,
        wid: str,
        launch: _Launch,
        approval_mode: Literal["normal", "yolo"],
    ) -> None:
        # The replacement is committed. The old binding was kept intact until
        # this point, so every failed path above leaves it usable.
        # Lazy: topic orchestration imports polling and topic handlers.
        from ..topics import topic_orchestration

        topic_orchestration.clear_pending_creation(wid)
        # Lazy: polling_state → recovery_banner via callback_registry side effects.
        from ..polling.polling_state import lifecycle_strategy

        user_id, thread_id = self.req.user_id, self.req.thread_id
        lifecycle_strategy.clear_dead_notification(user_id, thread_id)

        client = PTBTelegramClient(self.bot)
        try:
            await client.edit_forum_topic(
                chat_id=thread_router.resolve_chat_id(user_id, thread_id),
                message_thread_id=thread_id,
                name=format_topic_name_for_mode(launch.wname, approval_mode),
            )
        except TelegramError as e:
            if isinstance(e, RetryAfter):
                self.ui.note_rate_limit(retry_after_seconds(e))
            logger.debug("Failed to rename topic: %s", e)

        await self.ui.edit(f"✅ {launch.message}\n\n{self.req.success_label}")

        pending_text = self.ui.pending_text()
        self.ui.clear_state()
        if pending_text:
            await self._forward_pending_text(client, wid, pending_text)
        await self.ui.answer("Created")

    async def _forward_pending_text(
        self, client: PTBTelegramClient, wid: str, text: str
    ) -> None:
        user_id, thread_id = self.req.user_id, self.req.thread_id
        send_ok, send_msg = await send_telegram_to_window(
            user_id,
            wid,
            thread_id,
            text,
            self.req.chat_id,
            adapter=self.get_adapter(),
        )
        if send_ok:
            return
        logger.warning(
            "Failed to forward pending text to window %s (user %s): %s",
            wid,
            user_id,
            send_msg,
        )
        await safe_send(
            client,
            thread_router.resolve_chat_id(user_id, thread_id),
            f"❌ Failed to send pending message: {send_msg}",
            message_thread_id=thread_id,
        )


@dataclass
class _Launch:
    """Outcome of asking the backend to create the replacement target."""

    success: bool
    message: str
    wname: str = ""
    wid: str = ""
    cancelled: bool = False
    definitive_failure: bool = False


async def _target_confirmed_absent(
    rep: _Replacement,
    target_id: str | None,
    failure_message: str,
    confirmed: bool,
) -> bool | None:
    """Whether the failed target is provably gone; None leaves the claim uncertain."""
    if target_id is None:
        if confirmed or _is_definitive_create_failure(failure_message):
            return True
        _mark_provisioning_uncertain(rep.claim_id)
        return None
    if confirmed:
        return True
    try:
        absent, _ = await rep.get_adapter().close(target_id)
    except BaseException:  # noqa: BLE001 (an unreachable backend proves nothing about the target, so the claim is kept uncertain instead of aborted)
        _mark_provisioning_uncertain(rep.claim_id)
        return None
    return bool(absent)


async def _finish_failed_provisioning(
    rep: _Replacement,
    *,
    target_id: str | None,
    failure_message: str = "",
    target_confirmed_absent: bool = False,
) -> bool:
    """Abort a proven-absent target or quarantine an uncertain target."""
    absent = await _target_confirmed_absent(
        rep, target_id, failure_message, target_confirmed_absent
    )
    if absent is None:
        return False

    # Lazy: topic orchestration imports polling and topic handlers.
    from ..topics import topic_orchestration

    if absent:
        _abort_provisioning_claim(rep.claim_id)
        if target_id is not None:
            topic_orchestration.clear_pending_creation(target_id)
            _clear_failed_target_state(target_id)
        if not rep.preserve_topic:
            await _cleanup_failed_topic_with(
                rep.req.user_id,
                rep.req.thread_id,
                rep.chat_id,
                rep.bot,
                rep.ui.user_data,
            )
        return True

    _mark_provisioning_uncertain(rep.claim_id)
    topic_orchestration.clear_pending_creation(target_id or "")
    if failure_message:
        logger.warning("Provisioned target remains quarantined: %s", failure_message)
    return False


async def _create_and_bind_window(
    request: ReplacementRequest,
    ui: RecoveryUi,
    bot: Bot,
    get_adapter: Callable[[], TelegramSessionAdapter],
) -> bool:
    """Create a new window for the topic, bind it, rename the topic.

    Forwards any text that arrived while the window was dead. Returns True on
    success, False on failure. ``request.provider_name`` overrides the
    provider inherited from ``request.old_window_id``: a resume pick needs it,
    because the picker widens to every provider precisely when the old window
    has no provider name, so inheriting from it would launch the config
    default with another agent's resume args.

    ``get_adapter`` is called where each backend call is made, not once up
    front, so a missing session service fails at the same step as before.
    """
    return await _Replacement(request, ui, bot, get_adapter).run()


async def _replace_from_callback(
    query: CallbackQuery,
    context: ContextTypes.DEFAULT_TYPE,
    request: ReplacementRequest,
) -> bool:
    """Run the replacement for a recovery button tap."""
    chat = query.message.chat if query.message else None
    return await _create_and_bind_window(
        replace(request, chat_id=getattr(chat, "id", None)),
        CallbackUi(query, context),
        context.bot,
        partial(adapter_for, context.bot_data),
    )


def _cwd_for_window(window_id: str) -> str:
    """Return the bound cwd for ``window_id`` or empty string."""
    view = window_query.view_window(window_id)
    return view.cwd if view else ""


async def _recovery_cwd_or_report(
    query: CallbackQuery,
    window_id: str,
    context: ContextTypes.DEFAULT_TYPE,
) -> str | None:
    """Return the recovery cwd, or report which of two failures happened.

    Fresh/Continue/Resume all need the directory, and both ways of not having
    it used to share one message that claimed the directory was gone (#176).
    A missing window state means the directory is *unknown*, which is a
    different problem with a different way out: Browse still works without
    state, so offer it rather than ending the flow on a false statement about
    the filesystem.
    """
    cwd = _cwd_for_window(window_id)
    if not cwd:
        await safe_edit(
            query,
            "⚠ This topic's session state is gone, so its folder is unknown.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "\U0001f5c2 Browse other projects",
                            callback_data=compact_callback_data(
                                CB_RECOVERY_BROWSE,
                                f"{CB_RECOVERY_BROWSE}{window_id}",
                                window_id,
                            ),
                        )
                    ]
                ]
            ),
        )
        # Deliberately not cleared: Browse re-validates against this state.
        await query.answer("State gone")
        return None
    if not Path(cwd).is_dir():
        await safe_edit(query, "\u274c Directory no longer exists.")
        _clear_recovery_state(context.user_data)
        await query.answer("Project gone")
        return None
    return cwd


async def _handle_back(
    query: CallbackQuery,
    data: str,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    """Handle CB_RECOVERY_BACK: return to the recovery options menu."""
    window_id = data[len(CB_RECOVERY_BACK) :]
    validated = _validate_recovery_state(window_id, update, context)
    if validated is None:
        await query.answer("Stale recovery (topic mismatch)", show_alert=True)
        return
    thread_id, _ = validated
    if query.message is None or query.message.chat is None:
        await query.answer("Chat unavailable", show_alert=True)
        return
    chat_id = query.message.chat.id
    display = thread_router.get_display_name(window_id) or window_id
    banner = RecoveryBanner(
        chat_id=chat_id,
        thread_id=thread_id,
        window_id=window_id,
        mode="restore",
        provider=window_query.get_window_provider(window_id),
        display=display,
        cwd=_cwd_for_window(window_id),
    )
    text, kb = render_banner(banner)
    await safe_edit(query, text, reply_markup=kb)
    await query.answer()


async def _handle_fresh(
    query: CallbackQuery,
    user_id: int,
    data: str,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    """Handle CB_RECOVERY_FRESH: create fresh session in same directory."""
    old_wid = data[len(CB_RECOVERY_FRESH) :]
    validated = _validate_recovery_state(old_wid, update, context)
    if validated is None:
        await query.answer("Stale recovery (topic mismatch)", show_alert=True)
        return

    thread_id, _ = validated
    cwd = await _recovery_cwd_or_report(query, old_wid, context)
    if cwd is None:
        return

    request = ReplacementRequest(
        user_id=user_id,
        thread_id=thread_id,
        cwd=cwd,
        success_label="Fresh session started.",
        old_window_id=old_wid,
    )
    await _replace_from_callback(query, context, request)


async def _handle_continue(
    query: CallbackQuery,
    user_id: int,
    data: str,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    """Handle CB_RECOVERY_CONTINUE: resume most recent session via --continue.

    If there are no sessions on disk for ``cwd``, ``--continue`` would fail
    silently inside the agent. Surface the empty-state UI instead so the
    user can pick another project or start fresh.
    """
    old_wid = data[len(CB_RECOVERY_CONTINUE) :]
    validated = _validate_recovery_state(old_wid, update, context)
    if validated is None:
        await query.answer("Stale recovery (topic mismatch)", show_alert=True)
        return

    thread_id, _ = validated
    cwd = await _recovery_cwd_or_report(query, old_wid, context)
    if cwd is None:
        return

    provider_name = window_query.get_window_provider(old_wid)
    provider = get_provider_for_window(old_wid, provider_name=provider_name)
    # Probe with the resolved name, not the raw three-valued one. Resume may
    # widen an unknown provider to every picker-capable one because each entry
    # carries its own provider to the relaunch; Continue cannot, because it
    # launches exactly this ``provider``. Probing wider would find another
    # agent's sessions, skip the empty state, and run `<default> --continue`
    # into a folder it has nothing to continue — the silent failure the empty
    # state exists to prevent.
    if provider.capabilities.supports_resume_picker and not await asyncio.to_thread(
        scan_sessions_for_cwd,
        cwd,
        provider.capabilities.name,
    ):
        await _send_empty_state(query, old_wid, cwd)
        return

    request = ReplacementRequest(
        user_id=user_id,
        thread_id=thread_id,
        cwd=cwd,
        continue_session=True,
        success_label="Continuing previous session.",
        old_window_id=old_wid,
    )
    await _replace_from_callback(query, context, request)


async def _handle_resume(
    query: CallbackQuery,
    _user_id: int,
    data: str,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    """Handle CB_RECOVERY_RESUME: show session picker for --resume."""
    old_wid = data[len(CB_RECOVERY_RESUME) :]
    validated = _validate_recovery_state(old_wid, update, context)
    if validated is None:
        await query.answer("Stale recovery (topic mismatch)", show_alert=True)
        return

    cwd = await _recovery_cwd_or_report(query, old_wid, context)
    if cwd is None:
        return

    provider_name = window_query.get_window_provider(old_wid)
    sessions = await asyncio.to_thread(
        scan_sessions_for_cwd,
        cwd,
        provider_name,
    )
    if not sessions:
        await _send_empty_state(query, old_wid, cwd)
        return

    if context.user_data is not None:
        context.user_data[RECOVERY_SESSIONS] = [
            {
                "session_id": s.session_id,
                "summary": s.summary,
                "mtime": s.mtime,
                "provider_name": s.provider_name,
            }
            for s in sessions
        ]

    keyboard = _build_resume_picker_keyboard(sessions, old_wid)
    await safe_edit(
        query,
        f"⏪ Select a session to resume:\n(`{cwd}`)",
        reply_markup=keyboard,
    )
    await query.answer()


async def _send_empty_state(
    query: CallbackQuery,
    window_id: str,
    cwd: str,
) -> None:
    """Edit the recovery message to the no-sessions empty-state UI.

    Replaces the legacy ``query.answer("No sessions ...", show_alert=True)``
    toast with an inline keyboard so the user has explicit next steps
    instead of being trapped on a dismissable alert.
    """

    keyboard = _build_empty_resume_keyboard(window_id)
    await safe_edit(
        query,
        f"⚠ No sessions in this folder yet.\n(`{cwd}`)",
        reply_markup=keyboard,
    )
    await query.answer()


async def _handle_browse(
    query: CallbackQuery,
    _user_id: int,
    data: str,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    """Handle CB_RECOVERY_BROWSE: switch to the cross-project resume picker.

    The user explicitly chose to look outside the bound cwd, so the pending
    text — which targeted the original project — is dropped before
    delegating to the /resume cross-project flow.
    """

    # Lazy: sibling cycle — resume_command imports from this package.
    from ..user_state import RESUME_SESSIONS

    # Lazy: recovery_banner ↔ resume_command cycle through the picker
    from .resume_command import _build_resume_keyboard, scan_all_sessions

    old_wid = data[len(CB_RECOVERY_BROWSE) :]
    validated = _validate_recovery_state(old_wid, update, context)
    if validated is None:
        await query.answer("Stale recovery (topic mismatch)", show_alert=True)
        return

    provider_name = window_query.get_window_provider(old_wid)
    sessions = await asyncio.to_thread(scan_all_sessions, provider_name)
    if not sessions:
        await safe_edit(query, "⚠ No past sessions found in any project.")
        _clear_recovery_state(context.user_data)
        await query.answer("Nothing to resume")
        return

    if context.user_data is not None:
        context.user_data.pop(PENDING_THREAD_TEXT, None)
        context.user_data.pop(RECOVERY_SESSIONS, None)
        context.user_data[RESUME_SESSIONS] = [
            {
                "session_id": s.session_id,
                "summary": s.summary,
                "cwd": s.cwd,
                "mtime": s.mtime,
                "msg_count": s.msg_count,
                "provider_name": s.provider_name,
            }
            for s in sessions
        ]

    keyboard = _build_resume_keyboard(
        context.user_data[RESUME_SESSIONS] if context.user_data else [], page=0
    )
    await safe_edit(query, "⏪ Select a session to resume:", reply_markup=keyboard)
    await query.answer()


async def _handle_cancel(
    query: CallbackQuery,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    """Handle CB_RECOVERY_CANCEL: cancel recovery."""
    # Lazy: callback_helpers ↔ recovery cycle

    thread_id = get_thread_id(update)
    if thread_id is None:
        await query.answer("Stale recovery (topic mismatch)", show_alert=True)
        return

    pending_tid = (
        context.user_data.get(PENDING_THREAD_ID) if context.user_data else None
    )
    if pending_tid is not None and thread_id != pending_tid:
        await query.answer("Stale recovery (topic mismatch)", show_alert=True)
        return

    _clear_recovery_state(context.user_data)
    await safe_edit(query, "Cancelled. Send a message to try again.")
    await query.answer("Cancelled")
