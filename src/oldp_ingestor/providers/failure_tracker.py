"""Persistent per-item failure tracking across cron runs.

Some upstreams hand us individual documents that are permanently corrupt
(truncated XML, missing PDFs, deleted-but-still-listed entries). The
provider correctly skips them mid-run with a ``logger.warning``, but
nothing prevents the next cron run from picking the same doc out of the
same listing and failing again. Over time this masks "this case is gone"
as recurring noise and wastes upstream requests.

:class:`FailureTracker` persists a per-provider JSON file mapping
``doc_id`` to attempt count + last error. After ``max_retries``
consecutive failures the doc is considered permanently bad and
``should_skip()`` returns ``True`` until either ``record_success()`` is
called for it (e.g. upstream fixed itself and the next attempt parsed)
or the user wipes the state file.

Drop-in safe: :class:`NullFailureTracker` is a no-op default, so
providers that never get a real tracker behave exactly as before.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


class NullFailureTracker:
    """No-op tracker: matches the real interface, never persists, never skips.

    Used as the default ``Provider.failure_tracker`` so providers that
    aren't given a real one continue to work unchanged.
    """

    max_retries = 0  # Sentinel; should_skip is always False regardless.

    def should_skip(self, doc_id: str) -> bool:
        return False

    def record_failure(self, doc_id: str, error: Any) -> bool:
        return False

    def record_success(self, doc_id: str) -> None:
        return None

    def stats(self) -> dict:
        return {"tracked": 0, "exhausted": 0}


class FailureTracker:
    """JSON-backed retry counter shared across cron runs.

    State layout::

        {
            "<doc_id>": {
                "count": 3,
                "first_failure_at": "2026-04-30T03:15:00+00:00",
                "last_failure_at": "2026-05-01T03:15:00+00:00",
                "last_error": "Failed to parse XML: ..."
            },
            ...
        }

    Thread-safe under a single tracker instance; file-safe across
    concurrent processes only insofar as we write atomically (rename),
    so the worst concurrent case is a lost update — acceptable since
    cron runs are serialized per provider.

    Args:
        state_dir: Directory holding ``failures_<provider>.json``.
            Created if it does not exist.
        provider: Provider name (e.g. ``"by"``, ``"rii"``); used for
            the filename and log lines so multi-provider runs don't
            stomp on each other's state.
        max_retries: Skip the doc after this many failures. ``0``
            disables permanent skipping (failures still tracked, but
            never block).
    """

    def __init__(self, state_dir: str, provider: str, max_retries: int = 5) -> None:
        if not state_dir:
            raise ValueError(
                "state_dir is required (use NullFailureTracker for opt-out)"
            )
        if not provider:
            raise ValueError("provider name is required")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")

        self.state_dir = state_dir
        self.provider = provider
        self.max_retries = max_retries
        self._path = os.path.join(state_dir, f"failures_{provider}.json")
        self._lock = threading.Lock()
        self._state: dict[str, dict] = self._load()
        # Remember which doc_ids we've already logged the "skipping" line
        # for in this run, so a list of 200 IDs doesn't spam 200 lines.
        self._already_logged_skip: set[str] = set()

    def _load(self) -> dict[str, dict]:
        if not os.path.isfile(self._path):
            return {}
        try:
            with open(self._path, encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                logger.warning(
                    "FailureTracker: state file %s is not a dict, starting empty",
                    self._path,
                )
                return {}
            return data
        except (OSError, ValueError) as exc:
            logger.warning(
                "FailureTracker: failed to load %s (%s); starting empty",
                self._path,
                exc,
            )
            return {}

    def _save(self) -> None:
        os.makedirs(self.state_dir, exist_ok=True)
        tmp_path = self._path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(self._state, f, indent=2, sort_keys=True, ensure_ascii=False)
            f.write("\n")
        os.replace(tmp_path, self._path)

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def should_skip(self, doc_id: str) -> bool:
        """Return True if *doc_id* has hit ``max_retries`` and should be skipped.

        Logs a single INFO line per doc per run on the first skip; subsequent
        ``should_skip`` calls within the same run stay silent.
        """
        if self.max_retries <= 0:
            return False
        entry = self._state.get(doc_id)
        if entry is None:
            return False
        if entry.get("count", 0) < self.max_retries:
            return False
        if doc_id not in self._already_logged_skip:
            logger.info(
                "Skipping %s/%s — exhausted %d retry attempts (last error: %s)",
                self.provider,
                doc_id,
                entry.get("count", 0),
                entry.get("last_error", "")[:120],
            )
            self._already_logged_skip.add(doc_id)
        return True

    def record_failure(self, doc_id: str, error: Any) -> bool:
        """Increment the failure counter for *doc_id*. Returns True if this
        attempt was the one that exhausted the retry budget (so callers can
        emit a one-time WARNING)."""
        with self._lock:
            entry = self._state.get(doc_id)
            now = self._now_iso()
            error_str = str(error)[:500]
            if entry is None:
                entry = {
                    "count": 1,
                    "first_failure_at": now,
                    "last_failure_at": now,
                    "last_error": error_str,
                }
            else:
                entry["count"] = entry.get("count", 0) + 1
                entry["last_failure_at"] = now
                entry["last_error"] = error_str
            self._state[doc_id] = entry
            self._save()
            just_exhausted = self.max_retries > 0 and entry["count"] == self.max_retries

        if just_exhausted:
            logger.warning(
                "%s/%s has now failed %d times — will be skipped on future runs "
                "until it succeeds or state is cleared",
                self.provider,
                doc_id,
                entry["count"],
            )
        return just_exhausted

    def record_success(self, doc_id: str) -> None:
        """Clear any failure entry for *doc_id*. No-op if not tracked."""
        with self._lock:
            if self._state.pop(doc_id, None) is None:
                return
            self._save()

    def stats(self) -> dict:
        """Return a small summary suitable for end-of-run logging."""
        tracked = len(self._state)
        exhausted = (
            sum(
                1 for e in self._state.values() if e.get("count", 0) >= self.max_retries
            )
            if self.max_retries > 0
            else 0
        )
        return {"tracked": tracked, "exhausted": exhausted}
