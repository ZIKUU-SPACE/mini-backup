
# /srv/mini-backup/service/history.py

from __future__ import annotations

import asyncio
import json
import os
import uuid

from datetime import datetime
from pathlib import Path
from typing import Any


HISTORY_PATH = Path(
    "/srv/mini-backup/data/history.jsonl"
)


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(
        timespec="seconds"
    )


class BackupHistory:
    """
    Persistent execution history for Mini Backup Service.

    The history file is append-only JSON Lines.

    Each execution produces two events:
      - started
      - finished

    A missing finished event indicates that the execution
    was interrupted or that the service stopped before
    recording its result.

    All public operations are serialized by an asyncio lock.
    """

    def __init__(
        self,
        path: Path = HISTORY_PATH,
    ) -> None:
        self._path = path
        self._lock = asyncio.Lock()

    # MARK: - Storage

    def _append_sync(
        self,
        event: dict[str, Any],
    ) -> None:
        self._path.parent.mkdir(
            parents=True,
            exist_ok=True,
            mode=0o700,
        )

        payload = json.dumps(
            event,
            ensure_ascii=False,
            separators=(",", ":"),
        )

        descriptor = os.open(
            self._path,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND,
            0o600,
        )

        with os.fdopen(
            descriptor,
            "w",
            encoding="utf-8",
        ) as file:
            file.write(payload)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())

    def _read_events_sync(
        self,
    ) -> list[dict[str, Any]]:
        if not self._path.exists():
            return []

        events: list[dict[str, Any]] = []

        with self._path.open(
            "r",
            encoding="utf-8",
        ) as file:
            for line in file:
                line = line.strip()

                if not line:
                    continue

                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if isinstance(event, dict):
                    events.append(event)

        return events

    # MARK: - Projection

    @staticmethod
    def _project(
        events: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        executions: dict[
            str,
            dict[str, Any],
        ] = {}

        order: list[str] = []

        for event in events:
            run_id = event.get("run_id")
            event_type = event.get("event")

            if not isinstance(run_id, str):
                continue

            if event_type == "started":
                if run_id in executions:
                    continue

                started_at = event.get(
                    "started_at"
                )

                if not isinstance(started_at, str):
                    continue

                executions[run_id] = {
                    "run_id": run_id,
                    "started_at": started_at,
                    "completed_at": None,
                    "execution_state": "running",
                    "exit_code": None,
                    "status": "running",
                    "targets": [],
                }

                order.append(run_id)

            elif event_type == "finished":
                execution = executions.get(
                    run_id
                )

                if execution is None:
                    continue

                if execution["execution_state"] != "running":
                    continue

                execution.update({
                    "completed_at": event.get(
                        "completed_at"
                    ),
                    "execution_state": event.get(
                        "execution_state",
                        "failed",
                    ),
                    "exit_code": event.get(
                        "exit_code"
                    ),
                    "status": event.get(
                        "status",
                        "failed",
                    ),
                    "targets": event.get(
                        "targets",
                        [],
                    ),
                })

                for key in (
                    "report_started_at",
                    "report_completed_at",
                    "error",
                ):
                    if key in event:
                        execution[key] = event[key]

        return [
            executions[run_id]
            for run_id in reversed(order)
        ]

    # MARK: - Execution Start

    async def start(
        self,
        started_at: str,
    ) -> str:
        """
        Persist execution start before launching the
        backup process.

        Returns a unique execution identifier.
        """

        run_id = str(uuid.uuid4())

        event = {
            "event": "started",
            "run_id": run_id,
            "started_at": started_at,
        }

        async with self._lock:
            await asyncio.to_thread(
                self._append_sync,
                event,
            )

        return run_id

    # MARK: - Execution Completion

    async def finish(
        self,
        run_id: str,
        *,
        execution_state: str,
        exit_code: int | None,
        status: str,
        targets: list[dict[str, Any]] | None = None,
        completed_at: str | None = None,
        report_started_at: str | None = None,
        report_completed_at: str | None = None,
        error: str | None = None,
    ) -> None:
        """
        Persist the final result of an execution.

        A run may be finished only once.
        """

        async with self._lock:
            events = await asyncio.to_thread(
                self._read_events_sync
            )

            executions = self._project(events)

            execution = next(
                (
                    entry
                    for entry in executions
                    if entry["run_id"] == run_id
                ),
                None,
            )

            if execution is None:
                raise ValueError(
                    f"Unknown backup run: {run_id}"
                )

            if execution["execution_state"] != "running":
                return

            event: dict[str, Any] = {
                "event": "finished",
                "run_id": run_id,
                "completed_at": (
                    completed_at or _now_iso()
                ),
                "execution_state": execution_state,
                "exit_code": exit_code,
                "status": status,
                "targets": targets or [],
            }

            if report_started_at is not None:
                event["report_started_at"] = (
                    report_started_at
                )

            if report_completed_at is not None:
                event["report_completed_at"] = (
                    report_completed_at
                )

            if error is not None:
                event["error"] = error

            await asyncio.to_thread(
                self._append_sync,
                event,
            )

    # MARK: - History Retrieval

    async def recent(
        self,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """
        Return newest executions first.
        """

        if not 1 <= limit <= 500:
            raise ValueError(
                "History limit must be between 1 and 500"
            )

        async with self._lock:
            events = await asyncio.to_thread(
                self._read_events_sync
            )

            return self._project(events)[:limit]

    # MARK: - Recovery

    async def recover_interrupted(
        self,
    ) -> int:
        """
        Mark unfinished executions as failed.

        Call this once when the backup service starts,
        before accepting new backup requests.

        The service supervisor must ensure that previous
        backup processes have stopped before recovery.
        """

        async with self._lock:
            events = await asyncio.to_thread(
                self._read_events_sync
            )

            executions = self._project(events)

            interrupted = [
                entry
                for entry in executions
                if entry["execution_state"] == "running"
            ]

            for execution in interrupted:
                event = {
                    "event": "finished",
                    "run_id": execution["run_id"],
                    "completed_at": _now_iso(),
                    "execution_state": "failed",
                    "exit_code": None,
                    "status": "failed",
                    "targets": [],
                    "error": (
                        "Backup service stopped before "
                        "execution completion was recorded."
                    ),
                }

                await asyncio.to_thread(
                    self._append_sync,
                    event,
                )

            return len(interrupted)
