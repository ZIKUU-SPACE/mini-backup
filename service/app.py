# /srv/mini-backup/service/app.py

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, status
from history import BackupHistory
from pydantic import BaseModel


BASE_DIR = Path("/srv/mini-backup")
BACKUP_COMMAND = BASE_DIR / "backup"
CONFIG_PATH = BASE_DIR / "config.json"

BACKUP_DIRECTORY_NAME = "zikuu-mini"

from contextlib import asynccontextmanager


@asynccontextmanager
async def backup_lifespan(app: FastAPI):
    history = BackupHistory()

    recovered = await history.recover_interrupted()

    if recovered:
        print(
            f"Recovered {recovered} interrupted backup runs."
        )

    yield


app = FastAPI(
    title="ZIKUU Mini Backup Service",
    version="0.1.0",
    lifespan=backup_lifespan,
)


class BackupExecutionState(BaseModel):
    state: str
    started_at: str | None = None
    completed_at: str | None = None
    exit_code: int | None = None
    stdout: str | None = None
    stderr: str | None = None


class BackupStartResponse(BaseModel):
    state: str
    started_at: str


class BackupReportResponse(BaseModel):
    generation: str
    report: dict[str, Any]


_state_lock = asyncio.Lock()

_execution_state = BackupExecutionState(
    state="idle",
)

_backup_task: asyncio.Task[None] | None = None

_backup_history = BackupHistory()


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(
        timespec="seconds",
    )


def _load_config() -> dict[str, Any]:
    try:
        with CONFIG_PATH.open(
            "r",
            encoding="utf-8",
        ) as file:
            data = json.load(file)
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Backup configuration not found: "
                f"{CONFIG_PATH}"
            ),
        ) from exc
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Backup configuration is invalid: "
                f"{CONFIG_PATH}"
            ),
        ) from exc

    if not isinstance(data, dict):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Backup configuration root must be an object.",
        )

    return data


async def _run_command(
    *args: str,
) -> tuple[int, str, str]:
    try:
        process = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except OSError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Failed to execute command: "
                f"{args[0]}"
            ),
        ) from exc

    stdout_bytes, stderr_bytes = (
        await process.communicate()
    )

    stdout = stdout_bytes.decode(
        "utf-8",
        errors="replace",
    ).strip()

    stderr = stderr_bytes.decode(
        "utf-8",
        errors="replace",
    ).strip()

    return (
        process.returncode,
        stdout,
        stderr,
    )


async def _resolve_backup_mount_point() -> Path:
    config = _load_config()

    backup_device = config.get(
        "backup_device",
    )

    if not isinstance(
        backup_device,
        dict,
    ):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "backup_device is missing from "
                "backup configuration."
            ),
        )

    backup_uuid = backup_device.get(
        "uuid",
    )
    expected_label = backup_device.get(
        "label",
    )

    if (
        not isinstance(
            backup_uuid,
            str,
        )
        or not backup_uuid
    ):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Backup device UUID is not configured."
            ),
        )

    if (
        not isinstance(
            expected_label,
            str,
        )
        or not expected_label
    ):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Backup device label is not configured."
            ),
        )

    return_code, device, _ = (
        await _run_command(
            "blkid",
            "-U",
            backup_uuid,
        )
    )

    if (
        return_code != 0
        or not device
    ):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Backup device not found for UUID: "
                f"{backup_uuid}"
            ),
        )

    return_code, actual_label, _ = (
        await _run_command(
            "blkid",
            "-s",
            "LABEL",
            "-o",
            "value",
            device,
        )
    )

    if return_code != 0:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Failed to read backup device label."
            ),
        )

    if actual_label != expected_label:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "message":
                    "Backup device label mismatch.",
                "expected":
                    expected_label,
                "actual":
                    actual_label,
            },
        )

    return_code, mount_point, _ = (
        await _run_command(
            "findmnt",
            "-nr",
            "-S",
            device,
            "-o",
            "TARGET",
        )
    )

    if (
        return_code != 0
        or not mount_point
    ):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Backup device is not mounted: "
                f"{device}"
            ),
        )

    return Path(mount_point)


async def _get_report_path(
    generation: str,
) -> Path:
    if generation not in {
        "current",
        "previous",
    }:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Backup generation must be "
                "'current' or 'previous'."
            ),
        )

    mount_point = (
        await _resolve_backup_mount_point()
    )

    return (
        mount_point
        / BACKUP_DIRECTORY_NAME
        / generation
        / "backup-report.json"
    )


async def _load_backup_report(
    generation: str,
) -> dict[str, Any]:
    report_path = await _get_report_path(
        generation,
    )

    try:
        text = await asyncio.to_thread(
            report_path.read_text,
            encoding="utf-8",
        )
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"{generation} backup report "
                "does not exist."
            ),
        ) from exc
    except OSError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Failed to read backup report: "
                f"{report_path}"
            ),
        ) from exc

    try:
        report = json.loads(text)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=(
                "Backup report contains invalid JSON: "
                f"{report_path}"
            ),
        ) from exc

    if not isinstance(
        report,
        dict,
    ):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=(
                "Backup report root must be an object."
            ),
        )

    return report


async def _execute_backup() -> None:
    global _execution_state

    try:
        process = await asyncio.create_subprocess_exec(
            "/usr/bin/bash",
            str(BACKUP_COMMAND),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout_bytes, stderr_bytes = (
            await process.communicate()
        )

        stdout = stdout_bytes.decode(
            "utf-8",
            errors="replace",
        ).strip()

        stderr = stderr_bytes.decode(
            "utf-8",
            errors="replace",
        ).strip()

        async with _state_lock:
            _execution_state = (
                BackupExecutionState(
                    state=(
                        "completed"
                        if process.returncode == 0
                        else "failed"
                    ),
                    started_at=(
                        _execution_state.started_at
                    ),
                    completed_at=_now_iso(),
                    exit_code=process.returncode,
                    stdout=stdout or None,
                    stderr=stderr or None,
                )
            )

    except Exception as exc:
        async with _state_lock:
            _execution_state = (
                BackupExecutionState(
                    state="failed",
                    started_at=(
                        _execution_state.started_at
                    ),
                    completed_at=_now_iso(),
                    exit_code=None,
                    stdout=None,
                    stderr=str(exc),
                )
            )

async def _execute_backup_with_history(
    run_id: str,
) -> None:
    """
    Run the existing backup operation and persist its result.

    History failures must not change the execution result.
    """
    try:
        await _execute_backup()
    finally:
        async with _state_lock:
            execution = _execution_state.model_copy(
                deep=True
            )

        report: dict[str, Any] | None = None

        if execution.state == "completed":
            try:
                report = await _load_backup_report(
                    "current"
                )
            except Exception as exc:
                print(
                    "Failed to load backup history report:",
                    exc,
                )

        overall_status = (
            report.get("status", "unknown")
            if report is not None
            else (
                "failed"
                if execution.state == "failed"
                else "unknown"
            )
        )

        try:
            await _backup_history.finish(
                run_id,
                execution_state=execution.state,
                exit_code=execution.exit_code,
                status=overall_status,
                targets=(
                    report.get("targets", [])
                    if report is not None
                    else []
                ),
                completed_at=execution.completed_at,
                report_started_at=(
                    report.get("started_at")
                    if report is not None
                    else None
                ),
                report_completed_at=(
                    report.get("completed_at")
                    if report is not None
                    else None
                ),
                error=execution.stderr,
            )
        except Exception as exc:
            print(
                "Failed to persist backup history:",
                exc,
            )


@app.get("/health")
async def health() -> dict[str, str]:
    return {
        "status": "ok",
    }


@app.get(
    "/status",
    response_model=BackupExecutionState,
)
async def get_backup_status() -> BackupExecutionState:
    async with _state_lock:
        return _execution_state.model_copy(
            deep=True,
        )


@app.post(
    "/backup",
    response_model=BackupStartResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def start_backup() -> BackupStartResponse:
    global _backup_task
    global _execution_state

    if not BACKUP_COMMAND.is_file():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Backup command not found: "
                f"{BACKUP_COMMAND}"
            ),
        )

    async with _state_lock:
        if (
            _backup_task is not None
            and not _backup_task.done()
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Backup is already running.",
            )

        started_at = _now_iso()

        try:
            run_id = await _backup_history.start(
                started_at
            )
        except OSError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=(
                    "Failed to persist backup start: "
                    f"{exc}"
                ),
            ) from exc

        _execution_state = BackupExecutionState(
            state="running",
            started_at=started_at,
        )

        _backup_task = asyncio.create_task(
            _execute_backup_with_history(run_id)
        )

    return BackupStartResponse(
        state="running",
        started_at=started_at,
    )


@app.get(
    "/backups/current",
    response_model=BackupReportResponse,
)
async def get_current_backup() -> BackupReportResponse:
    report = await _load_backup_report(
        "current",
    )

    return BackupReportResponse(
        generation="current",
        report=report,
    )


@app.get(
    "/backups/previous",
    response_model=BackupReportResponse,
)
async def get_previous_backup() -> BackupReportResponse:
    report = await _load_backup_report(
        "previous",
    )

    return BackupReportResponse(
        generation="previous",
        report=report,
    )

@app.get("/history")
async def get_backup_history(
    limit: int = Query(
        default=10,
        ge=1,
        le=500,
    ),
) -> dict[str, list[dict[str, Any]]]:
    entries = await _backup_history.recent(
        limit=limit,
    )

    return {
        "entries": entries,
    }
