"""增量同步接口：作业时间线 + 按水位差捕获增量的完整链路。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.delta_sync import DeltaSyncService
from app.services.delta_tracker import delta_tracker
from app.services.source_records import source_record_service

router = APIRouter(prefix="/api/delta_sync", tags=["增量同步"])
service = DeltaSyncService()
LIST_FIELDS = ["水位编号", "源数据集", "增量字段", "上次水位", "当前水位", "同步周期", "捕获行数", "最后同步"]
STATUSES = ["待初始化", "运行中", "追赶延迟", "已暂停"]


class CapturePayload(BaseModel):
    """增量捕获载荷：不传目标水位时取源数据集当前最大业务时间。"""

    target_watermark: str | None = None


class LateRecordPayload(BaseModel):
    """迟到记录处置：replacement=回补原业务区间发替代批次；correction=进入当前批次更正清单。"""

    record_key: str
    mode: str


class SourceRecordPayload(BaseModel):
    """向源数据集写入一条源记录（可构造业务时间早、采集时间新的迟到记录）。"""

    dataset: str
    record_key: str
    business_time: str
    ingest_time: str
    payload: str = ""


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def list_entries() -> dict[str, Any]:
    items = service.get_timeline()
    return {"items": items}


@router.get("/export")
def export_entries() -> StreamingResponse:
    items = service.get_timeline()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    all_keys = ["id"]
    if items:
        all_keys = list(items[0].keys())
    writer.writerow(all_keys)
    for item in items:
        writer.writerow([csv_cell(item.get(k, "")) for k in all_keys])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=delta_sync.csv"})


# ---------- 增量捕获链路：固定路径需声明在 /{entry_id} 之前 ----------

@router.get("/configs")
def list_sync_configs() -> list[dict[str, Any]]:
    """返回同步水位原始配置（上次/当前水位、增量字段、同步周期等）。"""
    return service.list_configs()


@router.get("/source/datasets")
def list_source_datasets() -> list[dict[str, Any]]:
    return source_record_service.list_datasets()


@router.get("/source/records")
def list_source_records(dataset: str | None = None) -> list[dict[str, Any]]:
    return source_record_service.list_records(dataset)


@router.post("/source/records")
def add_source_record(payload: SourceRecordPayload) -> ActionResult:
    record, error = source_record_service.add_record(
        payload.dataset, payload.record_key, payload.business_time, payload.ingest_time, payload.payload,
    )
    if record is None:
        return ActionResult(ok=False, message=error)
    return ActionResult(ok=True, message=f"源记录 {payload.record_key} 已写入 {payload.dataset}", entry=record)


@router.post("/{entry_id}/capture")
def capture_increment(entry_id: int, payload: CapturePayload) -> dict[str, Any]:
    """按水位差执行一次增量捕获，生成增量批次数据集与同步位置。"""
    result = delta_tracker.capture_increment(entry_id, payload.target_watermark)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error"))
    return result


@router.post("/{entry_id}/late-records")
def handle_late_record(entry_id: int, payload: LateRecordPayload) -> dict[str, Any]:
    """处置迟到记录：回补原业务区间发替代批次，或挂入当前批次更正清单。"""
    result = delta_tracker.backfill_late_record(entry_id, payload.record_key, payload.mode)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error"))
    return result


@router.get("/positions")
def list_positions() -> list[dict[str, Any]]:
    return delta_tracker.list_positions()


@router.get("/positions/{position_id}")
def get_position(position_id: int) -> dict[str, Any]:
    position = delta_tracker.get_position(position_id)
    if position is None:
        raise HTTPException(status_code=404, detail=f"同步位置 {position_id} 不存在")
    return position


@router.get("/batches")
def list_batches(sync_id: int | None = Query(default=None)) -> list[dict[str, Any]]:
    return delta_tracker.list_batches(sync_id)


@router.get("/registry")
def list_registry(dataset: str | None = Query(default=None)) -> list[dict[str, Any]]:
    return delta_tracker.list_registry(dataset)


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"同步水位 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="同步水位已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
