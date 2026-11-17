"""增量追踪器：按水位差捕获增量，并驱动采集仓储持久化增量批次。

数据流：delta_sync + 源数据记录 -> result_store

核心链路：
1. 读取同步水位的上次水位、当前水位，与源数据集记录按业务时间（增量字段）
   做 (当前水位, 目标水位] 区间筛选，得到本次真实追赶范围；
2. 先把增量批次写入采集仓储，再登记同步位置与源记录键，最后才推进当前水位，
   保证“批次落库后水位才前进”的原子顺序，避免水位前进但数据丢失；
3. 同一条源记录键在 source_registry 中只会登记一次，任何增量批次都不能重复计入；
4. 迟到记录（业务时间早于当前水位、采集时间刚到达）支持两种处置：
   - replacement：回补原业务区间，发布替代批次，原批次标记被替代、保留留痕；
   - correction：进入当前批次更正清单并保留原业务时间，同时生成更正批次与同步位置。
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.store import store
from app.services.result_store import ResultStoreService
from app.services.source_records import DATASET_FIELDS, source_record_service

SYNC_MODULE = "delta_sync"
RESULT_MODULE = "result_store"
POSITION_TABLE = "sync_positions"
BATCH_TABLE = "delta_batches"
REGISTRY_TABLE = "source_registry"

BATCH_INCREMENT = "增量"
BATCH_REPLACEMENT = "替代"
BATCH_CORRECTION = "更正"


def _now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


class DeltaTracker:
    def __init__(self) -> None:
        self._result_svc = ResultStoreService()

    # ---------- 基础工具 ----------

    def _next_id(self, table: str) -> int:
        rows = store.joint_rows(table)
        return max((int(r.get("id", 0)) for r in rows), default=0) + 1

    def _checksum(self, records: list[dict[str, Any]]) -> str:
        material = "|".join(
            f"{r['record_key']}@{r['business_time']}" for r in sorted(records, key=lambda x: x["record_key"])
        )
        return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]

    def _registry_find(self, dataset: str, record_key: str) -> dict[str, Any] | None:
        for row in store.joint_rows(REGISTRY_TABLE):
            if row.get("dataset") == dataset and row.get("record_key") == record_key:
                return row
        return None

    def _positions_of(self, sync_id: int) -> list[dict[str, Any]]:
        return [p for p in store.joint_rows(POSITION_TABLE) if int(p.get("sync_id", 0)) == sync_id]

    def _batches_of(self, sync_id: int) -> list[dict[str, Any]]:
        return [b for b in store.joint_rows(BATCH_TABLE) if int(b.get("sync_id", 0)) == sync_id]

    def _next_batch_seq(self, sync_id: int) -> int:
        return len(self._batches_of(sync_id)) + 1

    def _find_covering_position(self, sync_id: int, business_time: str) -> dict[str, Any] | None:
        """找到业务时间落在其水位区间内、当前仍有效的最新增量/替代位置。

        同一区间被多次回补时，后续替代批次应链接到上一个替代批次，形成
        增量 -> R1 -> R2 的替代链，而不是都直接挂在最初的增量批次上。
        """
        candidates = [
            p for p in self._positions_of(sync_id)
            if p.get("position_type") in (BATCH_INCREMENT, BATCH_REPLACEMENT)
            and str(p.get("range_from", "")) < business_time <= str(p.get("range_to", ""))
        ]
        if not candidates:
            return None
        latest_position = max(candidates, key=lambda p: int(p.get("id", 0)))
        latest_batch = next(
            (b for b in store.joint_rows(BATCH_TABLE)
             if int(b.get("id", 0)) == int(latest_position["batch_id"])),
            None,
        )
        # 最新位置若已被更新的替代批次取代，则顺着替代链找到当前有效批次
        while latest_batch is not None and latest_batch.get("superseded"):
            successor = next(
                (b for b in self._batches_of(sync_id)
                 if b.get("batch_no") == latest_batch.get("superseded_by")),
                None,
            )
            if successor is None:
                break
            latest_batch = successor
        return next(
            (p for p in candidates if int(p.get("batch_id", 0)) == int(latest_batch["id"])),
            latest_position,
        )

    def _base_batch_no(self, sync_id: int, batch: dict[str, Any]) -> str:
        """返回替代链根增量批次的编号（去掉 -Rx / -Cx 后缀）。"""
        if batch.get("batch_type") == BATCH_INCREMENT:
            return str(batch["batch_no"])
        return self._base_batch_no(
            sync_id,
            next((b for b in self._batches_of(sync_id)
                  if b["batch_no"] == batch.get("replaced_batch_no")), batch),
        )

    # ---------- 增量捕获 ----------

    def capture_increment(self, sync_id: int, target_watermark: str | None = None) -> dict[str, Any]:
        """基于水位差执行一次增量捕获：区间筛选 -> 批次落库 -> 登记 -> 推进水位。"""
        sync = store.find(SYNC_MODULE, sync_id)
        if sync is None:
            return {"ok": False, "error": f"同步水位 {sync_id} 不存在"}

        dataset = str(sync.get("源数据集", ""))
        if dataset not in DATASET_FIELDS:
            return {"ok": False, "error": f"源数据集 {dataset} 未注册源记录，无法按水位差捕获"}
        field = str(sync.get("增量字段", "") or DATASET_FIELDS[dataset])

        last_watermark = str(sync.get("上次水位", "0") or "0")
        current_watermark = str(sync.get("当前水位", "0") or "0")

        source_rows = source_record_service.list_records(dataset)
        max_business = max((r["business_time"] for r in source_rows), default=current_watermark)
        target = target_watermark.strip() if target_watermark else max_business

        # 同一水位差只算一次增量捕获：目标水位未超过当前水位直接拒绝
        if target <= current_watermark:
            return {"ok": False, "error": "目标水位未超过当前水位，同一水位差只允许一次增量捕获"}

        # 按 (当前水位, 目标水位] 区间筛选真实追赶范围
        interval_records = source_record_service.scan_interval(dataset, current_watermark, target)
        # 防御性去重：已登记的源记录键不再进入任何增量批次
        fresh_records = [r for r in interval_records if self._registry_find(dataset, r["record_key"]) is None]
        skipped_keys = [r["record_key"] for r in interval_records if self._registry_find(dataset, r["record_key"])]
        if not fresh_records:
            return {"ok": False, "error": "水位区间内没有可捕获的新源记录，本次不推进水位"}

        checksum = self._checksum(fresh_records)
        seq = self._next_batch_seq(sync_id)
        batch_no = f"DELTA-{sync_id}-{seq:03d}"
        now = _now()
        row_count = len(fresh_records)
        record_keys = [r["record_key"] for r in fresh_records]

        # 先在内存里备齐所有产物，任一步失败都不写库、不动水位
        position_id = self._next_id(POSITION_TABLE)
        batch_record_id = self._next_id(BATCH_TABLE)
        result_values = {
            "批次编号": batch_no,
            "关联任务": dataset,
            "原始行数": str(row_count),
            "有效行数": str(row_count),
            "存储路径": f"/store/delta/{sync_id}/{batch_no}.json",
            "压缩格式": "snappy",
            "校验摘要": checksum,
            "落地时间": now,
        }
        result_entry, missing = self._result_svc.create_entry(result_values)
        if result_entry is None:
            return {"ok": False, "error": f"增量批次写入采集仓储失败：缺少 {missing}"}

        batch_record = {
            "id": batch_record_id,
            "batch_no": batch_no,
            "batch_type": BATCH_INCREMENT,
            "sync_id": sync_id,
            "position_id": position_id,
            "result_batch_id": result_entry["id"],
            "dataset": dataset,
            "incremental_field": field,
            "sync_cycle": str(sync.get("同步周期", "")),
            "range_from": current_watermark,
            "range_to": target,
            "row_count": row_count,
            "record_keys": record_keys,
            "checksum": checksum,
            "superseded": False,
            "corrections": [],
            "created_at": now,
        }
        position = {
            "id": position_id,
            "sync_id": sync_id,
            "position_type": BATCH_INCREMENT,
            "last_watermark": last_watermark,
            "current_watermark": current_watermark,
            "range_from": current_watermark,
            "range_to": target,
            "captured_rows": row_count,
            "batch_no": batch_no,
            "batch_id": batch_record_id,
            "result_batch_id": result_entry["id"],
            "checksum": checksum,
            "record_keys": record_keys,
            "skipped_keys": skipped_keys,
            "synced_at": now,
        }
        registry_entries = [
            {
                "id": self._next_id(REGISTRY_TABLE) + idx,
                "dataset": dataset,
                "record_key": key,
                "batch_no": batch_no,
                "batch_id": batch_record_id,
                "position_id": position_id,
                "counted_once": True,
                "registered_at": now,
            }
            for idx, key in enumerate(record_keys)
        ]

        # 原子顺序：采集仓储批次 -> 增量批次数据集 -> 同步位置 -> 源记录键登记 -> 推进水位
        store.joint_append(BATCH_TABLE, batch_record)
        store.joint_append(POSITION_TABLE, position)
        for entry in registry_entries:
            store.joint_append(REGISTRY_TABLE, entry)

        sync["上次水位"] = current_watermark
        sync["当前水位"] = target
        sync["捕获行数"] = str(row_count)
        sync["最后同步"] = now
        sync["status"] = "运行中"
        sync["pending"] = True
        sync["abnormal"] = False

        return {
            "ok": True,
            "captured_rows": row_count,
            "batch_no": batch_no,
            "position": position,
            "batch": batch_record,
            "result_batch_id": result_entry["id"],
            "skipped_keys": skipped_keys,
        }

    def sync_all(self) -> dict[str, Any]:
        """对所有待初始化/运行中/追赶延迟的同步任务各执行一次增量捕获。"""
        tasks = [s for s in store.rows(SYNC_MODULE)
                 if s.get("status") in ("待初始化", "运行中", "追赶延迟")]
        results: list[dict[str, Any]] = []
        for task in tasks:
            results.append(self.capture_increment(int(task["id"])))
        total_captured = sum(r.get("captured_rows", 0) for r in results if r.get("ok"))
        return {"ok": True, "synced": len(results), "total_captured": total_captured, "results": results}

    # ---------- 迟到记录回补 / 更正 ----------

    def backfill_late_record(self, sync_id: int, record_key: str, mode: str) -> dict[str, Any]:
        """处置业务时间早于当前水位、采集时间刚到达的迟到记录。"""
        sync = store.find(SYNC_MODULE, sync_id)
        if sync is None:
            return {"ok": False, "error": f"同步水位 {sync_id} 不存在"}
        dataset = str(sync.get("源数据集", ""))
        record = source_record_service.get_record(dataset, record_key)
        if record is None:
            return {"ok": False, "error": f"源记录 {record_key} 不在数据集 {dataset} 中"}

        registered = self._registry_find(dataset, record_key)
        if registered is not None:
            return {"ok": False, "error": f"源记录 {record_key} 已计入批次 {registered.get('batch_no')}，不能重复计入"}
        if record["business_time"] > str(sync.get("当前水位", "")):
            return {"ok": False, "error": "该记录业务时间晚于当前水位，属于正常增量范围，请直接执行增量捕获"}
        if mode not in ("replacement", "correction"):
            return {"ok": False, "error": "处置方式必须是 replacement（替代批次）或 correction（更正清单）"}

        if mode == "replacement":
            return self._backfill_by_replacement(sync, record)
        return self._backfill_by_correction(sync, record)

    def _backfill_by_replacement(self, sync: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
        """回补原业务区间：发布覆盖原区间的替代批次，原批次保留并标记被替代。"""
        sync_id = int(sync["id"])
        dataset = record["dataset"]
        covering = self._find_covering_position(sync_id, record["business_time"])
        if covering is None:
            return {"ok": False, "error": "找不到覆盖该业务时间的历史增量批次，无法回补原业务区间"}

        # 被替代的可能是最初的增量批次，也可能是上一个替代批次（替代链）
        original_batch = next(
            (b for b in store.joint_rows(BATCH_TABLE) if int(b.get("id", 0)) == int(covering["batch_id"])),
            None,
        )
        if original_batch is None:
            return {"ok": False, "error": "原增量批次数据集缺失，无法生成替代批次"}

        # 原批次记录 + 迟到记录组成替代批次的完整数据集；原批次键不重复登记
        original_records = [
            source_record_service.get_record(dataset, key) for key in original_batch.get("record_keys", [])
        ]
        original_records = [r for r in original_records if r is not None]
        full_records = original_records + [record]
        checksum = self._checksum(full_records)
        now = _now()
        # 编号挂在替代链根增量批次下：DELTA-x-001-R1、-R2……，链上顺序唯一
        root_batch_no = self._base_batch_no(sync_id, original_batch)
        replacement_count = sum(
            1 for b in self._batches_of(sync_id)
            if b.get("batch_type") == BATCH_REPLACEMENT
            and str(b.get("batch_no", "")).startswith(root_batch_no + "-R")
        )
        batch_no = f"{root_batch_no}-R{replacement_count + 1}"

        position_id = self._next_id(POSITION_TABLE)
        batch_record_id = self._next_id(BATCH_TABLE)
        result_entry, missing = self._result_svc.create_entry({
            "批次编号": batch_no,
            "关联任务": dataset,
            "原始行数": str(len(full_records)),
            "有效行数": str(len(full_records)),
            "存储路径": f"/store/delta/{sync_id}/{batch_no}.json",
            "压缩格式": "snappy",
            "校验摘要": checksum,
            "落地时间": now,
        })
        if result_entry is None:
            return {"ok": False, "error": f"替代批次写入采集仓储失败：缺少 {missing}"}

        replacement_batch = {
            "id": batch_record_id,
            "batch_no": batch_no,
            "batch_type": BATCH_REPLACEMENT,
            "sync_id": sync_id,
            "position_id": position_id,
            "result_batch_id": result_entry["id"],
            "dataset": dataset,
            "incremental_field": original_batch.get("incremental_field", ""),
            "sync_cycle": original_batch.get("sync_cycle", ""),
            "range_from": original_batch["range_from"],
            "range_to": original_batch["range_to"],
            "row_count": len(full_records),
            "new_row_count": 1,
            "reused_keys": [r["record_key"] for r in original_records],
            "record_keys": [r["record_key"] for r in full_records],
            "checksum": checksum,
            "replaced_batch_id": original_batch["id"],
            "replaced_batch_no": original_batch["batch_no"],
            "superseded": False,
            "corrections": [],
            "created_at": now,
        }
        position = {
            "id": position_id,
            "sync_id": sync_id,
            "position_type": BATCH_REPLACEMENT,
            "last_watermark": sync.get("上次水位", ""),
            "current_watermark": sync.get("当前水位", ""),
            "range_from": original_batch["range_from"],
            "range_to": original_batch["range_to"],
            "captured_rows": 1,
            "batch_no": batch_no,
            "batch_id": batch_record_id,
            "result_batch_id": result_entry["id"],
            "checksum": checksum,
            "record_keys": [record["record_key"]],
            "reused_keys": [r["record_key"] for r in original_records],
            "replaced_position_id": covering["id"],
            "synced_at": now,
        }
        registry_entry = {
            "id": self._next_id(REGISTRY_TABLE),
            "dataset": dataset,
            "record_key": record["record_key"],
            "batch_no": batch_no,
            "batch_id": batch_record_id,
            "position_id": position_id,
            "counted_once": True,
            "registered_at": now,
        }

        # 落库顺序与增量捕获一致；替代批次落库后原批次才标记被替代，水位不推进
        store.joint_append(BATCH_TABLE, replacement_batch)
        store.joint_append(POSITION_TABLE, position)
        store.joint_append(REGISTRY_TABLE, registry_entry)
        original_batch["superseded"] = True
        original_batch["superseded_by"] = batch_no
        store.joint_update(BATCH_TABLE, original_batch)

        return {
            "ok": True,
            "mode": "replacement",
            "batch_no": batch_no,
            "position": position,
            "batch": replacement_batch,
            "result_batch_id": result_entry["id"],
            "watermark_advanced": False,
        }

    def _backfill_by_correction(self, sync: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
        """进入当前批次更正清单：保留原业务时间，另发更正批次与同步位置，水位不推进。"""
        sync_id = int(sync["id"])
        dataset = record["dataset"]
        increment_positions = [
            p for p in self._positions_of(sync_id) if p.get("position_type") == BATCH_INCREMENT
        ]
        if not increment_positions:
            return {"ok": False, "error": "当前同步位置还没有增量批次，无法挂接更正清单"}
        current_position = max(increment_positions, key=lambda p: int(p["id"]))
        current_batch = next(
            (b for b in store.joint_rows(BATCH_TABLE) if int(b.get("id", 0)) == int(current_position["batch_id"])),
            None,
        )
        if current_batch is None:
            return {"ok": False, "error": "当前增量批次数据集缺失，无法登记更正清单"}

        now = _now()
        correction_no = f"{current_batch['batch_no']}-C{len(current_batch.get('corrections', [])) + 1}"
        correction_checksum = self._checksum([record])

        position_id = self._next_id(POSITION_TABLE)
        batch_record_id = self._next_id(BATCH_TABLE)
        result_entry, missing = self._result_svc.create_entry({
            "批次编号": correction_no,
            "关联任务": dataset,
            "原始行数": "1",
            "有效行数": "1",
            "存储路径": f"/store/delta/{sync_id}/{correction_no}.json",
            "压缩格式": "snappy",
            "校验摘要": correction_checksum,
            "落地时间": now,
        })
        if result_entry is None:
            return {"ok": False, "error": f"更正批次写入采集仓储失败：缺少 {missing}"}

        correction_item = {
            "record_key": record["record_key"],
            "business_time": record["business_time"],
            "ingest_time": record["ingest_time"],
            "correction_batch_no": correction_no,
            "checksum": correction_checksum,
            "added_at": now,
        }
        correction_batch = {
            "id": batch_record_id,
            "batch_no": correction_no,
            "batch_type": BATCH_CORRECTION,
            "sync_id": sync_id,
            "position_id": position_id,
            "result_batch_id": result_entry["id"],
            "dataset": dataset,
            "incremental_field": current_batch.get("incremental_field", ""),
            "sync_cycle": current_batch.get("sync_cycle", ""),
            "range_from": current_batch["range_from"],
            "range_to": current_batch["range_to"],
            "correction_business_time": record["business_time"],
            "row_count": 1,
            "record_keys": [record["record_key"]],
            "checksum": correction_checksum,
            "attached_batch_id": current_batch["id"],
            "attached_batch_no": current_batch["batch_no"],
            "superseded": False,
            "corrections": [],
            "created_at": now,
        }
        position = {
            "id": position_id,
            "sync_id": sync_id,
            "position_type": BATCH_CORRECTION,
            "last_watermark": sync.get("上次水位", ""),
            "current_watermark": sync.get("当前水位", ""),
            "range_from": current_batch["range_from"],
            "range_to": current_batch["range_to"],
            "correction_business_time": record["business_time"],
            "captured_rows": 1,
            "batch_no": correction_no,
            "batch_id": batch_record_id,
            "result_batch_id": result_entry["id"],
            "checksum": correction_checksum,
            "record_keys": [record["record_key"]],
            "attached_position_id": current_position["id"],
            "synced_at": now,
        }
        registry_entry = {
            "id": self._next_id(REGISTRY_TABLE),
            "dataset": dataset,
            "record_key": record["record_key"],
            "batch_no": correction_no,
            "batch_id": batch_record_id,
            "position_id": position_id,
            "counted_once": True,
            "registered_at": now,
        }

        # 更正批次先落库，再回写当前批次更正清单，水位保持不变
        store.joint_append(BATCH_TABLE, correction_batch)
        store.joint_append(POSITION_TABLE, position)
        store.joint_append(REGISTRY_TABLE, registry_entry)
        current_batch.setdefault("corrections", []).append(correction_item)
        store.joint_update(BATCH_TABLE, current_batch)

        return {
            "ok": True,
            "mode": "correction",
            "batch_no": correction_no,
            "position": position,
            "batch": correction_batch,
            "attached_batch_no": current_batch["batch_no"],
            "result_batch_id": result_entry["id"],
            "watermark_advanced": False,
        }

    # ---------- 查询视图 ----------

    def list_batches(self, sync_id: int | None = None) -> list[dict[str, Any]]:
        rows = store.joint_rows(BATCH_TABLE)
        if sync_id is not None:
            rows = [b for b in rows if int(b.get("sync_id", 0)) == sync_id]
        return list(rows)

    def get_batch(self, batch_id: int) -> dict[str, Any] | None:
        return store.joint_find(BATCH_TABLE, batch_id)

    def list_positions(self) -> list[dict[str, Any]]:
        """同步位置视图：附带源数据集、增量字段、同步周期与批次追溯信息。"""
        enriched: list[dict[str, Any]] = []
        for position in store.joint_rows(POSITION_TABLE):
            item = dict(position)
            sync = store.find(SYNC_MODULE, int(position.get("sync_id", 0)))
            if sync is not None:
                item.setdefault("dataset", sync.get("源数据集", ""))
                item.setdefault("incremental_field", sync.get("增量字段", ""))
                item.setdefault("sync_cycle", sync.get("同步周期", ""))
                item.setdefault("sync_code", sync.get("水位编号", ""))
            item.setdefault("dataset", "")
            item.setdefault("incremental_field", "")
            item.setdefault("sync_cycle", "")
            item.setdefault("sync_code", "")
            enriched.append(item)
        return enriched

    def get_position(self, position_id: int) -> dict[str, Any] | None:
        position = store.joint_find(POSITION_TABLE, position_id)
        if position is None:
            return None
        item = dict(position)
        sync = store.find(SYNC_MODULE, int(position.get("sync_id", 0)))
        if sync is not None:
            item["dataset"] = sync.get("源数据集", "")
            item["incremental_field"] = sync.get("增量字段", "")
            item["sync_cycle"] = sync.get("同步周期", "")
            item["sync_code"] = sync.get("水位编号", "")
        batch = self.get_batch(int(position.get("batch_id", 0)))
        item["batch"] = batch
        return item

    def list_registry(self, dataset: str | None = None) -> list[dict[str, Any]]:
        rows = store.joint_rows(REGISTRY_TABLE)
        if dataset is not None:
            rows = [r for r in rows if r.get("dataset") == dataset]
        return list(rows)


delta_tracker = DeltaTracker()
