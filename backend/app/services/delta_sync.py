"""增量同步 时间线服务：生成作业时间序列和示例数据。"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from app.store import store

MODULE = "delta_sync"
REQUIRED_FIELDS = ["水位编号", "源数据集", "增量字段"]
STATUS_ORDER = ["待初始化", "运行中", "追赶延迟", "已暂停"]
ACTION_RULES = {"初始化水位": "运行中", "启动同步": "已暂停", "暂停同步": "追赶延迟"}
NEGATIVE_ACTIONS = []


class DeltaSyncService:
    def get_timeline(self) -> list[dict[str, Any]]:
        rows = store.rows(MODULE)
        today = date.today()
        items: list[dict] = []
        for i, row in enumerate(rows):
            start = (today - timedelta(days=7 - i * 2)).isoformat()
            end = (today + timedelta(days=3 - i)).isoformat()
            items.append({
                "id": int(row.get("id", i + 1)),
                "name": str(row.get(list(row.keys())[1] if len(row) > 1 else "name", "")),
                "start": start,
                "end": end,
                "status": str(row.get("status", STATUS_ORDER[i % len(STATUS_ORDER)])),
                "logs": f"[{start}] 作业启动\n[{start}] 正在处理...\n[{end}] 任务{'完成' if row.get('status') == STATUS_ORDER[-1] else '进行中'}",
            })
        return items

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def list_configs(self) -> list[dict[str, Any]]:
        """返回同步水位的真实配置：上次/当前水位、增量字段、同步周期与批次统计。"""
        configs: list[dict[str, Any]] = []
        for row in store.rows(MODULE):
            configs.append({
                "id": int(row.get("id", 0)),
                "水位编号": row.get("水位编号", ""),
                "源数据集": row.get("源数据集", ""),
                "增量字段": row.get("增量字段", ""),
                "上次水位": row.get("上次水位", ""),
                "当前水位": row.get("当前水位", ""),
                "同步周期": row.get("同步周期", ""),
                "捕获行数": row.get("捕获行数", "0"),
                "最后同步": row.get("最后同步", ""),
                "status": row.get("status", STATUS_ORDER[0]),
            })
        return configs

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"同步水位 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于增量同步可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"同步水位已{action}"
