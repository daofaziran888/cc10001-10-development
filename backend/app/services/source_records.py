"""源数据记录服务：模拟各源数据集中可供增量捕获的原始记录。

每条源记录留痕三个关键要素：
- 业务时间（business_time）：源系统中记录实际发生的时间，即增量字段取值；
- 采集时间（ingest_time）：记录到达采集侧的时间，迟到记录靠它识别；
- 源记录键（record_key）：源系统内的唯一键，用于保证一条记录只计入一个批次。

数据保存在内存中；真实项目里这里会替换为源库访问层（CDC 日志、 binlog 或对象存储清单）。
"""
from __future__ import annotations

from typing import Any

# 源数据集 -> 增量字段（业务时间字段）
DATASET_FIELDS: dict[str, str] = {
    "orders_order_events": "order_time",
    "logs_crawl_ingest": "event_time",
    "articles_content_snapshot": "publish_time",
}


def _rec(record_id: int, dataset: str, record_key: str, business_time: str,
         ingest_time: str, payload: str) -> dict[str, Any]:
    return {
        "id": record_id,
        "dataset": dataset,
        "record_key": record_key,
        "business_time": business_time,
        "ingest_time": ingest_time,
        "payload": payload,
    }


_SEED_RECORDS: list[dict[str, Any]] = [
    # orders_order_events：订单事件，业务时间与采集时间基本贴合
    _rec(1, "orders_order_events", "ORD-001", "2026-11-10T08:05:00", "2026-11-10T08:06:00", "订单创建事件"),
    _rec(2, "orders_order_events", "ORD-002", "2026-11-10T09:20:00", "2026-11-10T09:21:00", "订单支付事件"),
    _rec(3, "orders_order_events", "ORD-003", "2026-11-10T11:40:00", "2026-11-10T11:42:00", "订单拆单事件"),
    _rec(4, "orders_order_events", "ORD-004", "2026-11-10T13:05:00", "2026-11-10T13:06:00", "订单发货事件"),
    _rec(5, "orders_order_events", "ORD-005", "2026-11-10T15:30:00", "2026-11-10T15:31:00", "订单签收事件"),
    _rec(6, "orders_order_events", "ORD-006", "2026-11-11T09:00:00", "2026-11-11T09:02:00", "退款申请事件"),
    _rec(7, "orders_order_events", "ORD-007", "2026-11-11T14:00:00", "2026-11-11T14:01:00", "退款完成事件"),
    # logs_crawl_ingest：采集日志，入库存在少量延迟
    _rec(8, "logs_crawl_ingest", "LOG-001", "2026-11-10T08:30:00", "2026-11-10T08:33:00", "页面抓取成功"),
    _rec(9, "logs_crawl_ingest", "LOG-002", "2026-11-10T09:10:00", "2026-11-10T09:15:00", "代理切换记录"),
    _rec(10, "logs_crawl_ingest", "LOG-003", "2026-11-10T10:45:00", "2026-11-10T10:46:00", "验证码识别记录"),
    _rec(11, "logs_crawl_ingest", "LOG-004", "2026-11-10T11:20:00", "2026-11-10T11:25:00", "页面抓取重试"),
    _rec(12, "logs_crawl_ingest", "LOG-005", "2026-11-10T12:30:00", "2026-11-10T12:32:00", "抓取批次汇总"),
    _rec(13, "logs_crawl_ingest", "LOG-006", "2026-11-10T13:15:00", "2026-11-10T13:18:00", "限流退避记录"),
    # articles_content_snapshot：内容快照，以发布时间为业务时间
    _rec(14, "articles_content_snapshot", "ART-001", "2026-11-09T10:00:00", "2026-11-09T10:30:00", "商品详情快照"),
    _rec(15, "articles_content_snapshot", "ART-002", "2026-11-10T10:00:00", "2026-11-10T10:25:00", "评价聚合快照"),
    _rec(16, "articles_content_snapshot", "ART-003", "2026-11-11T10:00:00", "2026-11-11T10:20:00", "库存价格快照"),
    _rec(17, "articles_content_snapshot", "ART-004", "2026-11-12T10:00:00", "2026-11-12T10:30:00", "详情页改版快照"),
]


class SourceRecordService:
    """源数据集读取与写入：增量捕获链路的输入端。"""

    def __init__(self) -> None:
        self._records: list[dict[str, Any]] = [dict(row) for row in _SEED_RECORDS]

    def list_datasets(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for dataset, field in DATASET_FIELDS.items():
            rows = [r for r in self._records if r["dataset"] == dataset]
            result.append({
                "dataset": dataset,
                "incremental_field": field,
                "record_count": len(rows),
                "min_business_time": min((r["business_time"] for r in rows), default=""),
                "max_business_time": max((r["business_time"] for r in rows), default=""),
                "max_ingest_time": max((r["ingest_time"] for r in rows), default=""),
            })
        return result

    def list_records(self, dataset: str | None = None) -> list[dict[str, Any]]:
        if dataset is None:
            return [dict(row) for row in self._records]
        return [dict(row) for row in self._records if row["dataset"] == dataset]

    def get_record(self, dataset: str, record_key: str) -> dict[str, Any] | None:
        for row in self._records:
            if row["dataset"] == dataset and row["record_key"] == record_key:
                return dict(row)
        return None

    def add_record(self, dataset: str, record_key: str, business_time: str,
                   ingest_time: str, payload: str) -> tuple[dict[str, Any] | None, str]:
        if dataset not in DATASET_FIELDS:
            return None, f"源数据集 {dataset} 未注册"
        if not record_key.strip():
            return None, "源记录键不能为空"
        if not business_time.strip() or not ingest_time.strip():
            return None, "业务时间与采集时间不能为空"
        if self.get_record(dataset, record_key) is not None:
            return None, f"源记录键 {record_key} 在 {dataset} 中已存在"
        record = _rec(
            max((int(r["id"]) for r in self._records), default=0) + 1,
            dataset, record_key.strip(), business_time.strip(), ingest_time.strip(),
            payload.strip() or "迟到补录数据",
        )
        self._records.append(record)
        return dict(record), ""

    def scan_interval(self, dataset: str, lower_exclusive: str,
                      upper_inclusive: str) -> list[dict[str, Any]]:
        """按业务时间（增量字段）筛选 (lower, upper] 水位区间内的源记录。"""
        return [
            dict(row) for row in self._records
            if row["dataset"] == dataset
            and lower_exclusive < row["business_time"] <= upper_inclusive
        ]


source_record_service = SourceRecordService()
