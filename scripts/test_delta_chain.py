"""增量捕获链路功能验证脚本（不依赖测试框架，直接断言）。"""
import sys

sys.path.insert(0, "/workspace/backend")

from app.store import store
from app.services.delta_tracker import delta_tracker
from app.services.source_records import source_record_service

# 每次运行从空的联合存储开始，保证可重复
for table in ("sync_positions", "delta_batches", "source_registry"):
    store.joint_clear(table)

failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name} {detail}")
    if not cond:
        failures.append(name)


# 1. 首次按水位差捕获 orders（当前水位 0 -> 源最大业务时间）
r1 = delta_tracker.capture_increment(1)
check("orders 首次捕获成功", r1["ok"], str(r1.get("error", "")))
check("orders 捕获 7 行", r1.get("captured_rows") == 7, f"rows={r1.get('captured_rows')}")
check("orders 批次编号递增 DELTA-1-001", r1.get("batch_no") == "DELTA-1-001", r1.get("batch_no", ""))
pos1 = r1["position"]
check("位置区间从0到最大业务时间",
      pos1["range_from"] == "0" and pos1["range_to"] == "2026-11-11T14:00:00",
      f"{pos1['range_from']}~{pos1['range_to']}")
sync1 = store.find("delta_sync", 1)
check("水位在批次落库后才推进", sync1["当前水位"] == "2026-11-11T14:00:00", sync1["当前水位"])
check("采集仓储已写入批次",
      store.find("result_store", r1["result_batch_id"]) is not None)

# 2. 同一水位差只算一次捕获
r1b = delta_tracker.capture_increment(1)
check("重复水位差被拒绝", not r1b["ok"] and "同一水位差" in r1b["error"], r1b.get("error", ""))

# 3. logs 追赶：区间 (12:00, 13:15] 只命中 2 条
r2 = delta_tracker.capture_increment(2)
check("logs 追赶捕获成功", r2["ok"], str(r2.get("error", "")))
check("logs 按水位差只捕获 2 行", r2["captured_rows"] == 2, f"rows={r2['captured_rows']}")
check("logs 区间为 12:00~13:15",
      r2["position"]["range_from"] == "2026-11-10T12:00:00"
      and r2["position"]["range_to"] == "2026-11-10T13:15:00")
check("logs 捕获键为 LOG-005/LOG-006",
      set(r2["batch"]["record_keys"]) == {"LOG-005", "LOG-006"},
      str(r2["batch"]["record_keys"]))

# 4. 自定义目标水位 + 空区间拒绝
r2b = delta_tracker.capture_increment(2, "2026-11-10T13:15:00")
check("目标水位未超前被拒绝", not r2b["ok"])

# 5. articles 首次捕获
r3 = delta_tracker.capture_increment(3)
check("articles 捕获 4 行", r3["ok"] and r3["captured_rows"] == 4, str(r3.get("error", "")))

# 6. 全局单计：registry 中每个 (dataset, key) 只出现一次
registry = delta_tracker.list_registry()
keys = [(x["dataset"], x["record_key"]) for x in registry]
check("registry 总计 13 条", len(registry) == 13, str(len(registry)))
check("源记录无重复计入", len(keys) == len(set(keys)))
check("registry 全部标记 counted_once", all(x["counted_once"] for x in registry))

# 7. 迟到记录：业务时间早于当前水位、采集时间刚到
late, err = source_record_service.add_record(
    "orders_order_events", "ORD-LATE-1", "2026-11-10T10:00:00", "2026-11-17T09:00:00", "迟到订单补录")
check("迟到源记录写入成功", late is not None, err)

# 7a. 无增量批次的同步位不能走更正清单
late2, _ = source_record_service.add_record(
    "articles_content_snapshot", "ART-LATE-X", "2026-11-09T12:00:00", "2026-11-17T09:00:00", "迟到快照")
# articles 已经捕获过，构造一个全新未捕获场景前先验证业务时间晚于水位的拒绝
future_rec, _ = source_record_service.add_record(
    "orders_order_events", "ORD-FUTURE-1", "2026-11-20T10:00:00", "2026-11-17T09:00:00", "未来业务时间")
rf = delta_tracker.backfill_late_record(1, "ORD-FUTURE-1", "correction")
check("业务时间晚于水位不走迟到链路", not rf["ok"], rf.get("error", ""))

# 7b. replacement：回补原业务区间，发布替代批次
rep = delta_tracker.backfill_late_record(1, "ORD-LATE-1", "replacement")
check("替代批次发布成功", rep["ok"], str(rep.get("error", "")))
check("替代批次类型正确", rep["batch"]["batch_type"] == "替代")
check("替代批次覆盖原区间",
      rep["batch"]["range_from"] == "0"
      and rep["batch"]["range_to"] == "2026-11-11T14:00:00")
check("替代批次含 7 条复用 + 1 条新增", rep["batch"]["row_count"] == 8
      and rep["batch"]["new_row_count"] == 1)
check("复用键不重复登记", len(rep["batch"]["reused_keys"]) == 7
      and "ORD-LATE-1" not in rep["batch"]["reused_keys"])
orig_batch = next(b for b in delta_tracker.list_batches(1) if b["batch_no"] == "DELTA-1-001")
check("原批次标记被替代且留痕", orig_batch["superseded"] is True
      and orig_batch.get("superseded_by") == rep["batch_no"])
check("替代批次校验摘要不同", rep["batch"]["checksum"] != orig_batch["checksum"])
check("替代不推进水位", store.find("delta_sync", 1)["当前水位"] == "2026-11-11T14:00:00")

# 7c. 已计入的记录不能再次回补
rep_dup = delta_tracker.backfill_late_record(1, "ORD-LATE-1", "replacement")
check("重复回补被拒绝", not rep_dup["ok"] and "已计入" in rep_dup["error"], rep_dup.get("error", ""))

# 7c-2. 同一区间二次回补：替代链增量 -> R1 -> R2，R2 挂在 R1 上
late1b, _ = source_record_service.add_record(
    "orders_order_events", "ORD-LATE-1B", "2026-11-10T10:30:00", "2026-11-17T09:02:00", "二次迟到补录")
rep2 = delta_tracker.backfill_late_record(1, "ORD-LATE-1B", "replacement")
check("二次替代批次发布成功", rep2["ok"], str(rep2.get("error", "")))
check("二次替代编号为 R2", rep2["batch_no"] == "DELTA-1-001-R2", rep2["batch_no"])
check("R2 挂在 R1 上形成替代链",
      rep2["batch"]["replaced_batch_no"] == "DELTA-1-001-R1",
      rep2["batch"].get("replaced_batch_no", ""))
r1_batch = next(b for b in delta_tracker.list_batches(1) if b["batch_no"] == "DELTA-1-001-R1")
check("R1 已被 R2 标记替代", r1_batch["superseded"] is True
      and r1_batch.get("superseded_by") == "DELTA-1-001-R2")
check("R2 数据集累计 9 行（7+2 迟到）", rep2["batch"]["row_count"] == 9,
      str(rep2["batch"]["row_count"]))
check("R2 复用键不重复登记", len(rep2["batch"]["reused_keys"]) == 8
      and "ORD-LATE-1B" not in rep2["batch"]["reused_keys"])

# 7d. correction：进入当前批次更正清单，保留原业务时间
late3, _ = source_record_service.add_record(
    "orders_order_events", "ORD-LATE-2", "2026-11-11T08:30:00", "2026-11-17T09:05:00", "迟到退款补录")
cor = delta_tracker.backfill_late_record(1, "ORD-LATE-2", "correction")
check("更正批次发布成功", cor["ok"], str(cor.get("error", "")))
check("更正类型与保留业务时间",
      cor["batch"]["batch_type"] == "更正"
      and cor["batch"]["correction_business_time"] == "2026-11-11T08:30:00")
check("更正挂接当前增量批次", cor["attached_batch_no"] == "DELTA-1-001")
updated_current = next(b for b in delta_tracker.list_batches(1) if b["batch_no"] == "DELTA-1-001")
check("当前批次更正清单留痕",
      len(updated_current.get("corrections", [])) == 1
      and updated_current["corrections"][0]["record_key"] == "ORD-LATE-2")
check("更正不推进水位", store.find("delta_sync", 1)["当前水位"] == "2026-11-11T14:00:00")

# 8. 同步位置可回查批次与校验摘要
positions = delta_tracker.list_positions()
check("同步位置总数=6", len(positions) == 6, str(len(positions)))
detail = delta_tracker.get_position(rep["position"]["id"])
check("位置可回查批次与摘要",
      detail is not None and detail["batch"]["batch_no"] == rep["batch_no"]
      and detail["checksum"] == rep["batch"]["checksum"])
check("位置附带数据集/增量字段/周期解释",
      detail["dataset"] == "orders_order_events" and detail["incremental_field"] == "order_time"
      and detail["sync_cycle"] == "每10分钟")

# 9. 单计最终校验：13 原始 + 3 迟到（ORD-LATE-1/1B/2，ART-LATE-X 未计入）= 16
registry_final = delta_tracker.list_registry()
final_keys = [(x["dataset"], x["record_key"]) for x in registry_final]
check("最终登记 16 条且无重复", len(registry_final) == 16
      and len(final_keys) == len(set(final_keys)), str(len(registry_final)))
batches = delta_tracker.list_batches()
check("增量批次数据集共 6 个（3增量+2替代+1更正）", len(batches) == 6, str(len(batches)))
result_count = len(store.rows("result_store"))
check("采集仓储新增 6 个批次（含原种子3个共9个）", result_count == 9, str(result_count))

# 10. 未知数据集/未知同步位的错误路径
check("未知同步位报错", not delta_tracker.capture_increment(999)["ok"])
check("未知记录回补报错", not delta_tracker.backfill_late_record(1, "NO-SUCH-KEY", "correction")["ok"])

print()
if failures:
    print(f"FAILED {len(failures)}:", failures)
    sys.exit(1)
print("ALL CHECKS PASSED")
