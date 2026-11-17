"""增量捕获链路 HTTP 接口验证（标准库 urllib，配合运行中的 uvicorn）。"""
import json
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8000"
failures: list[str] = []


def call(method: str, path: str, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def check(name, cond, detail=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {name} {detail}")
    if not cond:
        failures.append(name)


# 0. 初始状态：位置与批次为空
_, positions0 = call("GET", "/api/delta_sync/positions")
_, batches0 = call("GET", "/api/delta_sync/batches")
check("初始无同步位置", positions0 == [])
check("初始无增量批次", batches0 == [])

# 1. 配置与源数据
_, configs = call("GET", "/api/delta_sync/configs")
check("配置含真实水位", configs[1]["当前水位"] == "2026-11-10T12:00:00", str(configs[1]))
_, datasets = call("GET", "/api/delta_sync/source/datasets")
check("三个源数据集已注册", {d["dataset"] for d in datasets} ==
      {"orders_order_events", "logs_crawl_ingest", "articles_content_snapshot"})
_, records = call("GET", "/api/delta_sync/source/records?dataset=logs_crawl_ingest")
check("logs 源记录 6 条", len(records) == 6, str(len(records)))

# 2. 捕获 sync 1（orders，水位 0 -> max）
code, r1 = call("POST", "/api/delta_sync/1/capture", {"target_watermark": None})
check("orders 捕获 200", code == 200 and r1["captured_rows"] == 7, str(r1)[:120])
check("批次号 DELTA-1-001", r1["batch_no"] == "DELTA-1-001")

# 3. 同一水位差再捕获 -> 400
code, err1 = call("POST", "/api/delta_sync/1/capture", {})
check("重复捕获 400", code == 400 and "同一水位差" in err1["detail"], str(err1))

# 4. sync 2 logs 区间追赶只命中 2 行
code, r2 = call("POST", "/api/delta_sync/2/capture", {})
check("logs 捕获 2 行", code == 200 and r2["captured_rows"] == 2)
check("logs 区间正确",
      r2["position"]["range_from"] == "2026-11-10T12:00:00"
      and r2["position"]["range_to"] == "2026-11-10T13:15:00")

# 5. sync 3 articles
code, r3 = call("POST", "/api/delta_sync/3/capture", {})
check("articles 捕获 4 行", code == 200 and r3["captured_rows"] == 4)

# 6. 同步位置可回查批次与校验摘要
_, positions = call("GET", "/api/delta_sync/positions")
check("3 个同步位置", len(positions) == 3)
p1 = positions[0]
_, detail = call("GET", f"/api/delta_sync/positions/{p1['id']}")
check("位置回查批次与摘要",
      detail["batch"]["batch_no"] == "DELTA-1-001" and len(detail["checksum"]) == 16
      and detail["dataset"] == "orders_order_events"
      and detail["incremental_field"] == "order_time"
      and detail["sync_cycle"] == "每10分钟")

# 7. 单计校验
_, registry = call("GET", "/api/delta_sync/registry")
keys = [(r["dataset"], r["record_key"]) for r in registry]
check("13 条键登记无重复", len(registry) == 13 and len(keys) == len(set(keys)))

# 8. 采集仓储已先于水位推进写入
with urllib.request.urlopen(BASE + "/api/result_store") as resp:
    store_rows = json.loads(resp.read().decode())["items"]
delta_store = [r for r in store_rows if str(r.get("批次编号", "")).startswith("DELTA-")]
check("仓储有 3 个增量批次", len(delta_store) == 3, str(len(delta_store)))
check("仓储批次带校验摘要", all(len(r.get("校验摘要", "")) == 16 for r in delta_store))

# 9. 迟到记录：先写入源记录（业务时间早于当前水位）
code, late = call("POST", "/api/delta_sync/source/records", {
    "dataset": "orders_order_events", "record_key": "ORD-LATE-1",
    "business_time": "2026-11-10T10:00:00", "ingest_time": "2026-11-17T09:00:00",
    "payload": "迟到订单补录"})
check("迟到源记录写入", code == 200 and late["ok"])

# 9a. 替代批次
code, rep = call("POST", "/api/delta_sync/1/late-records",
                 {"record_key": "ORD-LATE-1", "mode": "replacement"})
check("替代批次 200", code == 200 and rep["batch"]["batch_type"] == "替代", str(rep)[:150])
check("替代批次范围=原区间",
      rep["batch"]["range_from"] == "0" and rep["batch"]["range_to"] == "2026-11-11T14:00:00")

# 9b. 重复计入拒绝
code, dup = call("POST", "/api/delta_sync/1/late-records",
                 {"record_key": "ORD-LATE-1", "mode": "replacement"})
check("重复计入 400", code == 400 and "已计入" in dup["detail"])

# 9c. 更正清单
call("POST", "/api/delta_sync/source/records", {
    "dataset": "orders_order_events", "record_key": "ORD-LATE-2",
    "business_time": "2026-11-11T08:30:00", "ingest_time": "2026-11-17T09:05:00",
    "payload": "迟到退款补录"})
code, cor = call("POST", "/api/delta_sync/1/late-records",
                 {"record_key": "ORD-LATE-2", "mode": "correction"})
check("更正批次 200", code == 200 and cor["batch"]["batch_type"] == "更正")
check("更正保留原业务时间",
      cor["batch"]["correction_business_time"] == "2026-11-11T08:30:00")

# 10. 页面所需汇总视图
_, positions2 = call("GET", "/api/delta_sync/positions")
_, batches2 = call("GET", "/api/delta_sync/batches")
_, registry2 = call("GET", "/api/delta_sync/registry")
check("位置 5 个、批次 5 个、登记 15 条",
      len(positions2) == 5 and len(batches2) == 5 and len(registry2) == 15,
      f"{len(positions2)}/{len(batches2)}/{len(registry2)}")
superseded = [b for b in batches2 if b["superseded"]]
check("原增量批次标记被替代", len(superseded) == 1 and superseded[0]["superseded_by"] == "DELTA-1-001-R1")
corrected = [b for b in batches2 if b["batch_no"] == "DELTA-1-001"][0]
check("更正清单挂在当前批次",
      len(corrected["corrections"]) == 1 and corrected["corrections"][0]["record_key"] == "ORD-LATE-2")

# 11. 水位未被迟到处置推进
_, configs2 = call("GET", "/api/delta_sync/configs")
check("迟到处置不推进水位",
      configs2[0]["当前水位"] == "2026-11-11T14:00:00", configs2[0]["当前水位"])

# 12. 未知资源 404/400
code, nf = call("GET", "/api/delta_sync/positions/9999")
check("未知位置 404", code == 404)
code, bad = call("POST", "/api/delta_sync/999/capture", {})
check("未知同步位 400", code == 400)

# 13. 旧的 pipeline_api 同步接口仍然兼容
_, legacy_pos = call("GET", "/api/pipeline/sync/positions")
check("pipeline_api 位置接口兼容", len(legacy_pos) == 5)
_, legacy_batches = call("GET", "/api/pipeline/sync/batches")
check("pipeline_api 批次接口兼容", len(legacy_batches) == 5)

print()
if failures:
    print("FAILED:", failures)
    raise SystemExit(1)
print("ALL HTTP CHECKS PASSED")
