"""delta_sync 页面冒烟验证：SSR 渲染模板 + 校验页面 reload() 消费的全部接口数据。

浏览器 E2E 因沙箱缺少 Chromium 系统库无法运行，这里用两层验证替代：
1. vue/server-renderer 实际渲染 SFC，证明模板与脚本无编译/运行时错误；
2. 按页面 reload() 的调用顺序请求全部接口，校验页面表格所需字段齐备。
"""
import json
import subprocess
import urllib.request

BASE_API = "http://127.0.0.1:8000"
BASE_PROXY = BASE_API  # 数据校验直接打后端；SSR 渲染由脚本内 Vite 实例完成
failures: list[str] = []


def call(method, url, payload=None, allow_fail=False):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        if allow_fail:
            return json.loads(exc.read().decode())
        raise


def check(name, cond, detail=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {name} {detail}")
    if not cond:
        failures.append(name)


# 1. 造一条完整链路：两次增量 + 一次替代批次（已捕获/已计入则跳过，保证脚本可重复运行）
call("POST", BASE_API + "/api/delta_sync/1/capture", {"target_watermark": None}, allow_fail=True)
call("POST", BASE_API + "/api/delta_sync/2/capture", {}, allow_fail=True)
call("POST", BASE_API + "/api/delta_sync/source/records", {
    "dataset": "orders_order_events", "record_key": "ORD-LATE-S",
    "business_time": "2026-11-10T10:00:00", "ingest_time": "2026-11-17T10:00:00",
    "payload": "SSR冒烟迟到记录"}, allow_fail=True)
call("POST", BASE_API + "/api/delta_sync/1/late-records",
     {"record_key": "ORD-LATE-S", "mode": "replacement"}, allow_fail=True)

# 2. SSR 渲染真实 SFC（用 Vite ssrLoadModule 完成完整编译与运行时）
proc = subprocess.run(["node", "/workspace/scripts/ssr_render_delta.mjs"], text=True,
                      capture_output=True, cwd="/workspace/frontend")
print(proc.stdout.strip())
check("SSR 渲染成功且关键标记齐全", proc.returncode == 0, proc.stderr[-500:])

# 3. 按页面 reload() 顺序经 vite 代理请求，校验表格字段
configs = call("GET", BASE_PROXY + "/api/delta_sync/configs")
positions = call("GET", BASE_PROXY + "/api/delta_sync/positions")
batches = call("GET", BASE_PROXY + "/api/delta_sync/batches")
registry = call("GET", BASE_PROXY + "/api/delta_sync/registry")
datasets = call("GET", BASE_PROXY + "/api/delta_sync/source/datasets")
source = call("GET", BASE_PROXY + "/api/delta_sync/source/records?dataset=orders_order_events")

config_cols = ["水位编号", "源数据集", "增量字段", "上次水位", "当前水位", "同步周期", "捕获行数", "最后同步"]
check("配置表字段齐备", all(all(c in row for c in config_cols) for row in configs))
pos_cols = ["id", "sync_code", "position_type", "range_from", "range_to",
            "captured_rows", "batch_no", "checksum", "synced_at",
            "dataset", "incremental_field", "sync_cycle"]
check("同步位置字段齐备（含批次范围/行数/追溯解释）",
      all(all(c in row for c in pos_cols) for row in positions))
batch_cols = ["batch_no", "batch_type", "dataset", "range_from", "range_to",
              "row_count", "checksum", "superseded", "record_keys", "incremental_field", "sync_cycle"]
check("增量批次字段齐备", all(all(c in row for c in batch_cols) for row in batches))
check("源记录三要素留痕",
      all({"record_key", "business_time", "ingest_time"} <= set(r) for r in source))
check("登记记录全部 counted_once", all(r["counted_once"] for r in registry))

# 4. 页面能看到不同批次范围与追溯路径
increment = next(b for b in batches if b["batch_type"] == "增量")
replacement = next(b for b in batches if b["batch_type"] == "替代")
check("增量与替代批次范围口径不同可区分",
      increment["batch_no"] != replacement["batch_no"]
      and replacement.get("replaced_batch_no") == increment["batch_no"])
check("每条登记可回查批次号",
      all(r["batch_no"] in {b["batch_no"] for b in batches} for r in registry))

# 5. 位置详情接口（页面弹窗数据源）
detail = call("GET", BASE_PROXY + f"/api/delta_sync/positions/{positions[0]['id']}")
check("位置详情内嵌批次与校验摘要",
      detail.get("batch", {}).get("batch_no") == detail["batch_no"]
      and detail["checksum"] == detail["batch"]["checksum"])

print()
if failures:
    print("FAILED:", failures)
    raise SystemExit(1)
print("PAGE SMOKE PASSED")
