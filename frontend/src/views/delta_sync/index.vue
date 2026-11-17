<template>
  <section class="page" data-module="delta_sync">
    <header class="page-head">
      <div><h2>增量同步</h2><p class="page-desc">按水位差捕获增量：读取上次/当前水位与源记录，按水位区间生成增量批次与同步位置，迟到记录可回补或更正。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记同步水位</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出作业清单</button>
      </div>
    </header>
    <div class="filter-bar">
      <label><span>开始日期</span><input type="date" v-model="dateFrom" /></label>
      <label><span>结束日期</span><input type="date" v-model="dateTo" /></label>
      <button class="btn" type="button" @click="reload">查询</button>
    </div>
    <div class="timeline-wrap">
      <div v-for="item in timelineItems" :key="item.id" class="timeline-row">
        <span class="timeline-label">{{ item.name || item.label }}</span>
        <div class="timeline-track">
          <div class="timeline-bar" :class="`status-${item.status}`" :style="{ left: barLeft(item) + '%', width: barWidth(item) + '%' }" :data-testid="`bar-${item.id}`" :title="`${item.start} ~ ${item.end}`" @click="selectItem(item)">&nbsp;</div>
        </div>
        <span style="font-size:12px;color:var(--muted);margin-left:8px;">{{ item.start }} ~ {{ item.end }}</span>
      </div>
      <p v-if="!timelineItems.length" style="text-align:center;color:var(--muted)">暂无作业记录</p>
    </div>
    <div v-if="selectedItem" class="timeline-log" data-testid="timeline-log">
      <strong>{{ selectedItem.name || selectedItem.label }}</strong> — 状态：{{ selectedItem.status }}
      <div style="margin-top:4px;">{{ selectedItem.logs || '暂无日志' }}</div>
    </div>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <h3 class="section-title">同步水位与增量捕获</h3>
    <table class="data-table">
      <thead>
        <tr><th v-for="column in configColumns" :key="column">{{ column }}</th><th>捕获动作</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in configs" :key="String(row.id)" :data-testid="`config-row-${row.id}`" :data-status="row.status">
          <td v-for="column in configColumns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`capture-${row.id}`" @click="openCapture(row)">按水位差捕获</button>
          </td>
        </tr>
        <tr v-if="!configs.length"><td :colspan="configColumns.length + 1" class="empty-state">暂无同步水位配置</td></tr>
      </tbody>
    </table>

    <h3 class="section-title">同步位置（批次范围与捕获行数）</h3>
    <table class="data-table">
      <thead>
        <tr><th v-for="column in positionColumns" :key="column">{{ column }}</th><th>追溯</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in positions" :key="String(row.id)" :data-testid="`position-row-${row.id}`">
          <td v-for="column in positionColumns" :key="column">
            <span v-if="column === 'position_type'" class="type-badge" :class="`badge-${row.position_type}`">{{ row.position_type }}</span>
            <template v-else>{{ row[column] ?? '—' }}</template>
          </td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`position-detail-${row.id}`" @click="openPosition(row.id)">批次与摘要</button>
          </td>
        </tr>
        <tr v-if="!positions.length"><td :colspan="positionColumns.length + 1" class="empty-state">暂无同步位置，执行一次按水位差捕获后生成</td></tr>
      </tbody>
    </table>

    <h3 class="section-title">增量批次数据集</h3>
    <table class="data-table">
      <thead>
        <tr><th v-for="column in batchColumns" :key="column">{{ column }}</th><th>明细</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in batches" :key="String(row.id)" :data-testid="`batch-row-${row.id}`">
          <td v-for="column in batchColumns" :key="column">
            <span v-if="column === 'batch_type'" class="type-badge" :class="`badge-${row.batch_type}`">{{ row.batch_type }}</span>
            <span v-else-if="column === 'superseded'">{{ row.superseded ? `已被${row.superseded_by ?? '替代'}` : '当前有效' }}</span>
            <template v-else>{{ row[column] ?? '—' }}</template>
          </td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`batch-detail-${row.id}`" @click="openBatch(row.id)">查看批次</button>
          </td>
        </tr>
        <tr v-if="!batches.length"><td :colspan="batchColumns.length + 1" class="empty-state">暂无增量批次数据集</td></tr>
      </tbody>
    </table>

    <h3 class="section-title">源数据记录（业务时间 / 采集时间 / 源记录键）</h3>
    <div class="filter-bar">
      <label class="filter-item"><span>源数据集</span>
        <select v-model="selectedDataset" data-testid="dataset-select" @change="loadSourceRecords">
          <option v-for="ds in datasets" :key="ds.dataset" :value="ds.dataset">{{ ds.dataset }}（增量字段：{{ ds.incremental_field }}）</option>
        </select>
      </label>
      <button class="btn primary" type="button" data-testid="open-source-create" @click="openSourceCreate">写入源记录</button>
    </div>
    <table class="data-table">
      <thead>
        <tr><th v-for="column in sourceColumns" :key="column">{{ column }}</th><th>计入状态</th><th>迟到处置</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in sourceRecords" :key="String(row.id)" :data-testid="`source-row-${row.id}`">
          <td v-for="column in sourceColumns" :key="column">{{ row[column] ?? '—' }}</td>
          <td>
            <span v-if="registryOf(row)" class="type-badge badge-已计入">已计入 {{ registryOf(row)?.batch_no }}</span>
            <span v-else class="type-badge badge-未计入">未计入</span>
          </td>
          <td class="row-actions">
            <template v-if="isLate(row) && !registryOf(row)">
              <button class="link" type="button" :data-testid="`replace-${row.id}`" @click="handleLate(row, 'replacement')">回补替代批次</button>
              <button class="link" type="button" :data-testid="`correct-${row.id}`" @click="handleLate(row, 'correction')">加入更正清单</button>
            </template>
            <span v-else class="muted-text">—</span>
          </td>
        </tr>
        <tr v-if="!sourceRecords.length"><td :colspan="sourceColumns.length + 2" class="empty-state">该数据集暂无源记录</td></tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记同步水位</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>

    <div v-if="captureTarget" class="modal-backdrop" data-testid="capture-modal">
      <form class="modal" @submit.prevent="submitCapture">
        <h3>按水位差捕获增量</h3>
        <dl class="detail-list">
          <dt>源数据集 / 增量字段</dt><dd>{{ captureTarget.源数据集 }} / {{ captureTarget.增量字段 }}</dd>
          <dt>同步周期 / 批次编号口径</dt><dd>{{ captureTarget.同步周期 }} / DELTA-{{ captureTarget.id }}-序号</dd>
          <dt>上次水位</dt><dd>{{ captureTarget.上次水位 }}</dd>
          <dt>当前水位</dt><dd>{{ captureTarget.当前水位 }}</dd>
        </dl>
        <label class="create-field"><span>目标水位（留空取源数据集当前最大业务时间）</span>
          <input v-model="captureWatermark" data-testid="capture-watermark" placeholder="例如 2026-11-12T00:00:00" />
        </label>
        <p class="modal-hint">筛选区间：({{ captureTarget.当前水位 }}, 目标水位]，增量批次写入采集仓储后才推进当前水位。</p>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="captureTarget=null">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-capture">执行捕获</button>
        </div>
      </form>
    </div>

    <div v-if="showSourceModal" class="modal-backdrop" data-testid="source-modal">
      <form class="modal" @submit.prevent="submitSourceRecord">
        <h3>写入源记录</h3>
        <label class="create-field"><span>源数据集</span>
          <select v-model="sourceForm.dataset" required>
            <option v-for="ds in datasets" :key="ds.dataset" :value="ds.dataset">{{ ds.dataset }}</option>
          </select>
        </label>
        <label class="create-field"><span>源记录键</span><input v-model="sourceForm.record_key" data-testid="source-key" required /></label>
        <label class="create-field"><span>业务时间（增量字段取值）</span><input v-model="sourceForm.business_time" data-testid="source-business-time" placeholder="2026-11-10T10:00:00" required /></label>
        <label class="create-field"><span>采集时间（到达采集侧的时间）</span><input v-model="sourceForm.ingest_time" data-testid="source-ingest-time" placeholder="2026-11-17T09:00:00" required /></label>
        <label class="create-field"><span>记录摘要</span><input v-model="sourceForm.payload" placeholder="可选" /></label>
        <p class="modal-hint">业务时间早于当前水位、采集时间为新时间的记录会被识别为迟到记录。</p>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showSourceModal=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-source">写入</button>
        </div>
      </form>
    </div>

    <div v-if="positionDetail" class="modal-backdrop" data-testid="position-modal">
      <div class="modal">
        <h3>同步位置 #{{ positionDetail.id }} 追溯</h3>
        <dl class="detail-list">
          <dt>同步水位 / 类型</dt><dd>{{ positionDetail.sync_code }} / {{ positionDetail.position_type }}</dd>
          <dt>源数据集 / 增量字段 / 周期</dt><dd>{{ positionDetail.dataset }} / {{ positionDetail.incremental_field }} / {{ positionDetail.sync_cycle }}</dd>
          <dt>批次范围</dt><dd>({{ positionDetail.range_from }}, {{ positionDetail.range_to }}]</dd>
          <dt>捕获行数</dt><dd>{{ positionDetail.captured_rows }}</dd>
          <dt>批次编号</dt><dd>{{ positionDetail.batch_no }}</dd>
          <dt>校验摘要</dt><dd class="checksum">{{ positionDetail.checksum }}</dd>
          <dt>源记录键</dt><dd>{{ positionDetail.record_keys?.join('、') || '—' }}</dd>
        </dl>
        <div style="display:flex;justify-content:flex-end;margin-top:12px"><button class="btn primary" type="button" @click="positionDetail=null">关闭</button></div>
      </div>
    </div>

    <div v-if="batchDetail" class="modal-backdrop" data-testid="batch-modal">
      <div class="modal">
        <h3>{{ batchDetail.batch_no }}（{{ batchDetail.batch_type }}批次）</h3>
        <dl class="detail-list">
          <dt>源数据集 / 增量字段 / 同步周期</dt><dd>{{ batchDetail.dataset }} / {{ batchDetail.incremental_field }} / {{ batchDetail.sync_cycle }}</dd>
          <dt>批次范围</dt><dd>({{ batchDetail.range_from }}, {{ batchDetail.range_to }}]</dd>
          <dt>捕获行数</dt><dd>{{ batchDetail.row_count }}<span v-if="batchDetail.new_row_count">（新增 {{ batchDetail.new_row_count }}，复用 {{ (batchDetail.reused_keys || []).length }}）</span></dd>
          <dt>校验摘要</dt><dd class="checksum">{{ batchDetail.checksum }}</dd>
          <dt>源记录键</dt><dd>{{ (batchDetail.record_keys || []).join('、') || '—' }}</dd>
          <dt v-if="batchDetail.replaced_batch_no">替代关系</dt><dd v-if="batchDetail.replaced_batch_no">替代原批次 {{ batchDetail.replaced_batch_no }}</dd>
          <dt v-if="batchDetail.attached_batch_no">更正挂接</dt><dd v-if="batchDetail.attached_batch_no">挂在当前批次 {{ batchDetail.attached_batch_no }} 的更正清单，原业务时间 {{ batchDetail.correction_business_time }}</dd>
          <dt v-if="batchDetail.superseded">批次状态</dt><dd v-if="batchDetail.superseded">已被 {{ batchDetail.superseded_by }} 替代（留痕保留）</dd>
          <dt v-if="batchDetail.corrections?.length">更正清单</dt>
          <dd v-if="batchDetail.corrections?.length">
            <div v-for="c in batchDetail.corrections" :key="c.record_key">{{ c.record_key }}（业务时间 {{ c.business_time }}，更正批次 {{ c.correction_batch_no }}）</div>
          </dd>
        </dl>
        <div style="display:flex;justify-content:flex-end;margin-top:12px"><button class="btn primary" type="button" @click="batchDetail=null">关闭</button></div>
      </div>
    </div>

    <footer class="page-foot">
      <span>{{ positions.length }} 个同步位置 · {{ batches.length }} 个增量批次 · {{ registry.length }} 条源记录键登记</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/delta_sync'
const columns = ["水位编号", "源数据集", "增量字段", "上次水位", "当前水位", "同步周期", "捕获行数", "最后同步"]
const configColumns = ["水位编号", "源数据集", "增量字段", "上次水位", "当前水位", "同步周期", "捕获行数", "最后同步"]
const positionColumns = ["id", "sync_code", "position_type", "range_from", "range_to", "captured_rows", "batch_no", "checksum", "synced_at"]
const batchColumns = ["batch_no", "batch_type", "dataset", "range_from", "range_to", "row_count", "checksum", "superseded"]
const sourceColumns = ["record_key", "business_time", "ingest_time", "payload"]

type SyncConfig = Record<string, string | number> & { id: number; 源数据集: string; 当前水位: string }
type Position = Record<string, any> & { id: number }
type DeltaBatch = Record<string, any> & { id: number }
type SourceRecord = { id: number; dataset: string; record_key: string; business_time: string; ingest_time: string; payload: string; [key: string]: any }
type RegistryEntry = { dataset: string; record_key: string; batch_no: string; counted_once: boolean }
type DatasetInfo = { dataset: string; incremental_field: string; record_count: number }

const timelineItems = ref<any[]>([])
const selectedItem = ref<any>(null)
const dateFrom = ref(''); const dateTo = ref('')
const errorMessage = ref(''); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const allItems = ref<any[]>([])

const configs = ref<SyncConfig[]>([])
const positions = ref<Position[]>([])
const batches = ref<DeltaBatch[]>([])
const registry = ref<RegistryEntry[]>([])
const datasets = ref<DatasetInfo[]>([])
const sourceRecords = ref<SourceRecord[]>([])
const selectedDataset = ref('')

const captureTarget = ref<SyncConfig | null>(null)
const captureWatermark = ref('')
const showSourceModal = ref(false)
const sourceForm = ref({ dataset: '', record_key: '', business_time: '', ingest_time: '', payload: '' })
const positionDetail = ref<Position | null>(null)
const batchDetail = ref<DeltaBatch | null>(null)

const stats = computed(() => [
  { label: '同步位置', value: positions.value.length },
  { label: '增量批次', value: batches.value.length },
  { label: '源记录键登记', value: registry.value.length },
  { label: '被替代批次', value: batches.value.filter(b => b.superseded).length },
])

function barLeft(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 0; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return ((new Date(item.start).getTime() - min) / range) * 100 }
function barWidth(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 20; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return Math.max(5, ((new Date(item.end).getTime() - new Date(item.start).getTime()) / range) * 100) }
function selectItem(item: any) { selectedItem.value = selectedItem.value?.id === item.id ? null : item }

async function getJson(path: string) {
  const res = await request(path)
  if (!res.ok) throw new Error(`接口返回 ${res.status}`)
  return res.json()
}

async function reload() {
  errorMessage.value = ''
  try {
    const data = await getJson(ENDPOINT)
    allItems.value = data.items || []
    applyFilter()
    const [cfg, pos, bch, reg, ds] = await Promise.all([
      getJson(`${ENDPOINT}/configs`),
      getJson(`${ENDPOINT}/positions`),
      getJson(`${ENDPOINT}/batches`),
      getJson(`${ENDPOINT}/registry`),
      getJson(`${ENDPOINT}/source/datasets`),
    ])
    configs.value = cfg
    positions.value = pos
    batches.value = bch
    registry.value = reg
    datasets.value = ds
    if (!selectedDataset.value && ds.length) selectedDataset.value = ds[0].dataset
    await loadSourceRecords()
  } catch (e: any) {
    errorMessage.value = e.message || '加载失败'
  }
}
function applyFilter() {
  let items = allItems.value
  if(dateFrom.value) items = items.filter((i: any) => i.start >= dateFrom.value)
  if(dateTo.value) items = items.filter((i: any) => i.end <= dateTo.value)
  timelineItems.value = items; selectedItem.value = null
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }

async function loadSourceRecords() {
  if (!selectedDataset.value) { sourceRecords.value = []; return }
  sourceRecords.value = await getJson(`${ENDPOINT}/source/records?dataset=${encodeURIComponent(selectedDataset.value)}`)
}

function openCapture(row: SyncConfig) {
  captureTarget.value = row
  captureWatermark.value = ''
}
async function submitCapture() {
  if (!captureTarget.value) return
  try {
    const res = await request(`${ENDPOINT}/${captureTarget.value.id}/capture`, {
      method: 'POST',
      body: JSON.stringify({ target_watermark: captureWatermark.value || null }),
    })
    const payload = await res.json()
    if (!res.ok) throw new Error(payload.detail || '增量捕获失败')
    captureTarget.value = null
    await reload()
  } catch (e: any) {
    errorMessage.value = e.message
  }
}

function openSourceCreate() {
  sourceForm.value = { dataset: selectedDataset.value || datasets.value[0]?.dataset || '', record_key: '', business_time: '', ingest_time: '', payload: '' }
  showSourceModal.value = true
}
async function submitSourceRecord() {
  try {
    const res = await request(`${ENDPOINT}/source/records`, { method: 'POST', body: JSON.stringify(sourceForm.value) })
    const payload = await res.json()
    if (!res.ok || !payload.ok) throw new Error(payload.message || '源记录写入失败')
    showSourceModal.value = false
    selectedDataset.value = sourceForm.value.dataset
    await reload()
  } catch (e: any) {
    errorMessage.value = e.message
  }
}

function registryOf(row: SourceRecord): RegistryEntry | undefined {
  return registry.value.find(r => r.dataset === row.dataset && r.record_key === row.record_key)
}
function configOfDataset(dataset: string): SyncConfig | undefined {
  return configs.value.find(c => c.源数据集 === dataset)
}
function isLate(row: SourceRecord): boolean {
  const cfg = configOfDataset(row.dataset)
  if (!cfg || cfg.当前水位 === '0' || !cfg.当前水位) return false
  return row.business_time < cfg.当前水位
}
async function handleLate(row: SourceRecord, mode: 'replacement' | 'correction') {
  const cfg = configOfDataset(row.dataset)
  if (!cfg) { errorMessage.value = '该数据集没有对应同步水位'; return }
  try {
    const res = await request(`${ENDPOINT}/${cfg.id}/late-records`, {
      method: 'POST',
      body: JSON.stringify({ record_key: row.record_key, mode }),
    })
    const payload = await res.json()
    if (!res.ok) throw new Error(payload.detail || '迟到记录处置失败')
    await reload()
  } catch (e: any) {
    errorMessage.value = e.message
  }
}

async function openPosition(positionId: number) {
  positionDetail.value = await getJson(`${ENDPOINT}/positions/${positionId}`)
}
async function openBatch(batchId: number) {
  const list = await getJson(`${ENDPOINT}/batches`)
  batchDetail.value = list.find((b: DeltaBatch) => b.id === batchId) || null
}

onMounted(reload)
</script>
<style scoped>
.section-title { font-size: 15px; margin: 18px 0 8px; }
.type-badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 12px; white-space: nowrap; }
.badge-增量 { background: #dbeafe; color: #1d4ed8; }
.badge-替代 { background: #fef3c7; color: #b45309; }
.badge-更正 { background: #ede9fe; color: #6d28d9; }
.badge-已计入 { background: #dcfce7; color: #15803d; }
.badge-未计入 { background: #f1f5f9; color: #64748b; }
.muted-text { color: var(--muted); font-size: 12px; }
.detail-list { display: grid; grid-template-columns: 160px 1fr; gap: 6px 12px; margin: 0; font-size: 13px; }
.detail-list dt { color: var(--muted); }
.detail-list dd { margin: 0; word-break: break-all; }
.checksum { font-family: monospace; }
.modal-hint { font-size: 12px; color: var(--muted); margin: 4px 0; }
select { border: 1px solid var(--border); border-radius: 6px; padding: 8px; min-width: 280px; }
</style>
