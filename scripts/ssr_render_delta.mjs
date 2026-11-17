// SSR 冒烟：用 Vite 的 ssrLoadModule 真实加载并渲染 delta_sync 页面，验证模板与脚本无运行时错误。
// SSR 不执行 onMounted，数据驱动的 v-for/v-if 标记改从 SFC 源码确认存在。
// 放在仓库 scripts/ 下，通过 createRequire 借用 frontend 的 node_modules 解析 vite/vue。
import { createRequire } from 'node:module'
import { readFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'

const require = createRequire('/workspace/frontend/package.json')
const { createServer } = require('vite')
const { createSSRApp, h } = require('vue')
const { renderToString } = require('vue/server-renderer')

globalThis.window = { open: () => {} }

const vite = await createServer({
  root: '/workspace/frontend',
  server: { middlewareMode: true },
  appType: 'custom',
  logLevel: 'error',
})
try {
  const mod = await vite.ssrLoadModule('/src/views/delta_sync/index.vue')
  const app = createSSRApp({ render: () => h(mod.default) })
  const html = await renderToString(app)

  // 结构性标记：首屏 SSR 必须直接渲染出来
  const staticMarkers = [
    '同步水位与增量捕获', '同步位置（批次范围与捕获行数）', '增量批次数据集',
    '源数据记录（业务时间 / 采集时间 / 源记录键）',
    'data-testid="open-create"', 'data-testid="export"', '按水位差捕获',
  ]
  // 数据驱动标记：onMounted 拉到数据后才渲染，从 SFC 源码确认绑定存在
  const source = await readFile(
    fileURLToPath(new URL('../frontend/src/views/delta_sync/index.vue', import.meta.url)),
    'utf8',
  )
  const dynamicMarkers = ['timeline-bar', '回补替代批次', '加入更正清单',
    'config-row-', 'position-row-', 'batch-row-', 'source-row-']

  const missingStatic = staticMarkers.filter((m) => !html.includes(m))
  const missingDynamic = dynamicMarkers.filter((m) => !source.includes(m))
  console.log('HTML length:', html.length)
  console.log('missing static markers:', JSON.stringify(missingStatic))
  console.log('missing dynamic markers:', JSON.stringify(missingDynamic))
  process.exit(missingStatic.length || missingDynamic.length ? 1 : 0)
} finally {
  await vite.close()
}
