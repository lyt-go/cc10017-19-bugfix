<template>
  <section class="page" data-module="stream_quality">
    <header class="page-head">
      <div>
        <h2>流式质量管理</h2>
        <p class="page-desc">维护流式数据实时完整性、延迟与分布的质量监控规则，为流式质量提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记质量规则</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出流式质量清单</button>
      </div>
    </header>

    <div v-if="correction" class="correction-panel" data-testid="correction-panel">
      <div class="correction-head">
        <strong>巡检校正数据结果 {{ correction['校正批次'] }}</strong>
        <span class="correction-status" :data-testid="'correction-status'">{{ correction.status }}</span>
      </div>
      <dl class="correction-grid">
        <div>
          <dt>规则状态</dt>
          <dd data-testid="correction-rule-status">{{ correction.rule_status ?? '—' }}</dd>
        </div>
        <div>
          <dt>检测窗口</dt>
          <dd data-testid="correction-window">{{ correction.window ?? '—' }}</dd>
        </div>
        <div>
          <dt>停用生效点</dt>
          <dd data-testid="correction-disable-point">{{ correction['停用生效点'] ?? '—' }}</dd>
        </div>
        <div>
          <dt>保留已阻断死信</dt>
          <dd data-testid="correction-preserved">{{ (correction.preserved_dead_letters ?? []).join('、') || '—' }}</dd>
        </div>
        <div>
          <dt>本窗口新增死信</dt>
          <dd data-testid="correction-new-dead-letters">{{ (correction.new_dead_letters ?? []).join('、') || '无' }}</dd>
        </div>
        <div>
          <dt>待续做批次</dt>
          <dd data-testid="correction-remaining">{{ (correction['续做批次'] ?? []).join('、') || '无' }}</dd>
        </div>
      </dl>
      <p v-if="correction.blocking_suppressed" class="correction-note" data-testid="correction-suppressed">
        规则已停用：不再产生新的阻断死信，已生成记录保留并建立校正关系。
      </p>
    </div>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button
              class="link"
              type="button"
              :data-testid="`inspect-${row.id}`"
              @click="inspect(row)"
            >
              执行巡检
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无流式质量数据，可先登记质量规则</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记质量规则</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条流式质量记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

type Correction = {
  id?: number
  校正批次?: string
  status?: string
  rule_status?: string
  window?: string
  停用生效点?: string | null
  blocking_suppressed?: boolean
  preserved_dead_letters?: string[]
  new_dead_letters?: string[]
  续做批次?: string[]
}

const ENDPOINT = '/api/stream_quality'
const columns = ["规则编号", "监控主题", "校验类型", "告警阈值", "采样比例", "检测窗口", "阻断动作", "规则版本"]
const actions = ["提交校验", "标记生效", "停用规则"]
const statuses = ["草稿", "待校验", "已生效", "已停用"]
const metricNames = ["质量规则总数", "待校验规则", "已生效规则"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const correction = ref<Correction | null>(null)
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

async function loadCorrection(row: Row) {
  correction.value = null
  if (row.id == null) return
  try {
    const response = await request(`${ENDPOINT}/${row.id}/correction`)
    if (!response.ok) return
    const payload = await response.json()
    correction.value = payload.item ?? null
  } catch {
    correction.value = null
  }
}

async function inspect(row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/inspect`, { method: 'POST' })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '巡检失败')
    await reload()
    await loadCorrection(row)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '巡检失败'
  }
}

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'stream_quality.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('流式质量动作未生效，请稍后重试')
    }
    await reload()
    await loadCorrection(row)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '流式质量操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('质量规则列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '流式质量列表读取失败'
  }
}

onMounted(reload)
</script>
