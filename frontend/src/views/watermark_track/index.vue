<template>
  <section class="page" data-module="watermark_track">
    <header class="page-head">
      <div>
        <h2>水位追踪管理</h2>
        <p class="page-desc">维护流处理事件时间与处理时间的双水位追踪，为水位追踪提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记水位标记</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出水位追踪清单</button>
      </div>
    </header>

    <div v-if="correction" class="correction-panel" data-testid="correction-panel">
      <div class="correction-head">
        <strong>巡检校正数据结果 {{ correction['校正批次'] }}</strong>
        <span class="correction-status">{{ blockingSuppressed ? '已阻断抑制' : '阻断生效' }}</span>
      </div>
      <dl class="correction-grid">
        <div>
          <dt>对应主题</dt>
          <dd data-testid="correction-topic">{{ correction.topic ?? '—' }}</dd>
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
      </dl>
      <p class="correction-note" data-testid="correction-note">
        {{ blockingSuppressed
          ? '该主题质量规则已停用：水位追踪不再触发新阻断，已生成死信保留。'
          : '质量规则仍生效：水位越界会按阈值触发阻断。' }}
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
              :data-testid="`check-window-${row.id}`"
              @click="checkWindow(row)"
            >
              核对窗口
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无水位追踪数据，可先登记水位标记</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记水位标记</h3>
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
      <span>共 {{ total }} 条水位追踪记录</span>
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
  topic?: string
  window?: string
  停用生效点?: string | null
  preserved_dead_letters?: string[]
}

const ENDPOINT = '/api/watermark_track'
const columns = ["标记编号", "数据主题", "分区编号", "事件时间水位", "处理时间水位", "延迟记录数", "消费偏移", "检查时间"]
const actions = ["标记正常", "标记滞后", "停用追踪"]
const statuses = ["草稿", "正常", "滞后", "已停用"]
const metricNames = ["水位标记总数", "正常水位", "滞后水位"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const correction = ref<Correction | null>(null)
const blockingSuppressed = ref(false)
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

async function loadCorrection(row: Row) {
  correction.value = null
  blockingSuppressed.value = false
  if (row.id == null) return
  try {
    // 页面刷新后重新读取同一份巡检校正数据结果，保证与质量工作台一致。
    const response = await request(`${ENDPOINT}/${row.id}/correction`)
    if (!response.ok) return
    const payload = await response.json()
    correction.value = payload.item ?? null
    blockingSuppressed.value = Boolean(payload.blocking_suppressed)
  } catch {
    correction.value = null
    blockingSuppressed.value = false
  }
}

async function checkWindow(row: Row) {
  await loadCorrection(row)
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
    link.download = 'watermark_track.csv'
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
      throw new Error('水位追踪动作未生效，请稍后重试')
    }
    await reload()
    await loadCorrection(row)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '水位追踪操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('水位标记列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '水位追踪列表读取失败'
  }
}

onMounted(reload)
</script>
