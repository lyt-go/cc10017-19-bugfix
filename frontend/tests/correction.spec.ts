import { expect, test } from '@playwright/test'

/**
 * 巡检校正数据结果跨模块验收：
 * 停用订单主题质量规则 → 执行巡检不新增死信 → 水位追踪核对同一窗口 → 再次巡检不增长。
 */
test('停用规则后巡检不再新增死信，水位追踪同步校正结果', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', e => errors.push(e.message))
  page.on('response', r => {
    if (r.url().includes('/api/') && r.status() >= 400) errors.push(`${r.status()} ${r.url()}`)
  })

  const deadLetterCount = async () => {
    const response = await page.request.get('/api/dead_letter?size=200')
    const payload = await response.json()
    return payload.items.filter((row: Record<string, string>) => row['原始主题'] === 'TOPI-0001').length
  }

  await page.goto('/stream_quality')
  const ruleRow = page.getByTestId('row-1')
  await expect(ruleRow).toBeVisible()
  await ruleRow.getByRole('button', { name: '停用规则', exact: true }).click()
  await expect(ruleRow).toHaveAttribute('data-status', '已停用')

  await page.getByTestId('inspect-1').click()
  await expect(page.getByTestId('correction-panel')).toBeVisible()
  await expect(page.getByTestId('correction-rule-status')).toHaveText('已停用')
  await expect(page.getByTestId('correction-new-dead-letters')).toHaveText('无')
  await expect(page.getByTestId('correction-suppressed')).toBeVisible()
  await expect(page.getByTestId('correction-preserved')).not.toHaveText('—')
  const countAfterInspect = await deadLetterCount()

  // 同一窗口再次巡检：校正结果复用，死信数量不增长。
  await page.getByTestId('inspect-1').click()
  await expect(page.getByTestId('correction-panel')).toBeVisible()
  await expect.poll(deadLetterCount).toBe(countAfterInspect)

  // 水位追踪：核对窗口并刷新，读到同一份巡检校正数据结果。
  await page.goto('/watermark_track')
  await page.getByTestId('check-window-1').click()
  await expect(page.getByTestId('correction-panel')).toBeVisible()
  await expect(page.getByTestId('correction-topic')).toHaveText('TOPI-0001')
  await expect(page.getByTestId('correction-window')).toHaveText('5min')
  await expect(page.getByTestId('correction-disable-point')).not.toHaveText('—')

  await page.reload()
  await page.getByTestId('check-window-1').click()
  await expect(page.getByTestId('correction-panel')).toBeVisible()
  await expect(page.getByTestId('correction-window')).toHaveText('5min')
  await expect.poll(deadLetterCount).toBe(countAfterInspect)

  expect(errors).toEqual([])
})
