"""水位追踪业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "watermark_track"
LIST_FIELDS = ["标记编号", "数据主题", "分区编号", "事件时间水位", "处理时间水位", "延迟记录数", "消费偏移", "检查时间"]
REQUIRED_FIELDS = ["标记编号", "数据主题", "分区编号"]
STATUS_ORDER = ["草稿", "正常", "滞后", "已停用"]
ACTION_RULES = {"标记正常": "正常", "标记滞后": "滞后", "停用追踪": "已停用"}
NEGATIVE_ACTIONS = ["停用追踪"]


class WatermarkTrackService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("标记编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"水位标记 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于水位追踪可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS

        if action == "标记滞后":
            # 读取巡检校正数据结果：停用主题不再触发新阻断。
            correction = self.get_quality_correction(entry.get("数据主题"))
            if correction and correction.get("blocking_suppressed"):
                entry["abnormal"] = False
                entry["阻断状态"] = "已抑制"
                entry["校正批次"] = correction.get("校正批次")
                entry["停用生效点"] = correction.get("停用生效点")
            else:
                entry["abnormal"] = True
                entry["阻断状态"] = "已触发"
                entry["校正批次"] = (correction or {}).get("校正批次")

        return entry, f"水位标记已{action}"

    def get_quality_correction(self, topic_code: str | None) -> dict[str, Any] | None:
        """读取该主题最新的巡检校正数据结果（跨模块交接对象）。"""
        if not topic_code:
            return None
        corrections = [
            row for row in store.rows("quality_inspection_corrections")
            if row.get("topic") == topic_code
        ]
        if not corrections:
            return None
        return corrections[-1]

    def list_quality_corrections(self, topic_code: str | None = None) -> list[dict[str, Any]]:
        """列出巡检校正数据结果，供水位追踪页核对检测窗口。"""
        rows = [dict(row) for row in store.rows("quality_inspection_corrections")]
        if topic_code:
            rows = [row for row in rows if row.get("topic") == topic_code]
        return rows

    def blocking_suppressed(self, topic_code: str | None) -> bool:
        """停用主题在质量侧已阻断抑制时，水位追踪不再触发新阻断。"""
        correction = self.get_quality_correction(topic_code)
        return bool(correction and correction.get("blocking_suppressed"))
