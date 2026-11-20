"""流式质量业务规则：质量规则管理、校验执行与死信联动。

增强：质量规则生效时触发水位检查，异常时生成死信记录。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

MODULE = "stream_quality"
LIST_FIELDS = ["规则编号", "监控主题", "校验类型", "告警阈值", "采样比例", "检测窗口", "阻断动作", "规则版本"]
REQUIRED_FIELDS = ["规则编号", "监控主题", "校验类型"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
NEGATIVE_ACTIONS = ["停用规则"]


class StreamQualityService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("规则编号", "")).casefold()]
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
        # 校验监控主题
        topic_code = values.get("监控主题")
        if topic_code:
            topic = self._find_topic(topic_code)
            if not topic:
                return None, [f"监控主题 {topic_code} 未注册"]

        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry.setdefault("status", STATUS_ORDER[0])
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"质量规则 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于流式质量可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"

        if action == "标记生效":
            topic_code = entry.get("监控主题")
            watermark = self._find_watermark(topic_code)
            if not watermark:
                return None, f"主题 {topic_code} 未配置水位追踪"
            # 执行质量检查：按动作落定后的规则状态判断是否阻断。
            self._run_quality_check(entry, watermark, triggered_by="标记生效", rule_status=target)

        if action == "停用规则":
            # 停用生效点只记录一次，供巡检校正结果冻结规则快照使用。
            entry.setdefault("停用时间", datetime.now().isoformat())

        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS

        if action == "停用规则":
            # 状态落定后再冻结校正结果，确保“停用规则”本身不会补写死信。
            self._freeze_rule_correction(entry)
        elif action == "标记生效":
            # 重新生效时刷新已有校正结果：只重置后续批次，不产生第二个停用生效点。
            from app.pipeline_engine import pipeline_engine

            pipeline_engine.refresh_quality_rule(entry, window=entry.get("检测窗口"))

        return entry, f"质量规则已{action}"

    def _freeze_rule_correction(self, entry: dict[str, Any]) -> dict[str, Any]:
        """停用规则时冻结巡检校正数据结果，阻断后续新增死信。"""
        from app.pipeline_engine import pipeline_engine

        return pipeline_engine.freeze_quality_rule(entry, window=entry.get("检测窗口"))

    def _find_topic(self, topic_code: str) -> dict[str, Any] | None:
        for row in store.rows("topic_registry"):
            if row.get("主题编号") == topic_code:
                return row
        return None

    def _find_watermark(self, topic_code: str) -> dict[str, Any] | None:
        for row in store.rows("watermark_track"):
            if row.get("数据主题") == topic_code:
                return row
        return None

    def _run_quality_check(
        self,
        entry: dict[str, Any],
        watermark: dict[str, Any],
        *,
        triggered_by: str = "标记生效",
        rule_status: str | None = None,
    ) -> dict[str, Any]:
        """执行质量检查，异常时产生死信记录。

        每次检查都重新读取规则当前状态：已停用规则不再产生新的阻断死信，
        历史死信保留，并通过巡检校正数据结果建立校正关系。
        """
        effective_status = rule_status or entry.get("status")
        window = str(entry.get("检测窗口") or "默认窗口").strip()
        threshold, threshold_anomaly = self._parse_metric(entry.get("告警阈值"), "告警阈值")
        late_count, late_anomaly = self._parse_metric(watermark.get("延迟记录数"), "延迟记录数")
        anomalies = [item for item in (threshold_anomaly, late_anomaly) if item]

        violations = []
        # 边界口径：只有严格越界才算超限，等于阈值属于正常范围。
        if not anomalies and late_count is not None and threshold is not None and late_count > threshold:
            violations.append({
                "type": "延迟超限",
                "detail": f"延迟记录 {int(late_count)} 超过阈值 {threshold}",
                "topic": entry.get("监控主题"),
                "window": window,
            })

        disabled = effective_status == "已停用"
        created_dead_letters: list[str] = []
        if violations and not disabled:
            created_dead_letters.append(
                self._create_dead_letter(entry, watermark, "延迟超限", window=window))

        result = {
            "rule_id": entry.get("id"),
            "rule_code": entry.get("规则编号"),
            "topic": entry.get("监控主题"),
            "window": window,
            "rule_status": effective_status,
            "disabled": disabled,
            "threshold": threshold,
            "late_count": late_count,
            "violations": violations,
            "anomalies": anomalies,
            "created_dead_letters": created_dead_letters,
            "triggered_by": triggered_by,
            "checked_at": datetime.now().isoformat(),
            "passed": len(violations) == 0 and not anomalies,
            "overall": "异常" if anomalies else ("告警" if violations else "通过"),
        }
        self._ensure_artifact_table("quality_check_results")
        store.rows("quality_check_results").append(result)
        entry["_last_check"] = result
        return result

    @staticmethod
    def _parse_metric(value: Any, label: str) -> tuple[float | None, str | None]:
        """解析阈值/计数；非法或越界值只作异常说明，不当作正常结果。"""
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None, f"{label}「{value}」不是有效数值，仅作异常说明"
        if number < 0:
            return None, f"{label}「{value}」为负数，超出有效口径，仅作异常说明"
        return number, None

    def _create_dead_letter(
        self,
        entry: dict[str, Any],
        watermark: dict[str, Any],
        reason: str,
        *,
        window: str | None = None,
    ) -> str:
        """按“规则 + 检测窗口”幂等写死信，重复检查同一窗口不新增记录。"""
        rule_code = entry.get("规则编号")
        window = window or str(entry.get("检测窗口") or "默认窗口")
        for row in store.rows("dead_letter"):
            if (
                row.get("原始主题") == entry.get("监控主题")
                and row.get("关联规则编号") == rule_code
                and row.get("关联检测窗口") == window
            ):
                return str(row.get("记录编号"))

        dl_rows = store.rows("dead_letter")
        dl_id = max((int(r.get("id", 0)) for r in dl_rows), default=0) + 1
        dl = {
            "id": dl_id,
            "status": "待处置",
            "pending": True,
            "abnormal": True,
            "记录编号": f"DEAD-{dl_id:04d}",
            "原始主题": entry.get("监控主题"),
            "原始分区": watermark.get("分区编号", "0"),
            "失败原因": reason,
            "原始消息体": f"延迟记录数:{watermark.get('延迟记录数')}",
            "重试次数": "0",
            "入库时间": datetime.now().isoformat(),
            "处置状态": "待分析",
            "关联规则编号": rule_code,
            "关联检测窗口": window,
            "阻断来源": "质量规则生效",
        }
        dl_rows.append(dl)
        return dl["记录编号"]

    def get_quality_results(self, entry_id: int) -> list[dict[str, Any]]:
        self._ensure_artifact_table("quality_check_results")
        return [r for r in store.rows("quality_check_results") if r.get("rule_id") == entry_id]

    def _ensure_artifact_table(self, name: str) -> None:
        store.rows(name)
