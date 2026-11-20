"""数据管道引擎：实现跨模块的真实数据流转与链路编排。

核心链路：
1. 实时接入链：主题注册 → Kafka接入 → Schema演进 → Flink计算
2. CDC入湖链：CDC捕获 → UPSERT合并 → 湖分区 → Iceberg管理
3. 质量容错链：水位追踪 → 流式质量 → 死信处理（产物：巡检校正数据结果）
4. 数据消费链：流式血缘 → 物化视图 → 下沉分发
5. 维护链：流式补数 → 合并清理 → 生命周期
6. 实时指标链：消费主题+Flink结果 → 实时指标
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from app.store import store


class PipelineEngine:
    """编排多模块数据链，维护链路状态与产物。"""

    # 链路类型常量
    INGEST_CHAIN = "realtime_ingest"
    CDC_CHAIN = "cdc_to_lake"
    QUALITY_CHAIN = "quality_tolerance"
    CONSUME_CHAIN = "data_consumption"
    MAINTAIN_CHAIN = "maintenance"
    METRICS_CHAIN = "realtime_metrics"

    def __init__(self) -> None:
        self._pipelines: dict[str, list[dict[str, Any]]] = {}

    # ───────────────────────────────
    # 实时接入链
    # ───────────────────────────────

    def ingest_chain_status(self, topic_code: str) -> dict[str, Any]:
        """查询实时接入链全链路状态：主题→Kafka→Schema→Flink。"""
        topic = self._find_by_field("topic_registry", "主题编号", topic_code)
        kafka = self._find_by_field("kafka_connector", "配置编号", topic_code.replace("TOPI", "KAFK")) if topic else None
        schema = self._find_by_field("schema_evolve", "关联主题", topic_code) if topic else None
        flink = self._find_by_field("flink_job", "消费主题", topic_code) if topic else None

        return {
            "topic": {"exists": topic is not None, "status": topic.get("status") if topic else None, "entry": topic},
            "kafka_connector": {"exists": kafka is not None, "status": kafka.get("status") if kafka else None, "entry": kafka},
            "schema_evolve": {"exists": schema is not None, "status": schema.get("status") if schema else None, "entry": schema},
            "flink_job": {"exists": flink is not None, "status": flink.get("status") if flink else None, "entry": flink},
            "chain_ready": all([
                topic is not None and topic.get("status") == "已上线",
                kafka is not None and kafka.get("status") == "已生效",
                schema is not None and schema.get("status") == "已生效",
                flink is not None and flink.get("status") == "运行中",
            ]),
        }

    def create_ingest_pipeline(self, topic_code: str, topic_name: str, msg_format: str) -> dict[str, Any]:
        """为主题创建完整的实时接入链路记录。"""
        pipeline_id = f"INGEST-{topic_code}"
        entry = {
            "pipeline_id": pipeline_id,
            "chain_type": self.INGEST_CHAIN,
            "topic_code": topic_code,
            "created_at": datetime.now().isoformat(),
            "stages": [
                {"module": "topic_registry", "status": "草稿", "artifact": topic_code},
                {"module": "kafka_connector", "status": "待创建", "artifact": None},
                {"module": "schema_evolve", "status": "待创建", "artifact": None},
                {"module": "flink_job", "status": "待创建", "artifact": None},
            ],
            "overall_status": "草稿",
        }
        self._pipelines.setdefault(self.INGEST_CHAIN, []).append(entry)
        return entry

    def advance_ingest_stage(self, topic_code: str, stage_module: str, new_status: str, artifact: str | None = None) -> dict[str, Any] | None:
        """推进实时接入链的某个阶段状态。"""
        for pipe in self._pipelines.get(self.INGEST_CHAIN, []):
            if pipe.get("topic_code") == topic_code:
                for stage in pipe["stages"]:
                    if stage["module"] == stage_module:
                        stage["status"] = new_status
                        if artifact:
                            stage["artifact"] = artifact
                        break
                # 重新计算整体状态
                statuses = [s["status"] for s in pipe["stages"]]
                if all(s == "已完成" for s in statuses):
                    pipe["overall_status"] = "运行中"
                elif any(s in ("失败", "异常") for s in statuses):
                    pipe["overall_status"] = "异常"
                else:
                    pipe["overall_status"] = "进行中"
                return pipe
        return None

    # ───────────────────────────────
    # CDC入湖链
    # ───────────────────────────────

    def cdc_chain_status(self, source_db: str, source_table: str) -> dict[str, Any]:
        """查询CDC入湖链：CDC→UPSERT→分区→Iceberg。"""
        cdc = self._find_by_field("change_capture", "源数据库", source_db)
        if cdc and cdc.get("源表名") != source_table:
            cdc = None
        target_topic = cdc.get("目标主题") if cdc else None
        upsert = self._find_by_field("upsert_merge", "目标表名", source_table) if cdc else None
        partition = self._find_by_field("lake_partition", "目标表名", source_table) if cdc else None
        iceberg = self._find_by_field("iceberg_table", "表名称", source_table) if cdc else None

        return {
            "cdc": {"exists": cdc is not None, "status": cdc.get("status") if cdc else None, "entry": cdc},
            "upsert_merge": {"exists": upsert is not None, "status": upsert.get("status") if upsert else None, "entry": upsert},
            "lake_partition": {"exists": partition is not None, "status": partition.get("status") if partition else None, "entry": partition},
            "iceberg_table": {"exists": iceberg is not None, "status": iceberg.get("status") if iceberg else None, "entry": iceberg},
            "target_topic": target_topic,
            "chain_ready": all([
                cdc is not None and cdc.get("status") == "运行中",
                upsert is not None and upsert.get("status") == "已生效",
                partition is not None and partition.get("status") == "已生效",
                iceberg is not None and iceberg.get("status") in ("已就绪", "运行中"),
            ]),
        }

    def run_cdc_upsert_simulation(self, cdc_id: int) -> dict[str, Any]:
        """模拟CDC捕获后的增量合并：产生合并结果记录。"""
        cdc = store.find("change_capture", cdc_id)
        if not cdc:
            return {"ok": False, "error": f"CDC配置 {cdc_id} 不存在"}

        source_table = cdc.get("源表名", "unknown")
        upsert = self._find_by_field("upsert_merge", "目标表名", source_table)
        if not upsert:
            return {"ok": False, "error": f"源表 {source_table} 未配置合并策略"}

        merge_key = upsert.get("合并键列", "id")
        # 模拟产生合并产物
        merged_records = []
        for i in range(3):
            merged_records.append({
                "merge_key": f"{merge_key}-{(i+1):04d}",
                "operation": ["INSERT", "UPDATE", "DELETE"][i % 3],
                "source_table": source_table,
                "cdc_config_id": cdc_id,
                "merged_at": datetime.now().isoformat(),
            })

        # 写入upsert_merge模块的附属产物表
        self._ensure_artifact_table("upsert_merge_artifacts")
        for rec in merged_records:
            store.rows("upsert_merge_artifacts").append(rec)

        return {
            "ok": True,
            "cdc_id": cdc_id,
            "source_table": source_table,
            "merge_key": merge_key,
            "merged_count": len(merged_records),
            "artifacts": merged_records,
        }

    def apply_partition_to_iceberg(self, table_name: str) -> dict[str, Any]:
        """将分区策略应用到Iceberg表：跨模块数据产物。"""
        partition = self._find_by_field("lake_partition", "目标表名", table_name)
        iceberg = self._find_by_field("iceberg_table", "表名称", table_name)
        if not partition or not iceberg:
            return {"ok": False, "error": "分区定义或Iceberg表缺失"}

        # 生成分区元数据产物
        partition_spec = {
            "table_name": table_name,
            "partition_column": partition.get("分区列名"),
            "granularity": partition.get("分区粒度"),
            "format": partition.get("文件格式"),
            "iceberg_metadata_version": iceberg.get("元数据版本"),
            "applied_at": datetime.now().isoformat(),
        }
        self._ensure_artifact_table("iceberg_partition_specs")
        store.rows("iceberg_partition_specs").append(partition_spec)

        return {"ok": True, "spec": partition_spec}

    # ───────────────────────────────
    # 质量容错链
    # ───────────────────────────────

    def quality_chain_check(self, topic_code: str) -> dict[str, Any]:
        """检查质量容错链状态：水位→质量规则→死信。

        规则停用后以巡检校正数据结果为准：停用主题不再产生新阻断，
        已生成死信保留，并返回与停用规则的校正关系。
        """
        watermark = self._find_by_field("watermark_track", "数据主题", topic_code)
        quality = self._find_quality_rule(topic_code)
        dead_letters = [r for r in store.rows("dead_letter") if r.get("原始主题") == topic_code]
        corrections = self.list_quality_corrections(topic_code=topic_code)
        correction = corrections[-1] if corrections else None

        late_count, _ = self._parse_metric(watermark.get("延迟记录数") if watermark else None, "延迟记录数")
        threshold_val, _ = self._parse_metric(quality.get("告警阈值") if quality else None, "告警阈值")
        late_count = late_count or 0.0
        threshold_val = threshold_val or 0.0
        disabled = bool(quality and quality.get("status") == "已停用")
        # 只有当前规则就是停用规则、且校正结果已是该规则时才抑制阻断，
        # 避免同主题另有生效规则时被历史停用规则误抑制。
        suppressed = bool(
            disabled
            and correction
            and int(correction.get("rule_id", 0)) == int(quality.get("id", 0))
            and correction.get("blocking_suppressed")
        )
        quality_alert = (not suppressed) and late_count > threshold_val

        return {
            "watermark": {"exists": watermark is not None, "status": watermark.get("status") if watermark else None,
                          "late_count": late_count, "entry": watermark},
            "stream_quality": {"exists": quality is not None, "status": quality.get("status") if quality else None,
                               "threshold": threshold_val, "disabled": disabled, "entry": quality},
            "dead_letters": {"count": len(dead_letters), "entries": dead_letters},
            "correction": correction,
            "corrections": corrections,
            "blocking_suppressed": suppressed,
            "quality_alert": quality_alert,
            "chain_healthy": watermark is not None and quality is not None and not quality_alert and len(dead_letters) < 3,
        }

    # 巡检校正结果的分批执行顺序。批次状态会落库，中断后只续做未完成批次。
    CORRECTION_TABLE = "quality_inspection_corrections"
    CORRECTION_BATCHES = [
        "冻结规则快照",
        "读取检测窗口",
        "关联已阻断死信",
        "阻断新增死信",
        "同步水位追踪",
    ]

    def run_quality_inspection(
        self,
        topic_code: str,
        window: str | None = None,
        rule_id: int | None = None,
    ) -> dict[str, Any]:
        """执行质量巡检，并生成/续做巡检校正数据结果。

        关键口径：
        - 每次都重新读取该主题当前的质量规则，不使用缓存副本；
        - 规则已停用时不再产生新的阻断死信，只保留已生成的死信并建立校正关系；
        - 同一规则、同一检测窗口只保留一个校正结果与一个停用生效点；
        - 校正结果按批次推进，中断后再次巡检只续做未完成批次。
        """
        watermark = self._find_by_field("watermark_track", "数据主题", topic_code)
        quality = self._find_quality_rule(topic_code, rule_id)

        if not watermark or not quality:
            return {"ok": False, "error": "水位追踪或质量规则未配置"}
        if quality.get("status") not in ("已生效", "已停用"):
            return {
                "ok": False,
                "error": f"质量规则 {quality.get('规则编号')} 状态为「{quality.get('status')}」，"
                         f"未生效规则不参与巡检",
            }

        window = str(window or quality.get("检测窗口") or "默认窗口").strip()
        late_count, late_anomaly = self._parse_metric(watermark.get("延迟记录数"), "延迟记录数")
        threshold, threshold_anomaly = self._parse_metric(quality.get("告警阈值"), "告警阈值")
        anomalies = [item for item in (late_anomaly, threshold_anomaly) if item]

        # 边界口径：只有严格越界才算超限，等于阈值属于正常范围；
        # 越界/非法值只写异常说明，不落成“通过”的正常结果。
        violations: list[dict[str, Any]] = []
        if late_anomaly is None and threshold_anomaly is None and late_count > threshold:
            violations.append({
                "type": "延迟超限",
                "detail": f"延迟记录 {int(late_count)} 超过阈值 {threshold}",
                "topic": topic_code,
                "window": window,
                "timestamp": datetime.now().isoformat(),
            })

        disabled = quality.get("status") == "已停用"
        correction = self.ensure_quality_correction(quality, window=window)

        # 续做批次：已完成的批次不会重复执行，避免重复关联/重复写死信。
        run_info = self._resume_correction_batches(
            correction,
            quality,
            watermark,
            late_count=late_count,
            threshold=threshold,
            violations=violations,
            anomalies=anomalies,
            disabled=disabled,
        )

        dead_letters = [r for r in store.rows("dead_letter") if r.get("原始主题") == topic_code]
        pending_dead_letters = [r for r in dead_letters if r.get("status") == "待处置"]

        # 写入质量检查结果产物
        self._ensure_artifact_table("quality_inspection_results")
        if anomalies:
            overall = "异常"
        elif violations and disabled:
            # 停用规则不再阻断，但越界事实仍要留痕，不能保存为“通过”。
            overall = "已阻断"
        elif violations:
            overall = "告警"
        else:
            overall = "通过"
        result = {
            "topic": topic_code,
            "rule_id": quality.get("id"),
            "rule_code": quality.get("规则编号"),
            "rule_status": quality.get("status"),
            "window": window,
            "disabled": disabled,
            "threshold": threshold,
            "late_count": late_count,
            "inspected_at": datetime.now().isoformat(),
            "violation_count": len(violations),
            "violations": violations,
            "anomalies": anomalies,
            "overall": overall,
            "passed": overall == "通过",
            "blocking_suppressed": disabled,
            "correction_code": correction.get("校正批次"),
            "correction_id": correction.get("id"),
            "correction": correction,
            "dead_letter_count": len(dead_letters),
            "pending_dead_letter_count": len(pending_dead_letters),
            "preserved_dead_letters": list(correction.get("preserved_dead_letters", [])),
            "new_dead_letters": list(run_info.get("new_dead_letters", [])),
            "suppressed_dead_letters": run_info.get("suppressed_dead_letters", []),
            "resumed_batches": run_info.get("resumed_batches", []),
            "interrupted_batch": run_info.get("interrupted_batch"),
        }
        store.rows("quality_inspection_results").append(result)

        return {"ok": True, "result": result, "correction": correction}

    def freeze_quality_rule(self, rule: dict[str, Any], window: str | None = None) -> dict[str, Any]:
        """规则停用时建立唯一的停用生效点，并冻结校正批次。

        只推进“冻结规则快照”和“关联已阻断死信”两个批次：停用动作本身
        不补写死信；其余批次留给后续巡检续做。
        """
        window = str(window or rule.get("检测窗口") or "默认窗口").strip()
        correction = self.ensure_quality_correction(rule, window=window)
        for index in (0, 2):
            if correction["batches"][index].get("status") != "已完成":
                self._run_correction_batch(correction, index, rule, None)
        remaining = [b["名称"] for b in correction["batches"] if b.get("status") != "已完成"]
        # 停用即刻对水位追踪生效：同步水位批次仍留待巡检续做并记录时间。
        correction["blocking_suppressed"] = rule.get("status") == "已停用"
        correction["status"] = "进行中" if remaining else "已完成"
        correction["updated_at"] = datetime.now().isoformat()
        correction["续做批次"] = remaining
        return correction

    def refresh_quality_rule(self, rule: dict[str, Any], window: str | None = None) -> dict[str, Any] | None:
        """规则状态变化时刷新已有校正结果；没有校正结果时不新建。"""
        window = str(window or rule.get("检测窗口") or "默认窗口").strip()
        existed = self.get_quality_correction(int(rule.get("id", 0)), window=window)
        if existed is None:
            return None
        correction = self.ensure_quality_correction(rule, window=window)
        remaining = [b["名称"] for b in correction["batches"] if b.get("status") != "已完成"]
        correction["status"] = "进行中" if remaining else "已完成"
        correction["续做批次"] = remaining
        correction["updated_at"] = datetime.now().isoformat()
        return correction

    def ensure_quality_correction(self, rule: dict[str, Any], window: str) -> dict[str, Any]:
        """按“规则 + 检测窗口”取唯一校正结果；重复调用返回同一条记录。

        无论“先冻结规则再处理窗口”还是“先处理窗口再冻结规则”，都收敛到
        同一条校正结果：规则状态变化时重跑取快照/关联死信等批次，
        但不会新建第二个停用生效点，也不会重复补写死信。
        """
        rows = store.rows(self.CORRECTION_TABLE)
        rule_id = int(rule.get("id", 0))
        topic_code = rule.get("监控主题")
        for row in rows:
            if int(row.get("rule_id", 0)) == rule_id and row.get("window") == window:
                self._refresh_correction_for_rule_change(row, rule)
                return row

        correction_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        correction_code = f"QCORR-{correction_id:04d}"
        correction = {
            "id": correction_id,
            "校正批次": correction_code,
            "topic": topic_code,
            "window": window,
            "rule_id": rule_id,
            "rule_code": rule.get("规则编号"),
            "rule_status": rule.get("status"),
            "rule_snapshot": self._rule_snapshot(rule),
            "停用生效点": None,
            "disable_effective_at": None,
            "preserved_dead_letters": [],
            "preserved_dead_letter_ids": [],
            "new_dead_letters": [],
            "suppressed_dead_letters": [],
            "anomalies": [],
            "batches": [
                {
                    "批次号": f"{correction_code}-B{index + 1}",
                    "名称": name,
                    "status": "待执行",
                    "processed_at": None,
                }
                for index, name in enumerate(self.CORRECTION_BATCHES)
            ],
            "status": "进行中",
            "version": 1,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
        }
        rows.append(correction)
        return correction

    def _refresh_correction_for_rule_change(self, correction: dict[str, Any], rule: dict[str, Any]) -> None:
        """规则状态变化时只重置受影响批次，保持校正结果唯一、版本可追溯。"""
        previous = correction.get("rule_status")
        current = rule.get("status")
        if previous == current:
            return
        affected = {"冻结规则快照", "关联已阻断死信", "阻断新增死信", "同步水位追踪"}
        for batch in correction.get("batches", []):
            if batch.get("名称") in affected and batch.get("status") == "已完成":
                batch["status"] = "待执行"
                batch["processed_at"] = None
        correction["status"] = "进行中"
        correction["rule_status"] = current
        correction["blocking_suppressed"] = current == "已停用"
        correction["version"] = int(correction.get("version", 1)) + 1
        correction["status_changed_at"] = datetime.now().isoformat()
        correction["previous_rule_status"] = previous

    def list_quality_corrections(
        self,
        topic_code: str | None = None,
        rule_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """按主题/规则读取巡检校正数据结果，保持创建顺序稳定。"""
        rows = store.rows(self.CORRECTION_TABLE)
        items = [dict(row) for row in rows]
        if topic_code:
            items = [row for row in items if row.get("topic") == topic_code]
        if rule_id is not None:
            items = [row for row in items if int(row.get("rule_id", 0)) == int(rule_id)]
        return items

    def get_quality_correction(
        self,
        rule_id: int,
        window: str | None = None,
    ) -> dict[str, Any] | None:
        """读取某条规则最近一次巡检校正结果。"""
        items = self.list_quality_corrections(rule_id=rule_id)
        if window is not None:
            items = [row for row in items if row.get("window") == str(window)]
        if not items:
            return None
        return items[-1]

    def _rule_snapshot(self, rule: dict[str, Any]) -> dict[str, Any]:
        fields = ["规则编号", "监控主题", "校验类型", "告警阈值", "采样比例", "检测窗口", "阻断动作", "规则版本"]
        return {field: rule.get(field) for field in fields}

    def _find_quality_rule(self, topic_code: str, rule_id: int | None = None) -> dict[str, Any] | None:
        """重新从仓库读取主题的质量规则；指定规则时校验归属主题。

        主题级口径：优先取已生效规则；没有生效规则时以已停用规则为准
        （停用主题不再触发新阻断）；最后才回落到待校验/草稿。
        绝不复用上一次巡检留下的规则副本。
        """
        if rule_id is not None:
            rule = store.find("stream_quality", int(rule_id))
            if rule is None or rule.get("监控主题") != topic_code:
                return None
            return rule
        candidates = [row for row in store.rows("stream_quality") if row.get("监控主题") == topic_code]
        if not candidates:
            return None
        priority = {"已生效": 0, "已停用": 1, "待校验": 2, "草稿": 3}
        return min(
            candidates,
            key=lambda row: (priority.get(str(row.get("status")), 9), -int(row.get("id", 0))),
        )

    @staticmethod
    def _parse_metric(value: Any, label: str) -> tuple[float | None, str | None]:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None, f"{label}「{value}」不是有效数值，仅作异常说明"
        if number < 0:
            return None, f"{label}「{value}」为负数，超出有效口径，仅作异常说明"
        return number, None

    def _disable_point(self, rule: dict[str, Any], correction: dict[str, Any]) -> str:
        """停用生效点只落一次：停用时间优先，其次复用该规则已有的校正点。"""
        existing = correction.get("停用生效点")
        if existing:
            return existing
        if rule.get("停用时间"):
            return str(rule["停用时间"])
        for row in store.rows(self.CORRECTION_TABLE):
            if int(row.get("rule_id", 0)) == int(rule.get("id", 0)) and row.get("停用生效点"):
                return str(row["停用生效点"])
        return datetime.now().isoformat()

    def _resume_correction_batches(
        self,
        correction: dict[str, Any],
        rule: dict[str, Any],
        watermark: dict[str, Any] | None,
        *,
        late_count: float | None,
        threshold: float | None,
        violations: list[dict[str, Any]],
        anomalies: list[str],
        disabled: bool,
    ) -> dict[str, Any]:
        """只执行未完成批次；已完成批次不重复执行。"""
        batches = correction.setdefault("batches", [])
        completed = [b["名称"] for b in batches if b.get("status") == "已完成"]
        pending_indexes = [i for i, b in enumerate(batches) if b.get("status") != "已完成"]
        resumed = [batches[i]["名称"] for i in pending_indexes]
        processed: list[str] = []
        new_dead_letters: list[str] = []
        suppressed: list[dict[str, Any]] = []
        interrupted: str | None = None

        for index in pending_indexes:
            try:
                outcome = self._run_correction_batch(
                    correction,
                    index,
                    rule,
                    watermark,
                    late_count=late_count,
                    threshold=threshold,
                    violations=violations,
                    anomalies=anomalies,
                    disabled=disabled,
                )
            except Exception as exc:  # pragma: no cover - 防御性中断分支
                batches[index]["status"] = "中断"
                correction["status"] = "进行中"
                correction["last_error"] = str(exc)
                interrupted = batches[index]["名称"]
                break
            processed.append(batches[index]["名称"])
            new_dead_letters.extend(outcome.get("new_dead_letters", []))
            suppressed.extend(outcome.get("suppressed_dead_letters", []))

        # 同一窗口此前已判定“通过”，水位后续才越界：补写一次死信（按规则+窗口幂等）。
        # 已完成批次本身不重跑，这里只是补漏，不会重复生成。
        if not disabled and violations and interrupted is None:
            supplement = []
            try:
                for item in violations:
                    code = self._create_inspection_dead_letter(rule, watermark, correction, item)
                    if code:
                        supplement.append(code)
            except Exception as exc:  # pragma: no cover - 防御性中断分支
                for batch in batches:
                    if batch.get("名称") == "阻断新增死信":
                        batch["status"] = "中断"
                correction["last_error"] = str(exc)
                interrupted = "阻断新增死信"
            if supplement:
                correction["new_dead_letters"] = sorted(
                    set(correction.get("new_dead_letters", []) + supplement))
                new_dead_letters.extend(supplement)

        remaining = [b["名称"] for b in batches if b.get("status") != "已完成"]
        correction["status"] = "进行中" if remaining else "已完成"
        correction["updated_at"] = datetime.now().isoformat()
        correction["last_run"] = {
            "at": correction["updated_at"],
            "resumed_batches": resumed,
            "processed_batches": processed,
            "skipped_completed": completed,
            "remaining_batches": remaining,
            "interrupted_batch": interrupted,
        }
        correction["续做批次"] = resumed
        return {
            "resumed_batches": resumed,
            "processed_batches": processed,
            "new_dead_letters": sorted(set(new_dead_letters)),
            "suppressed_dead_letters": suppressed,
            "interrupted_batch": interrupted,
        }

    def _run_correction_batch(
        self,
        correction: dict[str, Any],
        index: int,
        rule: dict[str, Any],
        watermark: dict[str, Any] | None,
        *,
        late_count: float | None = None,
        threshold: float | None = None,
        violations: list[dict[str, Any]] | None = None,
        anomalies: list[str] | None = None,
        disabled: bool | None = None,
    ) -> dict[str, Any]:
        """执行单个校正批次，结果立即写回校正记录。"""
        batch = correction["batches"][index]
        if batch.get("status") == "已完成":
            return {}
        if disabled is None:
            disabled = rule.get("status") == "已停用"
        violations = violations or []
        outcome: dict[str, Any] = {}

        if index == 0:
            # 冻结规则快照：保留停用规则本身；停用生效点只写一次。
            correction["rule_snapshot"] = self._rule_snapshot(rule)
            correction["rule_status"] = rule.get("status")
            correction["rule_code"] = rule.get("规则编号")
            if disabled:
                # 停用快照一旦落库就不再覆盖，规则重新生效后仍可追溯停用版本。
                correction.setdefault("disabled_rule_snapshot", self._rule_snapshot(rule))
                point = self._disable_point(rule, correction)
                if not correction.get("停用生效点"):
                    correction["停用生效点"] = point
                correction["disable_effective_at"] = correction["停用生效点"]
                correction["unique_disable_point"] = True
                correction["conflict_rule"] = "停用生效点唯一，窗口迟到水位仅保留说明"
        elif index == 1:
            # 读取检测窗口：记录本次巡检的窗口口径与阈值输入。
            correction["window_checked_at"] = datetime.now().isoformat()
            correction["late_count"] = late_count
            correction["threshold"] = threshold
            correction["anomalies"] = list(anomalies or [])
            correction["violations"] = list(violations)
        elif index == 2:
            # 关联已阻断死信：停用后保留历史死信并建立校正关系。
            if disabled:
                preserved = list(correction.get("preserved_dead_letters", []))
                for dl in store.rows("dead_letter"):
                    if dl.get("原始主题") != correction.get("topic"):
                        continue
                    if not dl.get("关联规则编号"):
                        dl["关联规则编号"] = correction.get("rule_code")
                    if not dl.get("关联校正批次"):
                        dl["关联校正批次"] = correction.get("校正批次")
                    dl["关联检测窗口"] = dl.get("关联检测窗口") or correction.get("window")
                    dl["校正状态"] = "已保留"
                    if dl.get("记录编号") and dl["记录编号"] not in preserved:
                        preserved.append(dl["记录编号"])
                correction["preserved_dead_letters"] = sorted(preserved)
                correction["preserved_dead_letter_ids"] = [
                    int(dl.get("id", 0))
                    for dl in store.rows("dead_letter")
                    if dl.get("原始主题") == correction.get("topic")
                    and dl.get("关联校正批次") == correction.get("校正批次")
                ]
        elif index == 3:
            # 阻断新增死信：停用规则不再写死信；生效规则按窗口幂等补写。
            if disabled:
                # 停用前已生成的死信转入历史留存，校正在结果里保留而不是清空。
                history = list(correction.get("historical_new_dead_letters", []))
                for code in correction.get("new_dead_letters", []):
                    if code not in history:
                        history.append(code)
                correction["historical_new_dead_letters"] = sorted(history)
                correction["new_dead_letters"] = []
                correction["suppressed_dead_letters"] = [
                    {**item, "detail": f"{item.get('detail')}；规则已停用，未生成新死信"}
                    for item in violations
                ]
                outcome["suppressed_dead_letters"] = correction["suppressed_dead_letters"]
            else:
                created = []
                for item in violations:
                    code = self._create_inspection_dead_letter(rule, watermark, correction, item)
                    if code:
                        created.append(code)
                correction["new_dead_letters"] = sorted(set(created))
                correction["suppressed_dead_letters"] = []
                outcome["new_dead_letters"] = correction["new_dead_letters"]
        elif index == 4:
            # 同步水位追踪：水位模块读取该结果后不再让停用主题触发新阻断。
            correction["blocking_suppressed"] = bool(disabled)
            correction["watermark_synced"] = True
            correction["synced_at"] = datetime.now().isoformat()

        batch["status"] = "已完成"
        batch["processed_at"] = datetime.now().isoformat()
        correction["updated_at"] = batch["processed_at"]
        if correction["rule_status"] == "已停用":
            correction["status"] = "已完成" if all(b.get("status") == "已完成" for b in correction["batches"]) else "进行中"
        return outcome

    def _create_inspection_dead_letter(
        self,
        rule: dict[str, Any],
        watermark: dict[str, Any] | None,
        correction: dict[str, Any],
        violation: dict[str, Any],
    ) -> str | None:
        """按“规则 + 窗口”幂等补写死信，同一窗口重复巡检不会新增。"""
        if rule.get("status") == "已停用":
            return None
        rule_code = rule.get("规则编号")
        window = correction.get("window")
        topic_code = rule.get("监控主题")
        for row in store.rows("dead_letter"):
            if (
                row.get("原始主题") == topic_code
                and row.get("关联规则编号") == rule_code
                and row.get("关联检测窗口") == window
            ):
                return None

        dl_rows = store.rows("dead_letter")
        dl_id = max((int(r.get("id", 0)) for r in dl_rows), default=0) + 1
        dl = {
            "id": dl_id,
            "status": "待处置",
            "pending": True,
            "abnormal": True,
            "记录编号": f"DEAD-{dl_id:04d}",
            "原始主题": topic_code,
            "原始分区": (watermark or {}).get("分区编号", "0"),
            "失败原因": violation.get("type", "延迟超限"),
            "原始消息体": f"延迟记录数:{(watermark or {}).get('延迟记录数')}",
            "重试次数": "0",
            "入库时间": datetime.now().isoformat(),
            "处置状态": "待分析",
            "关联规则编号": rule_code,
            "关联检测窗口": window,
            "关联校正批次": correction.get("校正批次"),
            "阻断来源": "质量巡检",
        }
        dl_rows.append(dl)
        return dl["记录编号"]

    # ───────────────────────────────
    # 数据消费链
    # ───────────────────────────────

    def consumption_chain_status(self, upstream_topic: str) -> dict[str, Any]:
        """查询数据消费链：血缘→物化视图→下沉分发。"""
        lineage = self._find_by_field("stream_lineage", "上游主题", upstream_topic)
        downstream_table = lineage.get("下游表名") if lineage else None
        mv = self._find_by_field("materialized_view", "基表名", downstream_table) if downstream_table else None
        sink = self._find_by_field("sink_dispatch", "湖表名", downstream_table) if downstream_table else None

        return {
            "lineage": {"exists": lineage is not None, "status": lineage.get("status") if lineage else None, "entry": lineage},
            "materialized_view": {"exists": mv is not None, "status": mv.get("status") if mv else None, "entry": mv},
            "sink_dispatch": {"exists": sink is not None, "status": sink.get("status") if sink else None, "entry": sink},
            "downstream_table": downstream_table,
            "chain_ready": all([
                lineage is not None and lineage.get("status") == "已生效",
                mv is not None and mv.get("status") == "已启用",
                sink is not None and sink.get("status") == "已生效",
            ]),
        }

    def refresh_materialized_from_lineage(self, lineage_id: int) -> dict[str, Any]:
        """根据血缘记录刷新物化视图：血缘驱动物化查询。"""
        lineage = store.find("stream_lineage", lineage_id)
        if not lineage:
            return {"ok": False, "error": f"血缘记录 {lineage_id} 不存在"}

        upstream = lineage.get("上游主题")
        downstream = lineage.get("下游表名")
        field_map = lineage.get("字段映射", "")

        # 查找关联的物化视图
        mv = self._find_by_field("materialized_view", "基表名", downstream)
        if not mv:
            return {"ok": False, "error": f"下游表 {downstream} 未配置物化视图"}

        # 模拟生成刷新产物
        refreshed_data = {
            "lineage_id": lineage_id,
            "upstream_topic": upstream,
            "downstream_table": downstream,
            "field_mapping": field_map,
            "mv_id": mv.get("id"),
            "refreshed_at": datetime.now().isoformat(),
            "record_count": 42,  # 模拟值
        }
        self._ensure_artifact_table("materialized_refresh_log")
        store.rows("materialized_refresh_log").append(refreshed_data)

        # 同时触发下沉分发检查
        sink = self._find_by_field("sink_dispatch", "湖表名", downstream)
        if sink:
            dispatch_record = {
                "mv_refresh_id": refreshed_data["refreshed_at"],
                "sink_rule_id": sink.get("id"),
                "target_system": sink.get("目标系统"),
                "dispatched_at": datetime.now().isoformat(),
                "status": "待推送",
            }
            self._ensure_artifact_table("sink_dispatch_queue")
            store.rows("sink_dispatch_queue").append(dispatch_record)
            refreshed_data["dispatch_queued"] = True
        else:
            refreshed_data["dispatch_queued"] = False

        return {"ok": True, "refreshed": refreshed_data}

    # ───────────────────────────────
    # 维护链
    # ───────────────────────────────

    def maintenance_chain_status(self, target_table: str) -> dict[str, Any]:
        """查询维护链：补数→合并清理→生命周期。"""
        backfill = self._find_by_field("backfill_stream", "目标表名", target_table)
        compact = self._find_by_field("compact_policy", "目标表名", target_table)
        ttl = self._find_by_field("ttl_policy", "目标表名", target_table)

        return {
            "backfill": {"exists": backfill is not None, "status": backfill.get("status") if backfill else None, "entry": backfill},
            "compact_policy": {"exists": compact is not None, "status": compact.get("status") if compact else None, "entry": compact},
            "ttl_policy": {"exists": ttl is not None, "status": ttl.get("status") if ttl else None, "entry": ttl},
            "chain_ready": all([
                backfill is not None and backfill.get("status") == "已完成",
                compact is not None and compact.get("status") == "已生效",
                ttl is not None and ttl.get("status") == "已生效",
            ]),
        }

    def run_backfill_then_compact(self, backfill_id: int) -> dict[str, Any]:
        """执行补数后触发合并清理：维护链联动。"""
        backfill = store.find("backfill_stream", backfill_id)
        if not backfill:
            return {"ok": False, "error": f"补数任务 {backfill_id} 不存在"}

        target_table = backfill.get("目标表名")
        compact = self._find_by_field("compact_policy", "目标表名", target_table)
        if not compact:
            return {"ok": False, "error": f"表 {target_table} 未配置合并策略"}

        # 模拟补数过程
        start_offset = backfill.get("起始偏移", "0")
        end_offset = backfill.get("截止偏移", "0")
        try:
            start_i = int(start_offset)
            end_i = int(end_offset)
        except (ValueError, TypeError):
            start_i, end_i = 0, 100

        backfilled_count = max(0, end_i - start_i)

        # 更新补数任务状态
        backfill["status"] = "已完成"
        backfill["pending"] = False

        # 触发合并产物
        compact_result = {
            "backfill_id": backfill_id,
            "target_table": target_table,
            "backfilled_count": backfilled_count,
            "compact_policy_id": compact.get("id"),
            "merged_files": backfilled_count // 10 + 1,
            "compacted_at": datetime.now().isoformat(),
        }
        self._ensure_artifact_table("compact_results")
        store.rows("compact_results").append(compact_result)

        # 检查生命周期策略
        ttl = self._find_by_field("ttl_policy", "目标表名", target_table)
        if ttl:
            ttl_record = {
                "table": target_table,
                "hot_retention": ttl.get("热数据保留"),
                "warm_window": ttl.get("温数据窗口"),
                "cold_archive": ttl.get("冷归档策略"),
                "applied_at": datetime.now().isoformat(),
            }
            self._ensure_artifact_table("ttl_applied_log")
            store.rows("ttl_applied_log").append(ttl_record)
            compact_result["ttl_applied"] = ttl_record
        else:
            compact_result["ttl_applied"] = None

        return {"ok": True, "result": compact_result}

    # ───────────────────────────────
    # 实时指标链
    # ───────────────────────────────

    def compute_realtime_metrics(self, metric_id: int) -> dict[str, Any]:
        """计算实时指标：消费主题注册数据与Flink计算结果。"""
        metric = store.find("realtime_metrics", metric_id)
        if not metric:
            return {"ok": False, "error": f"指标定义 {metric_id} 不存在"}

        output_table = metric.get("输出表名")
        agg_window = metric.get("聚合窗口", "1h")
        agg_func = metric.get("聚合函数", "count")

        # 查找关联的主题和Flink作业
        related_topics = [r for r in store.rows("topic_registry") if r.get("status") == "已上线"]
        related_flink = [r for r in store.rows("flink_job") if r.get("status") == "运行中"]

        # 模拟指标计算
        topic_count = len(related_topics)
        flink_count = len(related_flink)

        if agg_func == "count":
            computed_value = topic_count + flink_count
        elif agg_func == "avg":
            computed_value = (topic_count + flink_count) / 2.0 if (topic_count + flink_count) > 0 else 0
        elif agg_func == "sum":
            computed_value = topic_count * 100 + flink_count * 50
        else:
            computed_value = topic_count

        result = {
            "metric_id": metric_id,
            "metric_name": metric.get("指标名称"),
            "output_table": output_table,
            "agg_window": agg_window,
            "agg_func": agg_func,
            "computed_value": computed_value,
            "source_topics": topic_count,
            "source_flink_jobs": flink_count,
            "computed_at": datetime.now().isoformat(),
        }

        self._ensure_artifact_table("realtime_metric_results")
        store.rows("realtime_metric_results").append(result)

        return {"ok": True, "result": result}

    # ───────────────────────────────
    # 链路血缘与依赖图
    # ───────────────────────────────

    def build_full_lineage_graph(self) -> dict[str, Any]:
        """构建全平台数据血缘图：包含所有模块间的数据依赖。"""
        nodes: list[dict] = []
        edges: list[dict] = []

        # 主题注册节点
        for row in store.rows("topic_registry"):
            if row.get("status") == "已上线":
                nodes.append({
                    "id": f"topic-{row['id']}",
                    "name": row.get("主题名称"),
                    "type": "topic",
                    "module": "topic_registry",
                    "status": row.get("status"),
                })

        # Kafka节点
        for row in store.rows("kafka_connector"):
            if row.get("status") == "已生效":
                nodes.append({
                    "id": f"kafka-{row['id']}",
                    "name": row.get("配置编号"),
                    "type": "connector",
                    "module": "kafka_connector",
                    "status": row.get("status"),
                })

        # Flink节点
        for row in store.rows("flink_job"):
            if row.get("status") == "运行中":
                nodes.append({
                    "id": f"flink-{row['id']}",
                    "name": row.get("任务编号"),
                    "type": "job",
                    "module": "flink_job",
                    "status": row.get("status"),
                })

        # CDC节点
        for row in store.rows("change_capture"):
            if row.get("status") == "运行中":
                nodes.append({
                    "id": f"cdc-{row['id']}",
                    "name": row.get("配置编号"),
                    "type": "cdc",
                    "module": "change_capture",
                    "status": row.get("status"),
                })

        # Iceberg节点
        for row in store.rows("iceberg_table"):
            if row.get("status") in ("已就绪", "运行中"):
                nodes.append({
                    "id": f"iceberg-{row['id']}",
                    "name": row.get("表名称"),
                    "type": "table",
                    "module": "iceberg_table",
                    "status": row.get("status"),
                })

        # 物化视图节点
        for row in store.rows("materialized_view"):
            if row.get("status") == "已启用":
                nodes.append({
                    "id": f"mv-{row['id']}",
                    "name": row.get("视图编号"),
                    "type": "view",
                    "module": "materialized_view",
                    "status": row.get("status"),
                })

        # 构建边：基于流式血缘记录
        for row in store.rows("stream_lineage"):
            if row.get("status") == "已生效":
                upstream_topic = row.get("上游主题")
                downstream_table = row.get("下游表名")
                # 查找对应节点id
                upstream_node = next((n for n in nodes if n["name"] == upstream_topic), None)
                downstream_node = next((n for n in nodes if n["name"] == downstream_table), None)
                if upstream_node and downstream_node:
                    edges.append({
                        "from": upstream_node["id"],
                        "to": downstream_node["id"],
                        "label": row.get("转换节点", "transform"),
                        "field_map": row.get("字段映射"),
                    })

        # 补充边：topic -> kafka -> flink
        topic_nodes = {n["name"]: n["id"] for n in nodes if n["type"] == "topic"}
        for row in store.rows("kafka_connector"):
            if row.get("status") == "已生效":
                kafka_id = f"kafka-{row['id']}"
                # 假设kafka消费的主题
                for topic_name, topic_id in topic_nodes.items():
                    edges.append({"from": topic_id, "to": kafka_id, "label": "consume"})

        flink_nodes = {n["name"]: n["id"] for n in nodes if n["type"] == "job"}
        for row in store.rows("flink_job"):
            if row.get("status") == "运行中":
                flink_id = f"flink-{row['id']}"
                consumed_topic = row.get("消费主题")
                if consumed_topic in topic_nodes:
                    edges.append({"from": topic_nodes[consumed_topic], "to": flink_id, "label": "compute"})

        return {"nodes": nodes, "edges": edges}

    # ───────────────────────────────
    # 链路总览
    # ───────────────────────────────

    def pipeline_overview(self) -> dict[str, Any]:
        """返回所有链路的运营总览。"""
        chains = {
            self.INGEST_CHAIN: self._count_chain_ready(self.INGEST_CHAIN),
            self.CDC_CHAIN: self._count_cdc_ready(),
            self.QUALITY_CHAIN: self._count_quality_ready(),
            self.CONSUME_CHAIN: self._count_consume_ready(),
            self.MAINTAIN_CHAIN: self._count_maintain_ready(),
            self.METRICS_CHAIN: self._count_metrics_ready(),
        }

        artifacts = {
            "upsert_merge_artifacts": len(store.rows("upsert_merge_artifacts")),
            "iceberg_partition_specs": len(store.rows("iceberg_partition_specs")),
            "quality_inspection_results": len(store.rows("quality_inspection_results")),
            "quality_inspection_corrections": len(store.rows("quality_inspection_corrections")),
            "materialized_refresh_log": len(store.rows("materialized_refresh_log")),
            "sink_dispatch_queue": len(store.rows("sink_dispatch_queue")),
            "compact_results": len(store.rows("compact_results")),
            "ttl_applied_log": len(store.rows("ttl_applied_log")),
            "realtime_metric_results": len(store.rows("realtime_metric_results")),
        }

        return {
            "chains": chains,
            "artifact_counts": artifacts,
            "total_pipelines": sum(len(v) for v in self._pipelines.values()),
        }

    # ───────────────────────────────
    # 内部辅助
    # ───────────────────────────────

    def _find_by_field(self, module: str, field: str, value: str | None) -> dict[str, Any] | None:
        if value is None:
            return None
        for row in store.rows(module):
            if str(row.get(field, "")) == str(value):
                return row
        return None

    def _ensure_artifact_table(self, name: str) -> None:
        store.rows(name)  # store.rows会setdefault

    def _count_chain_ready(self, chain_type: str) -> dict[str, int]:
        pipes = self._pipelines.get(chain_type, [])
        ready = sum(1 for p in pipes if p.get("overall_status") == "运行中")
        return {"total": len(pipes), "ready": ready}

    def _count_cdc_ready(self) -> dict[str, int]:
        cdc_rows = [r for r in store.rows("change_capture") if r.get("status") == "运行中"]
        ready = 0
        for cdc in cdc_rows:
            table = cdc.get("源表名")
            upsert = self._find_by_field("upsert_merge", "目标表名", table)
            partition = self._find_by_field("lake_partition", "目标表名", table)
            iceberg = self._find_by_field("iceberg_table", "表名称", table)
            if upsert and partition and iceberg:
                ready += 1
        return {"total": len(cdc_rows), "ready": ready}

    def _count_quality_ready(self) -> dict[str, int]:
        wm_rows = store.rows("watermark_track")
        ready = 0
        for wm in wm_rows:
            topic = wm.get("数据主题")
            quality = self._find_by_field("stream_quality", "监控主题", topic)
            if quality:
                ready += 1
        return {"total": len(wm_rows), "ready": ready}

    def _count_consume_ready(self) -> dict[str, int]:
        lineage_rows = [r for r in store.rows("stream_lineage") if r.get("status") == "已生效"]
        ready = 0
        for lin in lineage_rows:
            table = lin.get("下游表名")
            mv = self._find_by_field("materialized_view", "基表名", table)
            sink = self._find_by_field("sink_dispatch", "湖表名", table)
            if mv and sink:
                ready += 1
        return {"total": len(lineage_rows), "ready": ready}

    def _count_maintain_ready(self) -> dict[str, int]:
        bf_rows = store.rows("backfill_stream")
        ready = 0
        for bf in bf_rows:
            table = bf.get("目标表名")
            compact = self._find_by_field("compact_policy", "目标表名", table)
            ttl = self._find_by_field("ttl_policy", "目标表名", table)
            if compact and ttl:
                ready += 1
        return {"total": len(bf_rows), "ready": ready}

    def _count_metrics_ready(self) -> dict[str, int]:
        metrics = store.rows("realtime_metrics")
        ready = sum(1 for m in metrics if m.get("status") == "已生效")
        return {"total": len(metrics), "ready": ready}


pipeline_engine = PipelineEngine()
