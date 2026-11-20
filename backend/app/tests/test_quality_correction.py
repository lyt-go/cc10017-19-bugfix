"""巡检校正数据结果测试：停用规则后阻断边界、保留死信、批次续做与跨模块同步。"""
from __future__ import annotations

import unittest

from app.pipeline_engine import pipeline_engine
from app.services.stream_quality import StreamQualityService
from app.services.watermark_track import WatermarkTrackService
from app.store import store

TOPIC = "TOPI-0001"
RULE_ID = 1
WINDOW = "5min"


def dead_letters(topic: str = TOPIC) -> list[dict]:
    return [row for row in store.rows("dead_letter") if row.get("原始主题") == topic]


class QualityCorrectionTestBase(unittest.TestCase):
    """每个用例重置内存仓库，避免用例之间互相污染死信数量。"""

    def setUp(self) -> None:
        from app import seed

        store._tables = {name: [dict(row) for row in rows] for name, rows in seed.SEED_ROWS.items()}
        self.quality_svc = StreamQualityService()
        self.watermark_svc = WatermarkTrackService()


class TestDisableStopsNewDeadLetters(QualityCorrectionTestBase):
    def test_disabled_rule_inspection_creates_no_dead_letter(self) -> None:
        """停用规则后巡检不再新增死信，只保留已生成记录。"""
        before = {row.get("记录编号") for row in dead_letters()}
        entry, _ = self.quality_svc.run_action(RULE_ID, "停用规则")
        self.assertEqual(entry["status"], "已停用")
        self.assertEqual({row.get("记录编号") for row in dead_letters()}, before)

        result = pipeline_engine.run_quality_inspection(TOPIC)
        self.assertTrue(result["ok"])
        self.assertEqual(result["result"]["new_dead_letters"], [])
        self.assertTrue(result["result"]["blocking_suppressed"])
        self.assertEqual({row.get("记录编号") for row in dead_letters()}, before)

    def test_repeated_inspection_same_window_is_idempotent(self) -> None:
        """同一窗口再次巡检不新增死信，校正结果只有一份。"""
        self.quality_svc.run_action(RULE_ID, "停用规则")
        first = pipeline_engine.run_quality_inspection(TOPIC)
        count_after_first = len(dead_letters())
        second = pipeline_engine.run_quality_inspection(TOPIC)

        self.assertEqual(len(dead_letters()), count_after_first)
        self.assertEqual(first["correction"]["校正批次"], second["correction"]["校正批次"])
        corrections = pipeline_engine.list_quality_corrections(TOPIC)
        self.assertEqual(len(corrections), 1)

    def test_watermark_lag_does_not_trigger_blocking_for_disabled_topic(self) -> None:
        """水位追踪读取校正结果：停用主题标记滞后不再产生新阻断。"""
        self.quality_svc.run_action(RULE_ID, "停用规则")
        pipeline_engine.run_quality_inspection(TOPIC)
        count_before = len(dead_letters())

        entry, _ = self.watermark_svc.run_action(1, "标记滞后")
        self.assertEqual(entry["阻断状态"], "已抑制")
        self.assertFalse(entry["abnormal"])
        self.assertEqual(entry["校正批次"], pipeline_engine.list_quality_corrections(TOPIC)[0]["校正批次"])
        self.assertEqual(len(dead_letters()), count_before)

    def test_quality_chain_check_reports_suppression(self) -> None:
        """质量链查询返回校正结果，且不再报越界告警。"""
        self.quality_svc.run_action(RULE_ID, "停用规则")
        pipeline_engine.run_quality_inspection(TOPIC)
        status = pipeline_engine.quality_chain_check(TOPIC)
        self.assertTrue(status["stream_quality"]["disabled"])
        self.assertTrue(status["blocking_suppressed"])
        self.assertFalse(status["quality_alert"])
        self.assertIsNotNone(status["correction"])


class TestCorrectionRelation(QualityCorrectionTestBase):
    def test_correction_preserves_disabled_rule_and_existing_dead_letters(self) -> None:
        """校正结果保留停用规则与已阻断死信，并建立关联。"""
        existing = {row.get("记录编号") for row in dead_letters()}
        self.quality_svc.run_action(RULE_ID, "停用规则")
        result = pipeline_engine.run_quality_inspection(TOPIC)
        correction = result["correction"]

        self.assertEqual(correction["rule_status"], "已停用")
        self.assertEqual(correction["rule_snapshot"]["规则编号"], "STRE-0001")
        self.assertIsNotNone(correction["停用生效点"])
        self.assertEqual(set(correction["preserved_dead_letters"]), set(existing))

        for row in dead_letters():
            self.assertEqual(row.get("关联规则编号"), "STRE-0001")
            self.assertEqual(row.get("关联校正批次"), correction["校正批次"])
            self.assertEqual(row.get("校正状态"), "已保留")

    def test_reenabled_rule_resumes_blocking_by_window(self) -> None:
        """规则重新生效后，未覆盖窗口按窗口幂等补写死信。"""
        self.quality_svc.run_action(RULE_ID, "停用规则")
        pipeline_engine.run_quality_inspection(TOPIC)
        count_stopped = len(dead_letters())

        rule = store.find("stream_quality", RULE_ID)
        rule["status"] = "已生效"
        rule["告警阈值"] = "5"
        result = pipeline_engine.run_quality_inspection(TOPIC, window="6min")
        self.assertFalse(result["result"]["blocking_suppressed"])
        self.assertEqual(len(dead_letters()), count_stopped + 1)
        self.assertEqual(result["result"]["new_dead_letters"][0], dead_letters()[-1]["记录编号"])
        self.assertEqual(dead_letters()[-1]["关联检测窗口"], "6min")

        # 同一窗口再次巡检，不再新增。
        pipeline_engine.run_quality_inspection(TOPIC, window="6min")
        self.assertEqual(len(dead_letters()), count_stopped + 1)


class TestCorrectionBatches(QualityCorrectionTestBase):
    def test_interrupted_correction_only_resumes_unfinished_batches(self) -> None:
        """中断后只续做未完成批次，已完成批次不重复执行。"""
        self.quality_svc.run_action(RULE_ID, "停用规则")
        correction = pipeline_engine.list_quality_corrections(TOPIC)[0]
        done = [b["名称"] for b in correction["batches"] if b["status"] == "已完成"]
        self.assertEqual(done, ["冻结规则快照", "关联已阻断死信"])

        result = pipeline_engine.run_quality_inspection(TOPIC)
        self.assertEqual(
            result["result"]["resumed_batches"],
            ["读取检测窗口", "阻断新增死信", "同步水位追踪"],
        )
        stored = store.rows(pipeline_engine.CORRECTION_TABLE)[0]
        self.assertEqual(stored["status"], "已完成")

        again = pipeline_engine.run_quality_inspection(TOPIC)
        self.assertEqual(again["result"]["resumed_batches"], [])

    def test_batch_failure_keeps_unfinished_batches_for_resume(self) -> None:
        """批次执行中断时保留未完成批次，下一次巡检从断点续做。"""
        store.find("stream_quality", RULE_ID)["告警阈值"] = "5"
        original = pipeline_engine._create_inspection_dead_letter

        def boom(*args, **kwargs):
            raise RuntimeError("批次执行中断")

        pipeline_engine._create_inspection_dead_letter = boom
        try:
            first = pipeline_engine.run_quality_inspection(TOPIC)
        finally:
            pipeline_engine._create_inspection_dead_letter = original

        interruption = first["result"]
        self.assertEqual(interruption["interrupted_batch"], "阻断新增死信")
        stored = store.rows(pipeline_engine.CORRECTION_TABLE)[0]
        self.assertEqual(stored["status"], "进行中")
        completed = [b["名称"] for b in stored["batches"] if b["status"] == "已完成"]
        self.assertEqual(completed, ["冻结规则快照", "读取检测窗口", "关联已阻断死信"])
        self.assertEqual(len(dead_letters()), 2)

        # 断点续做：已完成的批次不重跑，未完成批次继续。
        resumed = pipeline_engine.run_quality_inspection(TOPIC)
        self.assertEqual(resumed["result"]["resumed_batches"], ["阻断新增死信", "同步水位追踪"])
        self.assertEqual(len(dead_letters()), 3)
        self.assertEqual(store.rows(pipeline_engine.CORRECTION_TABLE)[0]["status"], "已完成")

    def test_conflicting_order_keeps_single_disable_point(self) -> None:
        """先冻结规则再处理窗口 / 先处理窗口再冻结，都只保留一个停用生效点。"""
        window_before = pipeline_engine.run_quality_inspection(TOPIC)
        self.assertFalse(window_before["result"]["blocking_suppressed"])

        entry, _ = self.quality_svc.run_action(RULE_ID, "停用规则")
        point = entry["停用时间"]
        after = pipeline_engine.run_quality_inspection(TOPIC)
        self.assertEqual(after["correction"]["停用生效点"], point)
        self.assertEqual(
            len({row.get("停用生效点") for row in pipeline_engine.list_quality_corrections(TOPIC)}), 1)

    def test_boundary_value_equal_to_threshold_is_not_violation(self) -> None:
        """边界口径：等于阈值不算越界；越界非法值只写异常说明。"""
        watermark = store.find("watermark_track", 1)
        watermark["延迟记录数"] = "50"
        result = pipeline_engine.run_quality_inspection(TOPIC)
        self.assertEqual(result["result"]["violations"], [])
        self.assertEqual(result["result"]["overall"], "通过")

        watermark["延迟记录数"] = "-3"
        anomalous = pipeline_engine.run_quality_inspection(TOPIC)
        self.assertEqual(anomalous["result"]["overall"], "异常")
        self.assertTrue(anomalous["result"]["anomalies"])
        self.assertNotEqual(anomalous["result"]["overall"], "通过")

    def test_correction_matches_after_refresh(self) -> None:
        """刷新后水位追踪与流式质量读到同一份校正结果。"""
        self.quality_svc.run_action(RULE_ID, "停用规则")
        pipeline_engine.run_quality_inspection(TOPIC)

        from_watermark = self.watermark_svc.get_quality_correction(TOPIC)
        from_quality = pipeline_engine.get_quality_correction(RULE_ID, window=WINDOW)
        self.assertIsNotNone(from_watermark)
        self.assertEqual(from_watermark["校正批次"], from_quality["校正批次"])
        self.assertTrue(self.watermark_svc.blocking_suppressed(TOPIC))


if __name__ == "__main__":
    unittest.main()
