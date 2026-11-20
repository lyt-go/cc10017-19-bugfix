"""数据管道链路接口：暴露跨模块数据链的查询与执行能力。"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.pipeline_engine import pipeline_engine
from app.schemas import ActionResult

router = APIRouter(prefix="/api/pipeline", tags=["数据管道"])


@router.get("/ingest/{topic_code}")
def get_ingest_chain(topic_code: str) -> dict:
    """查询实时接入链状态：主题→Kafka→Schema→Flink。"""
    return pipeline_engine.ingest_chain_status(topic_code)


@router.post("/ingest/{topic_code}/advance")
def advance_ingest_stage(topic_code: str, payload: dict) -> ActionResult:
    """推进实时接入链阶段。"""
    stage = payload.get("stage")
    status = payload.get("status")
    artifact = payload.get("artifact")
    if not stage or not status:
        return ActionResult(ok=False, message="缺少 stage 或 status")
    result = pipeline_engine.advance_ingest_stage(topic_code, stage, status, artifact)
    if result is None:
        return ActionResult(ok=False, message=f"接入链 {topic_code} 不存在")
    return ActionResult(ok=True, message="阶段已推进", entry=result)


@router.get("/cdc/{source_db}/{source_table}")
def get_cdc_chain(source_db: str, source_table: str) -> dict:
    """查询CDC入湖链：CDC→UPSERT→分区→Iceberg。"""
    return pipeline_engine.cdc_chain_status(source_db, source_table)


@router.post("/cdc/{cdc_id}/upsert")
def run_cdc_upsert(cdc_id: int) -> ActionResult:
    """执行CDC增量合并模拟。"""
    result = pipeline_engine.run_cdc_upsert_simulation(cdc_id)
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("error", "未知错误"))
    return ActionResult(ok=True, message=f"CDC {cdc_id} 增量合并完成", entry=result)


@router.post("/partition/apply/{table_name}")
def apply_partition(table_name: str) -> ActionResult:
    """将分区策略应用到Iceberg表。"""
    result = pipeline_engine.apply_partition_to_iceberg(table_name)
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("error", "未知错误"))
    return ActionResult(ok=True, message="分区策略已应用", entry=result)


@router.get("/quality/{topic_code}")
def get_quality_chain(topic_code: str) -> dict:
    """查询质量容错链：水位→质量→死信，附带巡检校正数据结果。"""
    return pipeline_engine.quality_chain_check(topic_code)


@router.post("/quality/{topic_code}/inspect")
def run_quality_inspection(
    topic_code: str,
    window: str | None = None,
    rule_id: int | None = None,
) -> ActionResult:
    """执行质量巡检；停用规则不再生成新死信，只续做未完成的校正批次。"""
    result = pipeline_engine.run_quality_inspection(topic_code, window=window, rule_id=rule_id)
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("error", "未知错误"))
    correction = result.get("correction") or {}
    status = correction.get("status")
    return ActionResult(
        ok=True,
        message=f"质量巡检完成，校正结果 {correction.get('校正批次', '—')}（{status or '—'}）",
        entry=result,
    )


@router.get("/quality/{topic_code}/corrections")
def list_quality_corrections(topic_code: str) -> dict:
    """读取该主题的巡检校正数据结果；水位追踪与流式质量共用这份交接对象。"""
    items = pipeline_engine.list_quality_corrections(topic_code=topic_code)
    return {"items": items, "total": len(items), "topic": topic_code}


@router.get("/quality-rules/{rule_id}/correction")
def get_quality_correction(rule_id: int, window: str | None = None) -> dict:
    """按规则读取最近一次巡检校正结果，便于页面刷新后核对窗口。"""
    correction = pipeline_engine.get_quality_correction(rule_id, window=window)
    return {"item": correction, "exists": correction is not None}


@router.get("/consumption/{upstream_topic}")
def get_consumption_chain(upstream_topic: str) -> dict:
    """查询数据消费链：血缘→物化视图→下沉分发。"""
    return pipeline_engine.consumption_chain_status(upstream_topic)


@router.post("/consumption/lineage/{lineage_id}/refresh")
def refresh_materialized(lineage_id: int) -> ActionResult:
    """根据血缘刷新物化视图并触发下沉分发。"""
    result = pipeline_engine.refresh_materialized_from_lineage(lineage_id)
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("error", "未知错误"))
    return ActionResult(ok=True, message="物化视图已刷新", entry=result)


@router.get("/maintenance/{target_table}")
def get_maintenance_chain(target_table: str) -> dict:
    """查询维护链：补数→合并清理→生命周期。"""
    return pipeline_engine.maintenance_chain_status(target_table)


@router.post("/maintenance/backfill/{backfill_id}/compact")
def run_backfill_compact(backfill_id: int) -> ActionResult:
    """执行补数后触发合并清理与生命周期。"""
    result = pipeline_engine.run_backfill_then_compact(backfill_id)
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("error", "未知错误"))
    return ActionResult(ok=True, message="补数与合并清理完成", entry=result)


@router.post("/metrics/{metric_id}/compute")
def compute_metrics(metric_id: int) -> ActionResult:
    """计算实时指标。"""
    result = pipeline_engine.compute_realtime_metrics(metric_id)
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("error", "未知错误"))
    return ActionResult(ok=True, message="实时指标计算完成", entry=result)


@router.get("/lineage-graph")
def get_full_lineage_graph() -> dict:
    """获取全平台数据血缘图。"""
    return pipeline_engine.build_full_lineage_graph()


@router.get("/overview")
def get_pipeline_overview() -> dict:
    """数据管道链路总览。"""
    return pipeline_engine.pipeline_overview()
