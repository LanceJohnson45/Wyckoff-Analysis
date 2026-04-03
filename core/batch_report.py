# -*- coding: utf-8 -*-
"""
批量研报管线 — 公共 API 转发层。

将 scripts/step3_batch_report.py 中被其他模块引用的函数集中 re-export，
使消费者从 core/ 导入而非直接从 scripts/ 导入，保持分层干净。
"""
from scripts.step3_batch_report import (  # noqa: F401
    extract_operation_pool_codes,
    generate_stock_payload,
    run as run_step3,
)

__all__ = [
    "extract_operation_pool_codes",
    "generate_stock_payload",
    "run_step3",
]
