"""持久运行的共同期限，不因拥有者重启或阶段恢复重新起算。"""

from datetime import datetime, timedelta

from stock_research.errors import ResearchIntegrityError
from stock_research.schemas.premarket import PremarketBinding


def run_deadline(run: dict[str, object], timeout_seconds: int) -> datetime:
    """历史run保持创建时预算；盘前run同时受原事件的绝对截止约束。"""
    deadline = run["create_time"] + timedelta(seconds=timeout_seconds)
    binding = run["payload"].get("premarket")
    if binding is not None:
        try:
            original = PremarketBinding.model_validate(binding)
        except ValueError:
            raise ResearchIntegrityError("盘前运行的冻结期限无效") from None
        deadline = min(deadline, original.execution_deadline)
    return deadline
