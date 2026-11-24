"""领域错误不依赖HTTP，API边界负责稳定错误码翻译。"""

RETRYABLE_MODEL_ERRORS = frozenset({"transient_provider_error", "provider_stream_idle_timeout", "transport_error"})


class ResearchNotFound(ValueError):
    """当前workspace不存在该业务对象。"""


class ResearchConflict(ValueError):
    """另一个执行者持锁或状态不允许动作。"""


class ResearchIntegrityError(ValueError):
    """持久化内容摘要或版本不一致，不可继续使用。"""


class ResearchInputError(ValueError):
    """参数结构正确，但交易日或动作条件无效。"""


class ResearchUnavailable(RuntimeError):
    """数据质量或来源未就绪，不能冻结新研究输入。"""


class ResearchSourceChanged(ResearchUnavailable):
    """磁盘与已加载执行版本不同，必须重启后创建新研究。"""


class ResearchQualityError(ResearchUnavailable):
    """真实快照覆盖或基准质量未通过，不能创建研究运行。"""


class EvaluationHistoryInsufficient(ValueError):
    """冻结历史不能提供所需独立截面，重算原输入不会补齐资料。"""


class EvaluationQualityFailed(ValueError):
    """冻结截面未过数据质量门槛，必须补齐资料后创建新评估。"""
