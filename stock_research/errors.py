"""领域错误不依赖HTTP，API边界负责稳定错误码翻译。"""


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
