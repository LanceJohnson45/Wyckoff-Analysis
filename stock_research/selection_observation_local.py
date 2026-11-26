"""原任务固定输入先发布，再由既有生产观测路径计算和重读。"""

from collections.abc import Callable

from stock_research.artifacts import write_once
from stock_research.config import Settings
from stock_research.identity import digest
from stock_research.schemas.selection_observation_execution import SelectionObservationFiles
from stock_research.schemas.selection_observations import SelectionObservationInput
from stock_research.selection_observation_cli import (
    MAX_OBSERVATION_FILE_BYTES,
    observation_input,
    publish_observations,
)
from stock_research.source_guard import ensure_source_current


def fixed_observation_input(request: SelectionObservationFiles) -> SelectionObservationInput:
    """文件存在即核对原绑定；丢失或损坏的已冻结输入不能从当前源重建。"""
    path = request.frozen_input_path
    if path.is_symlink():
        raise ValueError("观测输入不能是符号链接")
    if path.exists():
        if path.stat().st_size > MAX_OBSERVATION_FILE_BYTES:
            raise ValueError("观测输入超出上限")
        inputs = SelectionObservationInput.model_validate_json(path.read_text(encoding="utf-8"))
    else:
        if request.expected_input_hash is not None:
            raise ValueError("原冻结输入缺失")
        inputs = observation_input(
            Settings(_env_file=None, local_market_path=request.market_path), request.scope, request.cutoff
        )
        content = inputs.model_dump_json() + "\n"
        if len(content.encode()) > MAX_OBSERVATION_FILE_BYTES:
            raise ValueError("观测输入超出上限")
        write_once(path, content)
        inputs = SelectionObservationInput.model_validate_json(path.read_text(encoding="utf-8"))
    if (
        inputs.scope != request.scope
        or inputs.cutoff != request.cutoff
        or inputs.source_hash != request.source_hash
        or request.expected_input_hash not in {None, digest(inputs.model_dump(mode="json"))}
    ):
        raise ValueError("观测输入与原任务绑定不一致")
    return inputs


def observe_files(request: SelectionObservationFiles, checkpoint: Callable[[str], None]) -> dict[str, object]:
    """所有备份和文件写入都在拥有的worker内，冻结后续跑不再读源。"""
    ensure_source_current()
    inputs = fixed_observation_input(request)
    checkpoint(digest(inputs.model_dump(mode="json")))
    return publish_observations(request.artifacts_path, inputs)
