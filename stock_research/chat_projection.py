"""按问答工具用途裁剪重复元数据，原事实引用和值保持不变。"""


def project_chat_data(tool: str, data: dict[str, object]) -> dict[str, object]:
    """完整参数走strategy_rules，历史查询不重复返回策略与报告全文。"""
    if tool == "stock_history":
        return {
            key: value
            for key, value in data.items()
            if key
            in {
                "run_id",
                "report_id",
                "snapshot_id",
                "snapshot_hash",
                "as_of",
                "company",
                "history",
                "history_total",
                "limitations",
            }
        }
    if tool == "compare_stocks":
        data["items"] = [project_chat_data("stock_factors", item) for item in data["items"]]
    if tool == "stock_factors":
        for observation in data.get("observations", []):
            for key in ("parameters", "factor_definitions", "effective_config"):
                observation.pop(key, None)
    if tool == "run_status":
        for key in ("selections", "reviews", "companies"):
            data.pop(key, None)
    strip_repeated_metadata(data)
    return data


def strip_repeated_metadata(value: object) -> None:
    """观点所引原ref不等于已读原事实，须经股票或分组工具实际读取。"""
    if isinstance(value, dict):
        value.pop("evidence_refs", None)
        if isinstance(value.get("quality"), dict):
            value["quality"].pop("eligible", None)
        for child in value.values():
            strip_repeated_metadata(child)
    elif isinstance(value, list):
        for child in value:
            strip_repeated_metadata(child)
