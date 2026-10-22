"""纯数据任务成功通知与失败可见性的回归，不发送真实消息。"""

from io import BytesIO
import json
from pathlib import Path

import pytest

from scripts import market_data_notification as notify


def summary(tmp_path: Path, *, status: str='ok', counts: dict | None=None) -> None:
    """写入与生产覆盖摘要相同的结构。"""
    counts=counts if counts is not None else {'gap_repaired':200,'manifest_cached':50,'excluded':10}
    (tmp_path/'prewarm_cn_2026-10-22_summary.json').write_text(json.dumps({'market':'cn','target_date':'2026-10-22','status':status,'counts':counts,'total':sum(counts.values())}),encoding='utf-8')


def test_success_report_distinguishes_new_rows_and_cached_symbols(tmp_path: Path) -> None:
    """成功通知来自覆盖产物，不冒充分析漏斗日报。"""
    summary(tmp_path)
    notice=notify.build_notice(tmp_path,'cn','success','https://github.com/example/repo/actions/runs/1')
    assert '成功' in notice.title
    for text in ['本轮补齐：200','检查点已完成：50','排除不更新：10','2026-10-22','actions/runs/1']:
        assert text in notice.content


@pytest.mark.parametrize('run_status,status,counts',[('failure','ok',{'gap_repaired':2}),('success','partial',{'gap_repaired':2,'pending':3}),('success','ok',{'failed':1}),('success','ok',{'gap_partial':1})])
def test_incomplete_task_is_never_reported_as_success(tmp_path: Path, run_status: str, status: str, counts: dict) -> None:
    """任务或数据不完整时不发成功卡片。"""
    summary(tmp_path,status=status,counts=counts)
    notice=notify.build_notice(tmp_path,'cn',run_status,'https://github.com/example/repo/actions/runs/1')
    assert '成功' not in notice.title
    assert '未完整完成' in notice.title


def test_no_summary_reports_missing_evidence(tmp_path: Path) -> None:
    """准备阶段失败也有清楚的通知内容。"""
    notice=notify.build_notice(tmp_path,'cn','failure','https://github.com/example/repo/actions/runs/1')
    assert '未取得更新摘要' in notice.title


@pytest.mark.parametrize('payload',[{'code':1},{'StatusCode':1},{}, {'code':False}])
def test_feishu_rejection_and_missing_business_code_fail(monkeypatch: pytest.MonkeyPatch,payload: dict) -> None:
    """HTTP 200不等于业务发送成功。"""
    monkeypatch.setattr(notify,'urlopen',lambda *args,**kwargs:BytesIO(json.dumps(payload).encode()))
    with pytest.raises(RuntimeError):
        notify.send_notice('https://example.test/webhook',notify.DataNotice('title','content'))


def test_transport_accepts_explicit_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """发送路径解析飞书明确成功响应。"""
    monkeypatch.setattr(notify,'urlopen',lambda *args,**kwargs:BytesIO(b'{"code":0}'))
    notify.send_notice('https://example.test/webhook',notify.DataNotice('title','content'))


def test_unconfigured_webhook_has_nonzero_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    """无配置不能打印发送完成或静默成功。"""
    monkeypatch.delenv('DATA_REPORT_WEBHOOK_URL',raising=False)
    monkeypatch.delenv('FEISHU_WEBHOOK_URL',raising=False)
    monkeypatch.setattr('sys.argv',['notification','--market','cn','--run-status','success','--run-url','https://example.test/run'])
    assert notify.main()==2


def test_delivery_failure_is_not_silently_swallowed(tmp_path: Path,monkeypatch: pytest.MonkeyPatch) -> None:
    """模拟拒绝响应经过CLI完整路径后仍返回失败。"""
    summary(tmp_path)
    monkeypatch.delenv('DATA_REPORT_WEBHOOK_URL',raising=False)
    monkeypatch.setenv('FEISHU_WEBHOOK_URL','https://example.test/webhook')
    monkeypatch.setattr('sys.argv',['notification','--market','cn','--summary-dir',str(tmp_path),'--run-status','success','--run-url','https://example.test/run'])
    monkeypatch.setattr(notify,'urlopen',lambda *args,**kwargs:BytesIO(b'{"code":1}'))
    assert notify.main()==2


def test_workflow_sends_data_result_on_success_and_failure() -> None:
    """防止CN工作流再次只有failure()通知，成功时根本不调用。"""
    workflow=Path('.github/workflows/wyckoff_funnel.yml').read_text()
    step=workflow.split('      - name: Notify daily data result',1)[1].split('      - name:',1)[0]
    assert 'always()' in step and 'failure()' not in step
    assert 'scripts.market_data_notification' in step
    assert 'secrets.FEISHU_WEBHOOK_URL' in step
