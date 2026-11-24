"""专用固定任务路由；服务端决定研究workspace、配置与绝对期限。"""

from fastapi import APIRouter, Request
from pydantic import UUID4

from stock_research.premarket_service import admit_job, cancel_job, job_receipt, retry_job
from stock_research.schemas.api import ApiResponse, Code
from stock_research.schemas.premarket import PremarketInput, PremarketReceipt

router = APIRouter()


@router.post("/v1/premarket-jobs", status_code=202, response_model=ApiResponse[PremarketReceipt])
async def admit(request: Request, body: PremarketInput) -> ApiResponse[PremarketReceipt]:
    return ApiResponse(
        code=Code.OK,
        message="ok",
        data=await admit_job(request.app.state.manager, body),
        correlation_id=request.state.correlation_id,
    )


@router.get("/v1/premarket-jobs/{job_id}", response_model=ApiResponse[PremarketReceipt])
async def read(request: Request, job_id: UUID4) -> ApiResponse[PremarketReceipt]:
    return ApiResponse(
        code=Code.OK,
        message="ok",
        data=await job_receipt(request.app.state.manager, str(job_id)),
        correlation_id=request.state.correlation_id,
    )


@router.post("/v1/premarket-jobs/{job_id}/retry", status_code=202, response_model=ApiResponse[PremarketReceipt])
async def retry(request: Request, job_id: UUID4) -> ApiResponse[PremarketReceipt]:
    return ApiResponse(
        code=Code.OK,
        message="ok",
        data=await retry_job(request.app.state.manager, str(job_id)),
        correlation_id=request.state.correlation_id,
    )


@router.post("/v1/premarket-jobs/{job_id}/cancel", response_model=ApiResponse[PremarketReceipt])
async def cancel(request: Request, job_id: UUID4) -> ApiResponse[PremarketReceipt]:
    return ApiResponse(
        code=Code.OK,
        message="ok",
        data=await cancel_job(request.app.state.manager, str(job_id)),
        correlation_id=request.state.correlation_id,
    )
