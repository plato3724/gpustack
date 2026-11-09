"""Inference admission using authenticated tenant and resolved model identities."""

from fastapi import Request
from starlette.responses import JSONResponse

from gpustack.api.tenant import resolve_tenant_context
from gpustack.server import quota_engine


def subject_from_request(request: Request) -> dict:
    user = getattr(request.state, "user", None)
    api_key = getattr(request.state, "api_key", None)
    ctx = getattr(request.state, "tenant_context", None)
    return {
        "user_id": getattr(user, "id", None),
        "api_key_id": getattr(api_key, "id", None),
        "organization_id": (
            getattr(api_key, "owner_principal_id", None)
            if api_key
            else getattr(ctx, "current_principal_id", None)
        ),
        "model_id": getattr(getattr(request.state, "model", None), "id", None),
    }


async def quota_guard(request: Request, model_ids=None):
    if getattr(request.state, "internal_inference", False):
        return None
    if not await quota_engine.load_rules():
        return None
    user = getattr(request.state, "user", None)
    if user is not None and not hasattr(request.state, "tenant_context"):
        await resolve_tenant_context(
            request, user, x_organization_id=request.headers.get("X-Organization-Id")
        )
    subject = subject_from_request(request)
    if model_ids is not None:
        subject["model_id"] = model_ids
    denial = await quota_engine.check_request(**subject)
    if denial is None:
        return None
    return JSONResponse(
        status_code=429,
        headers={"Retry-After": str(denial.retry_after)},
        content={
            "error": {
                "message": "Quota exceeded. Retry after the window slides.",
                "type": "rate_limit_error",
                "code": 429,
            }
        },
    )
