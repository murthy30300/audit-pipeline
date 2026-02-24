from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request
from datetime import datetime


class AuditLogMiddleware(BaseHTTPMiddleware):

    # ⭐ REQUIRED
    def __init__(self, app):

        super().__init__(app)


    async def dispatch(

        self,
        request: Request,
        call_next

    ):

        start = datetime.utcnow()

        response = await call_next(request)

        duration = (
            datetime.utcnow() - start
        ).total_seconds()

        print(
            f"[AUDIT] {request.method} {request.url.path}"
            f" status={response.status_code}"
            f" duration={duration:.3f}s"
        )

        return response