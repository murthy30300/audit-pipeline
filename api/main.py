# FastAPI app entrypoint
from fastapi import FastAPI

from .routers import lender, agent, manager, hr
from .audit_log import AuditLogMiddleware


app = FastAPI()
app.add_middleware(AuditLogMiddleware)


@app.get("/health")
def health():
    return {"status": "ok"}


# Dashboards
app.include_router(lender.router, prefix="/dashboard", tags=["Lender"])
app.include_router(agent.router, prefix="/dashboard", tags=["Agent"])
app.include_router(manager.router, prefix="/dashboard", tags=["Manager"])
app.include_router(hr.router, prefix="/dashboard", tags=["HR"])