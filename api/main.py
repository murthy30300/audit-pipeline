# FastAPI app entrypoint
from fastapi import FastAPI
from .routers import lender, agent, manager, hr
from .audit_log import AuditLogMiddleware

app = FastAPI()
app.add_middleware(AuditLogMiddleware)

app.include_router(lender.router)
app.include_router(agent.router)
app.include_router(manager.router)
app.include_router(hr.router)
