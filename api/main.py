from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import lender, agent, manager, hr
from .audit_log import AuditLogMiddleware


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(AuditLogMiddleware)

@app.get("/health")
def health():
    return {"status":"ok"}

app.include_router(lender.router,prefix="/dashboard")
app.include_router(agent.router,prefix="/dashboard")
app.include_router(manager.router,prefix="/dashboard")
app.include_router(hr.router,prefix="/dashboard")