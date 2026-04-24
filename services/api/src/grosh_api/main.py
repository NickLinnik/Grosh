import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI
from grosh_shared.db_url import for_asyncpg

from grosh_api.error_handlers import register_error_handlers
from grosh_api.routers.admin import router as admin_router
from grosh_api.routers.auth import router as auth_router
from grosh_api.routers.settings import router as settings_router


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    dsn = for_asyncpg(os.environ["DATABASE_URL"])
    application.state.pool = await asyncpg.create_pool(dsn)
    yield
    await application.state.pool.close()


app = FastAPI(title="Grosh API", version="0.1.0", lifespan=lifespan)

register_error_handlers(app)

app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(settings_router)


@app.get("/health", tags=["ops"], status_code=200)
async def health() -> dict[str, str]:
    return {"status": "ok"}
