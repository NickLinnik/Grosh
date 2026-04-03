from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    # startup
    yield
    # shutdown


app = FastAPI(title="Grosh API", version="0.1.0", lifespan=lifespan)


@app.get("/health", tags=["ops"], status_code=200)
async def health() -> dict[str, str]:
    return {"status": "ok"}
