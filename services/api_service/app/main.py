import httpx
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request

from services.common.app.config import get_env
from services.common.app.contracts import DataResponse
from services.common.app.contracts import ValuePayload
from services.common.app.faults import FaultState
from services.common.app.logging_utils import configure_logger
from services.common.app.logging_utils import EventType
from services.common.app.logging_utils import log_event
from services.common.app.metrics import MetricsRegistry
from services.common.app.runtime import TRACE_ID_HEADER
from services.common.app.runtime import attach_common_routes
from services.common.app.runtime import lifespan

SERVICE_NAME = get_env("SERVICE_NAME", "api-service")
CACHE_SERVICE_URL = get_env("CACHE_SERVICE_URL", "http://localhost:8001")
DATABASE_SERVICE_URL = get_env("DATABASE_SERVICE_URL", "http://localhost:8002")

app = FastAPI(title=SERVICE_NAME, lifespan=lifespan)
faults = FaultState()
metrics = MetricsRegistry(SERVICE_NAME)
logger = configure_logger(SERVICE_NAME)
attach_common_routes(app, SERVICE_NAME, faults, metrics)


@app.get("/items/{key}", response_model=DataResponse)
async def get_item(key: str, request: Request) -> DataResponse:
    trace_id = request.state.trace_id
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            response = await client.get(f"{CACHE_SERVICE_URL}/cache/{key}", headers={TRACE_ID_HEADER: trace_id})
    except httpx.RequestError as exc:
        log_event(
            logger,
            service=SERVICE_NAME,
            trace_id=trace_id,
            event_type=EventType.ERROR,
            message="cache request failed",
            path=f"/items/{key}",
            method="GET",
            error=str(exc),
            level=40,
        )
        raise HTTPException(status_code=502, detail="cache service unavailable") from exc

    if response.status_code == 404:
        raise HTTPException(status_code=404, detail=f"{key} not found")
    if response.status_code >= 500:
        raise HTTPException(status_code=502, detail="cache service unavailable")

    payload = response.json()
    return DataResponse(
        key=payload["key"],
        value=payload["value"],
        source=payload["source"],
        served_by=SERVICE_NAME,
        cache_hit=payload.get("cache_hit"),
    )


@app.put("/seed/{key}", response_model=DataResponse)
async def seed_item(key: str, payload: ValuePayload, request: Request) -> DataResponse:
    trace_id = request.state.trace_id
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            db_response = await client.put(
                f"{DATABASE_SERVICE_URL}/db/{key}",
                json=payload.model_dump(),
                headers={TRACE_ID_HEADER: trace_id},
            )
            if db_response.status_code >= 500:
                raise HTTPException(status_code=502, detail="database service unavailable")

            cache_response = await client.put(
                f"{CACHE_SERVICE_URL}/cache/{key}",
                json={"value": payload.value, "source": "api"},
                headers={TRACE_ID_HEADER: trace_id},
            )
            if cache_response.status_code >= 500:
                raise HTTPException(status_code=502, detail="cache service unavailable")
    except httpx.RequestError as exc:
        log_event(
            logger,
            service=SERVICE_NAME,
            trace_id=trace_id,
            event_type=EventType.ERROR,
            message="downstream seed request failed",
            path=f"/seed/{key}",
            method="PUT",
            error=str(exc),
            level=40,
        )
        raise HTTPException(status_code=502, detail="downstream service unavailable") from exc

    return DataResponse(key=key, value=payload.value, source="api", served_by=SERVICE_NAME, cache_hit=True)
