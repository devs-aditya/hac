import random
import threading
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi import HTTPException
from pydantic import BaseModel
from pydantic import Field

from services.common.app.logging_utils import utc_timestamp_ms
from services.common.app.plain_logging import get_plain_logger
from services.common.app.streaming import get_stream_publisher

SERVICE_NAME = "synthetic-generator-service"
TARGET_EVENTS_PER_SECOND = 80
SERVICES = ["api-service", "cache-service", "database-service"]
EVENT_TYPES = ["REQUEST_RECEIVED", "CACHE_HIT", "CACHE_MISS", "DB_QUERY", "RESPONSE_SENT"]
FAILURE_TYPES = {"latency", "error", "down"}


class InjectRequest(BaseModel):
    service: str
    type: str
    duration: int = Field(default=10, ge=1, le=3600)


class SyntheticEventGenerator:
    def __init__(self) -> None:
        self.logger = get_plain_logger(SERVICE_NAME)
        self.publisher = get_stream_publisher(self.logger)
        self._running = True
        self._last_event_at: str | None = None
        self._events_sent = 0
        self._lock = threading.Lock()
        self._failure_state: dict[str, dict[str, Any]] = {}
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        interval = 1.0 / TARGET_EVENTS_PER_SECOND
        while self._running:
            self._publish_cycle()
            time.sleep(interval)

    def _publish_cycle(self) -> None:
        event = self._build_event()
        metric = self._build_metric(event)
        self.publisher.publish_log(event)
        self.publisher.publish_metric(metric)
        self._events_sent += 2
        self._last_event_at = event["timestamp"]

    def _build_event(self) -> dict[str, Any]:
        scenario = random.random()
        service = self._pick_service()
        trace_id = uuid.uuid4().hex
        latency = random.uniform(20, 120)
        event_type = random.choice(EVENT_TYPES)
        status = "OK"
        error_rate = 0.0

        if scenario > 0.92:
            latency = random.uniform(650, 1200)
            status = "DEGRADED"
        elif scenario > 0.84:
            latency = random.uniform(500, 950)
            event_type = "ERROR"
            status = "FAILED"
            error_rate = random.uniform(0.5, 0.9)

        active_failure = self._get_active_failure(service)
        if active_failure:
            failure_type = active_failure["type"]
            if failure_type == "latency":
                latency = random.uniform(900, 1800)
                status = "DEGRADED"
                error_rate = max(error_rate, random.uniform(0.2, 0.5))
            elif failure_type == "error":
                event_type = "ERROR"
                status = "FAILED"
                latency = random.uniform(400, 1000)
                error_rate = max(error_rate, random.uniform(0.7, 1.0))
            elif failure_type == "down":
                event_type = "ERROR"
                status = "FAILED"
                latency = random.uniform(1500, 2500)
                error_rate = 1.0

        return {
            "timestamp": utc_timestamp_ms(),
            "service": service,
            "trace_id": trace_id,
            "event_type": event_type,
            "message": f"synthetic {event_type.lower()}",
            "latency": round(latency, 2),
            "status": status,
            "error_rate": round(error_rate, 2),
            "source": "synthetic-generator",
        }

    def _pick_service(self) -> str:
        active_failures = self._active_failures()
        if active_failures and random.random() < 0.75:
            return random.choice(active_failures)
        return random.choice(SERVICES)

    def _active_failures(self) -> list[str]:
        now = time.time()
        with self._lock:
            expired = [service for service, state in self._failure_state.items() if state["end_time"] <= now]
            for service in expired:
                self._failure_state.pop(service, None)
            return list(self._failure_state.keys())

    def _get_active_failure(self, service: str) -> dict[str, Any] | None:
        now = time.time()
        with self._lock:
            state = self._failure_state.get(service)
            if not state:
                return None
            if state["end_time"] <= now:
                self._failure_state.pop(service, None)
                return None
            return dict(state)

    def inject_failure(self, service: str, failure_type: str, duration: int) -> dict[str, Any]:
        if service not in SERVICES:
            raise ValueError(f"Unsupported service '{service}'. Supported services: {', '.join(SERVICES)}")
        if failure_type not in FAILURE_TYPES:
            raise ValueError(f"Unsupported failure type '{failure_type}'. Supported types: latency, error, down")
        end_time = time.time() + duration
        state = {
            "type": failure_type,
            "end_time": end_time,
        }
        with self._lock:
            self._failure_state[service] = state
        return {
            "status": "injected",
            "service": service,
            "type": failure_type,
            "duration": duration,
            "end_time_epoch_ms": int(end_time * 1000),
        }

    def _build_metric(self, event: dict[str, Any]) -> dict[str, Any]:
        return {
            "timestamp": event["timestamp"],
            "service": event["service"],
            "trace_id": event["trace_id"],
            "event_type": "METRIC_POINT",
            "metric_name": "latency_ms" if event["event_type"] != "ERROR" else "error_rate",
            "metric_value": event["latency"] if event["event_type"] != "ERROR" else max(event["error_rate"], 0.6),
            "latency": event["latency"],
            "error_rate": event["error_rate"],
            "status": event["status"],
            "source": "synthetic-generator",
        }

    def health(self) -> dict[str, Any]:
        return {
            "service": SERVICE_NAME,
            "active": self._thread.is_alive(),
            "events_sent": self._events_sent,
            "last_event_at": self._last_event_at,
            "target_events_per_second": TARGET_EVENTS_PER_SECOND,
            "active_failures": self._active_failures(),
        }


generator = SyntheticEventGenerator()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield


app = FastAPI(title=SERVICE_NAME, lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, Any]:
    return generator.health()


@app.post("/inject")
async def inject_failure(request: InjectRequest) -> dict[str, Any]:
    try:
        return generator.inject_failure(
            service=request.service,
            failure_type=request.type,
            duration=request.duration,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
