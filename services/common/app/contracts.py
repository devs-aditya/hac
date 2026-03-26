from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Field


class ValuePayload(BaseModel):
    value: str


class CacheWritePayload(BaseModel):
    value: str
    source: str = "api"


class FaultLatencyPayload(BaseModel):
    extra_latency_ms: int = Field(ge=0, le=60000)


class FaultMemoryLeakPayload(BaseModel):
    enabled: bool
    chunk_size_kb: int = Field(default=256, ge=1, le=4096)


class ServiceHealth(BaseModel):
    service: str
    status: str
    faults: dict[str, Any]


class MetricsSnapshot(BaseModel):
    service: str
    trace_id: str | None = None
    request_count: int
    error_count: int
    error_rate: float
    avg_latency_ms: float
    p95_latency_ms: float
    uptime_seconds: int


class DataResponse(BaseModel):
    key: str
    value: str
    source: str
    served_by: Optional[str] = None
    cache_hit: Optional[bool] = None
