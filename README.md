# Simulated Distributed System

Three independent FastAPI services simulate a small distributed call chain:

- API Service
- Cache Service
- Database Service
- Event Processor Service
- Synthetic Generator Service
- RCA Service

Current scope:

- REST-only service simulation
- standardized JSON logging
- local per-service metrics endpoints
- Redis Streams ingestion on `logs_topic` and `metrics_topic`
- centralized normalization and service state inference
- synthetic high-rate traffic generation for pipeline verification
- real-time causal RCA over the active 10-second window
- fault injection for latency spikes, process crash, and memory leak simulation

Redis Streams are used as the streaming backbone for this milestone. The planned topic contracts are documented in [docs/service_specification.md](/c:/Users/lenovo/OneDrive/문서/VS%20code/hac/docs/service_specification.md).

## Run With Docker

```bash
docker compose up --build
```

Services:

- API: `http://localhost:8000`
- Cache: `http://localhost:8001`
- Database: `http://localhost:8002`
- Event Processor: `http://localhost:8003`
- Synthetic Generator: `http://localhost:8004`
- RCA: `http://localhost:8005`

## Run As Separate Processes

Install dependencies:

```bash
pip install -r requirements.txt
```

Start each service in a separate terminal:

```bash
uvicorn services.database_service.app.main:app --host 0.0.0.0 --port 8002
uvicorn services.cache_service.app.main:app --host 0.0.0.0 --port 8001
uvicorn services.api_service.app.main:app --host 0.0.0.0 --port 8000
uvicorn services.event_processor_service.app.main:app --host 0.0.0.0 --port 8003
uvicorn services.synthetic_generator_service.app.main:app --host 0.0.0.0 --port 8004
uvicorn services.rca_service.app.main:app --host 0.0.0.0 --port 8005
```

## Example Requests

```bash
curl http://localhost:8000/items/user:123
curl -X PUT http://localhost:8000/seed/user:123 -H "Content-Type: application/json" -d "{\"value\":\"premium\"}"
curl -X POST http://localhost:8001/faults/latency -H "Content-Type: application/json" -d "{\"extra_latency_ms\":500}"
curl http://localhost:8002/metrics
curl http://localhost:8003/normalized-events?seconds=10
curl http://localhost:8003/service-health
curl http://localhost:8003/debug/pipeline-health
curl http://localhost:8005/rca/latest
python scripts/verify_pipeline.py
```
