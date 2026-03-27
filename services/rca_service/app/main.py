import json
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException

from services.common.app.broker import get_stream_backend
from services.common.app.config import get_env
from services.common.app.logging_utils import utc_timestamp_ms
from services.common.app.plain_logging import get_plain_logger

SERVICE_NAME = get_env("SERVICE_NAME", "rca-service")
NORMALIZED_TOPIC = get_env("NORMALIZED_TOPIC", "normalized_events_topic")
HEALTH_TOPIC = get_env("HEALTH_TOPIC", "service_health_topic")
REMEDIATION_TOPIC = get_env("REMEDIATION_TOPIC", "remediation_triggers_topic")
REMEDIATION_RESULTS_TOPIC = get_env("REMEDIATION_RESULTS_TOPIC", "remediation_results_topic")
WINDOW_SECONDS = int(get_env("WINDOW_SECONDS", "10"))
PROPAGATION_DELAY_SECONDS = float(get_env("PROPAGATION_DELAY_SECONDS", "2"))
RCA_HISTORY_SIZE = int(get_env("RCA_HISTORY_SIZE", "20"))
UNCERTAIN_MARGIN = float(get_env("RCA_UNCERTAIN_MARGIN", "0.12"))
LATENCY_HISTORY_SIZE = int(get_env("LATENCY_HISTORY_SIZE", "200"))
PIPELINE_SLA_MS = float(get_env("PIPELINE_SLA_MS", "15000"))
INCIDENT_HISTORY_LIMIT = int(get_env("INCIDENT_HISTORY_LIMIT", "100"))
INCIDENT_INDEX_KEY = "incidents:list"

DEPENDENCY_GRAPH: dict[str, list[str]] = {
    "api-service": ["cache-service"],
    "cache-service": ["database-service"],
    "database-service": [],
}

DEPENDENTS_GRAPH: dict[str, list[str]] = {
    "database-service": ["cache-service"],
    "cache-service": ["api-service"],
    "api-service": [],
}

SEVERITY_RANK = {
    "FAILED": 2,
    "DEGRADED": 1,
    "OK": 0,
}


def parse_timestamp_to_epoch(timestamp: str | None) -> float:
    if not timestamp:
        return time.time()
    normalized = timestamp.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).timestamp()


@dataclass
class Hypothesis:
    service: str
    score: float
    confidence: float
    temporal_consistency: float
    dependency_match: float
    signal_strength: float
    noise_penalty: float
    impact_match: float
    repeated_pattern_bonus: float
    affected_services: list[str]
    missing_impact: list[str]
    independent_failures: list[str]
    causal_chain: list[str]
    reasoning: list[str]
    first_timestamp: str
    first_ingestion_timestamp: str | None
    anomaly_detection_timestamp: str | None
    status: str
    valid: bool


class RcaEngine:
    def __init__(self) -> None:
        self.logger = get_plain_logger(SERVICE_NAME)
        self.backend = get_stream_backend()
        self._events: deque[dict[str, Any]] = deque()
        self._health: dict[str, dict[str, Any]] = {}
        self._history: deque[dict[str, Any]] = deque(maxlen=RCA_HISTORY_SIZE)
        self._latency_history: deque[dict[str, Any]] = deque(maxlen=LATENCY_HISTORY_SIZE)
        self._last_triggered_incident_id: str | None = None
        self._triggered_incidents: dict[str, str] = {}
        self._incident_timings: dict[str, dict[str, Any]] = {}
        self._active_incident_ids: set[str] = set()
        self._incident_records: dict[str, dict[str, Any]] = {}
        self._incident_order: deque[str] = deque()
        self._incident_remediation: dict[str, dict[str, Any]] = {}
        self._redis_client = getattr(self.backend, "_client", None)
        self._latest: dict[str, Any] = {
            "primary_root_cause": [],
            "secondary_root_causes": [],
            "independent_failures": [],
            "confidence": 0.0,
            "confidence_breakdown": {},
            "confidence_explanation": "No RCA candidate yet",
            "status": "UNCERTAIN",
            "alternative_causes": [],
            "affected_services": [],
            "missing_impact": [],
            "causal_chain": [],
            "reasoning": ["No RCA candidate yet"],
            "incident_id": None,
            "timings": {},
            "evaluated_at": utc_timestamp_ms(),
            "window_seconds": WINDOW_SECONDS,
            "propagation_delay_seconds": PROPAGATION_DELAY_SECONDS,
        }
        self._lock = threading.Lock()
        self._offsets = {
            NORMALIZED_TOPIC: "0-0",
            HEALTH_TOPIC: "0-0",
            REMEDIATION_RESULTS_TOPIC: "0-0",
        }
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()

    def _consume_loop(self) -> None:
        while True:
            try:
                response = self.backend.read(self._offsets, block_ms=250, count=128)
                for topic, entries in response:
                    for entry_id, fields in entries:
                        self._offsets[topic] = entry_id
                        raw_payload = fields.get("payload")
                        if not raw_payload:
                            continue
                        payload = json.loads(raw_payload)
                        if topic == NORMALIZED_TOPIC:
                            self._record_event(payload)
                        elif topic == HEALTH_TOPIC:
                            self._record_health(payload)
                        elif topic == REMEDIATION_RESULTS_TOPIC:
                            self._record_remediation_result(payload)
                self._recompute_latest()
            except Exception:
                time.sleep(0.5)

    def _record_event(self, payload: dict[str, Any]) -> None:
        event = {
            **payload,
            "_event_epoch": parse_timestamp_to_epoch(payload.get("timestamp")),
        }
        with self._lock:
            self._events.append(event)
            self._trim_locked(time.time())

    def _record_health(self, payload: dict[str, Any]) -> None:
        with self._lock:
            self._health[payload["service"]] = payload

    def _record_remediation_result(self, payload: dict[str, Any]) -> None:
        incident_id = payload.get("incident_id")
        if not incident_id:
            return
        with self._lock:
            self._incident_remediation[incident_id] = dict(payload)
            existing_record = self._incident_records.get(incident_id)
        if not existing_record:
            return
        self._persist_incident(
            incident_id=incident_id,
            rca_payload=existing_record.get("rca"),
            timeline_payload=existing_record.get("timeline"),
            remediation_payload=dict(payload),
            created_at=existing_record.get("created_at"),
        )

    def _trim_locked(self, now: float) -> None:
        cutoff = now - WINDOW_SECONDS
        while self._events and self._events[0]["_event_epoch"] < cutoff:
            self._events.popleft()
        while self._history and parse_timestamp_to_epoch(self._history[0]["evaluated_at"]) < cutoff:
            self._history.popleft()
        stale_incidents = [
            incident_id
            for incident_id, timings in self._incident_timings.items()
            if parse_timestamp_to_epoch(timings.get("event_ingestion_timestamp")) < cutoff
            and incident_id not in self._active_incident_ids
        ]
        for incident_id in stale_incidents:
            self._incident_timings.pop(incident_id, None)
            self._triggered_incidents.pop(incident_id, None)

    def _recompute_latest(self) -> None:
        with self._lock:
            now = time.time()
            self._trim_locked(now)
            events = list(self._events)
            health = dict(self._health)

        degraded_or_failed = {
            service: info
            for service, info in health.items()
            if info.get("status") in {"DEGRADED", "FAILED"}
        }
        if not degraded_or_failed:
            result = {
                "primary_root_cause": [],
                "secondary_root_causes": [],
                "independent_failures": [],
                "confidence": 0.0,
                "confidence_breakdown": {},
                "confidence_explanation": "All services are healthy in the active window",
                "status": "UNCERTAIN",
                "alternative_causes": [],
                "affected_services": [],
                "missing_impact": [],
                "causal_chain": [],
                "reasoning": ["All services are healthy in the active window"],
                "incident_id": None,
                "timings": {},
                "evaluated_at": utc_timestamp_ms(),
                "window_seconds": WINDOW_SECONDS,
                "propagation_delay_seconds": PROPAGATION_DELAY_SECONDS,
            }
            with self._lock:
                self._latest = result
            return

        hypotheses = self._build_hypotheses(events, health, degraded_or_failed)
        if not hypotheses:
            with self._lock:
                self._active_incident_ids = set()
            result = {
                "primary_root_cause": [],
                "secondary_root_causes": [],
                "independent_failures": [],
                "confidence": 0.12,
                "confidence_breakdown": {},
                "confidence_explanation": "No candidate could explain downstream impact in the active sliding window",
                "status": "UNCERTAIN",
                "alternative_causes": [],
                "affected_services": list(degraded_or_failed.keys()),
                "missing_impact": [],
                "causal_chain": [],
                "reasoning": ["No candidate could explain downstream impact in the active sliding window"],
                "incident_id": None,
                "timings": {},
                "evaluated_at": utc_timestamp_ms(),
                "window_seconds": WINDOW_SECONDS,
                "propagation_delay_seconds": PROPAGATION_DELAY_SECONDS,
            }
            with self._lock:
                self._latest = result
                self._history.append(result)
            return

        ordered = sorted(hypotheses, key=lambda item: item.score, reverse=True)
        evaluated_at = utc_timestamp_ms()
        self._sync_incident_timings(hypotheses, evaluated_at)
        primary = ordered[0]
        independent_failures = primary.independent_failures
        secondary_root_causes = self._secondary_roots(primary, ordered[1:])
        alternatives = [
            candidate for candidate in ordered[1:3]
            if candidate.service not in secondary_root_causes and candidate.service not in independent_failures
        ]
        uncertain = (
            (not primary.valid)
            or bool(primary.missing_impact)
            or bool(alternatives and primary.score - alternatives[0].score <= UNCERTAIN_MARGIN)
        )
        if primary.missing_impact:
            status = "PARTIAL_PROPAGATION"
        elif secondary_root_causes or independent_failures:
            status = "MULTI_CAUSE"
        elif uncertain:
            status = "UNCERTAIN"
        else:
            status = "CONFIDENT"
        reasoning = list(primary.reasoning)
        if primary.missing_impact:
            reasoning.append(
                f"Missing downstream impact: {', '.join(primary.missing_impact)}"
            )
        if secondary_root_causes:
            reasoning.append(
                f"Secondary root causes detected: {', '.join(secondary_root_causes)}"
            )
        if independent_failures:
            reasoning.append(
                f"Independent failures detected: {', '.join(independent_failures)}"
            )
        if alternatives:
            reasoning.append(
                f"Alternative hypothesis: {alternatives[0].service} scored {alternatives[0].confidence}"
            )
        incident_id = self._incident_id(primary)
        timings = self._build_timing_snapshot(incident_id, primary, evaluated_at)

        result = {
            "incident_id": incident_id,
            "primary_root_cause": [primary.service],
            "secondary_root_causes": secondary_root_causes,
            "independent_failures": independent_failures,
            "confidence": primary.confidence,
            "confidence_breakdown": {
                "temporal_consistency": primary.temporal_consistency,
                "dependency_match": primary.dependency_match,
                "signal_strength": primary.signal_strength,
                "noise_penalty": primary.noise_penalty,
                "impact_match": primary.impact_match,
                "repeat_pattern_bonus": primary.repeated_pattern_bonus,
                "raw_score": round(
                    0.3 * primary.temporal_consistency
                    + 0.3 * primary.dependency_match
                    + 0.3 * primary.signal_strength
                    + primary.noise_penalty,
                    3,
                ),
                "final_confidence": primary.confidence,
            },
            "confidence_explanation": self._confidence_explanation(
                primary,
                alternatives[0] if alternatives else None,
            ),
            "status": status,
            "alternative_causes": [
                {"service": candidate.service, "confidence": candidate.confidence}
                for candidate in alternatives
            ],
            "affected_services": primary.affected_services,
            "missing_impact": primary.missing_impact,
            "causal_chain": primary.causal_chain,
            "reasoning": reasoning,
            "timings": timings,
            "evaluated_at": evaluated_at,
            "window_seconds": WINDOW_SECONDS,
            "propagation_delay_seconds": PROPAGATION_DELAY_SECONDS,
        }
        remediation_triggered = self._trigger_remediation_if_needed(result)
        result["timings"]["remediation_triggered_at"] = remediation_triggered
        result["timings"]["total_pipeline_time_ms"] = self._duration_ms(
            result["timings"].get("event_ingestion_timestamp"),
            remediation_triggered,
        )
        result["timings"]["detection_to_rca_time_ms"] = self._duration_ms(
            result["timings"].get("anomaly_detection_timestamp"),
            result["timings"].get("rca_computation_timestamp"),
        )
        result["timings"]["status"] = "completed" if remediation_triggered else "active"
        self._incident_timings[incident_id] = dict(result["timings"])
        self._record_latency_sample(result)
        timeline_payload = self._timeline_for(result, events).get("timeline", [])
        with self._lock:
            remediation_payload = dict(self._incident_remediation.get(incident_id, {}))
        self._persist_incident(
            incident_id=incident_id,
            rca_payload=result,
            timeline_payload=timeline_payload,
            remediation_payload=remediation_payload,
            created_at=result.get("evaluated_at"),
        )
        with self._lock:
            self._latest = result
            self._history.append(result)

    def _build_hypotheses(
        self,
        events: list[dict[str, Any]],
        health: dict[str, dict[str, Any]],
        degraded_or_failed: dict[str, dict[str, Any]],
    ) -> list[Hypothesis]:
        hypotheses: list[Hypothesis] = []
        for service in degraded_or_failed:
            service_events = [
                event for event in events if event["service"] == service and event.get("status") in {"FAILED", "DEGRADED"}
            ]
            if not service_events:
                continue
            first_event = min(service_events, key=lambda item: item["_event_epoch"])
            affected_services, missing_impact_candidates, causal_chain, impact_match_score, valid = self._propagation_assessment(service, events, first_event)
            temporal_consistency = self._temporal_consistency(service, first_event, events)
            dependency_match = self._dependency_consistency(service, first_event, events)
            signal_strength = self._signal_strength(service, first_event, events, health.get(service, {}))
            repeated_patterns = self._repeated_pattern_bonus(service)
            missing_impact, independent_failures = self._classify_missing_or_independent(
                events,
                missing_impact_candidates,
            )
            noise_penalty = self._noise_penalty(
                service=service,
                first_event=first_event,
                events=events,
                missing_impact=missing_impact,
                valid=valid,
            )

            raw_score = (
                0.3 * temporal_consistency
                + 0.3 * dependency_match
                + 0.3 * signal_strength
                + noise_penalty
            )
            confidence = round(max(0.0, min(1.0, raw_score)), 2)
            reasoning = self._reasoning(
                service=service,
                first_event=first_event,
                affected_services=affected_services,
                missing_impact=missing_impact,
                causal_chain=causal_chain,
                valid=valid,
                impact_match_score=impact_match_score,
                independent_failures=independent_failures,
                events=events,
            )
            hypotheses.append(
                Hypothesis(
                    service=service,
                    score=confidence,
                    confidence=confidence,
                    temporal_consistency=round(temporal_consistency, 2),
                    dependency_match=round(dependency_match, 2),
                    signal_strength=round(signal_strength, 2),
                    noise_penalty=round(noise_penalty, 2),
                    impact_match=round(impact_match_score, 2),
                    repeated_pattern_bonus=round(repeated_patterns, 2),
                    affected_services=affected_services,
                    missing_impact=missing_impact,
                    independent_failures=independent_failures,
                    causal_chain=causal_chain,
                    reasoning=reasoning,
                    first_timestamp=first_event["timestamp"],
                    first_ingestion_timestamp=first_event.get("ingestion_timestamp"),
                    anomaly_detection_timestamp=first_event.get("processing_timestamp"),
                    status=first_event.get("status", "DEGRADED"),
                    valid=valid,
                )
            )
        return [hypothesis for hypothesis in hypotheses if hypothesis.confidence > 0.0]

    def _propagation_assessment(
        self,
        service: str,
        events: list[dict[str, Any]],
        first_event: dict[str, Any],
    ) -> tuple[list[str], list[str], list[str], float, bool]:
        root_epoch = first_event["_event_epoch"]
        expected = self._all_downstream(service)
        observed: list[str] = []
        missing: list[str] = []
        chain = [service]
        for depth, downstream in enumerate(expected, start=1):
            downstream_events = [
                event for event in events if event["service"] == downstream and event.get("status") in {"FAILED", "DEGRADED"}
            ]
            if not downstream_events:
                missing.append(downstream)
                continue
            first_downstream = min(downstream_events, key=lambda item: item["_event_epoch"])
            if root_epoch <= first_downstream["_event_epoch"] <= root_epoch + PROPAGATION_DELAY_SECONDS * depth:
                observed.append(downstream)
                chain.append(downstream)
            else:
                missing.append(downstream)

        if not expected:
            return observed, missing, [" -> ".join(chain)], 0.8, True

        match_score = len(observed) / len(expected)
        valid = len(observed) > 0
        return observed, missing, [" -> ".join(chain)], round(match_score, 2), valid

    def _all_downstream(self, service: str) -> list[str]:
        order: list[str] = []
        queue: deque[str] = deque(DEPENDENTS_GRAPH.get(service, []))
        while queue:
            current = queue.popleft()
            order.append(current)
            for child in DEPENDENTS_GRAPH.get(current, []):
                queue.append(child)
        return order

    def _temporal_consistency(self, service: str, first_event: dict[str, Any], events: list[dict[str, Any]]) -> float:
        upstream = [
            event
            for event in events
            if event["service"] in DEPENDENCY_GRAPH.get(service, [])
            and event.get("status") in {"FAILED", "DEGRADED"}
        ]
        if not upstream:
            return 1.0
        conflicts = len(
            [
                event
                for event in upstream
                if event["_event_epoch"] <= first_event["_event_epoch"] + PROPAGATION_DELAY_SECONDS
            ]
        )
        return max(0.0, 1.0 - 0.5 * conflicts)

    def _dependency_consistency(self, service: str, first_event: dict[str, Any], events: list[dict[str, Any]]) -> float:
        upstream = [
            event
            for event in events
            if event["service"] in DEPENDENCY_GRAPH.get(service, [])
            and event.get("status") in {"FAILED", "DEGRADED"}
            and event["_event_epoch"] < first_event["_event_epoch"]
        ]
        return 1.0 if not upstream else 0.35

    def _signal_strength(
        self,
        service: str,
        event: dict[str, Any],
        events: list[dict[str, Any]],
        health: dict[str, Any],
    ) -> float:
        service_events = [item for item in events if item.get("service") == service]
        anomaly_events = [
            item for item in service_events
            if item.get("status") in {"FAILED", "DEGRADED"} or item.get("event_type") == "ERROR"
        ]
        error_events = [item for item in anomaly_events if item.get("event_type") == "ERROR"]
        failed_events = [item for item in anomaly_events if item.get("status") == "FAILED"]
        latency_spikes = [item for item in anomaly_events if float(item.get("latency") or 0.0) > 500.0]

        error_score = min(1.0, len(error_events) / 5.0)
        failure_score = min(1.0, len(failed_events) / 3.0)
        latency_score = min(1.0, len(latency_spikes) / 4.0)
        health_error_score = min(1.0, float(health.get("recent_error_count", 0)) / 10.0)
        first_event_bonus = 0.2 if event.get("status") == "FAILED" else 0.1 if event.get("status") == "DEGRADED" else 0.0

        combined = (
            0.35 * error_score
            + 0.35 * failure_score
            + 0.2 * latency_score
            + 0.1 * health_error_score
            + first_event_bonus
        )
        return round(max(0.0, min(1.0, combined)), 2)

    def _repeated_pattern_bonus(self, service: str) -> float:
        if not self._history:
            return 0.0
        matches = 0
        for item in self._history:
            roots = item.get("primary_root_cause", [])
            if isinstance(roots, list) and service in roots:
                matches += 1
        return min(1.0, matches / 4.0)

    def _noise_factor(self, service: str, first_event: dict[str, Any], events: list[dict[str, Any]]) -> float:
        competing = [
            event
            for event in events
            if event["service"] != service
            and event.get("status") in {"FAILED", "DEGRADED"}
            and abs(event["_event_epoch"] - first_event["_event_epoch"]) <= PROPAGATION_DELAY_SECONDS
            and event["service"] not in DEPENDENCY_GRAPH.get(service, [])
            and event["service"] not in DEPENDENTS_GRAPH.get(service, [])
        ]
        return min(1.0, len(competing) / 3.0)

    def _noise_penalty(
        self,
        *,
        service: str,
        first_event: dict[str, Any],
        events: list[dict[str, Any]],
        missing_impact: list[str],
        valid: bool,
    ) -> float:
        conflicting_signals = self._noise_factor(service, first_event, events)
        missing_signal_penalty = min(0.2, 0.08 * len(missing_impact))
        validity_penalty = 0.08 if not valid else 0.0
        competing_root_penalty = min(0.12, 0.04 * len({
            event.get("service")
            for event in events
            if event.get("service") != service
            and event.get("status") in {"FAILED", "DEGRADED"}
            and abs(event.get("_event_epoch", 0.0) - first_event.get("_event_epoch", 0.0)) <= PROPAGATION_DELAY_SECONDS
        }))
        penalty = min(0.45, 0.15 * conflicting_signals + missing_signal_penalty + validity_penalty + competing_root_penalty)
        return round(-penalty, 2)

    def _classify_missing_or_independent(
        self,
        events: list[dict[str, Any]],
        missing_impact_candidates: list[str],
    ) -> tuple[list[str], list[str]]:
        missing_signals: list[str] = []
        independent: list[str] = []
        for candidate in missing_impact_candidates:
            candidate_events = [
                event
                for event in events
                if event["service"] == candidate and event.get("status") in {"FAILED", "DEGRADED"}
            ]
            if not candidate_events:
                missing_signals.append(candidate)
                continue
            candidate_first = min(candidate_events, key=lambda item: item["_event_epoch"])
            upstream_failures = [
                event
                for event in events
                if event["service"] in DEPENDENCY_GRAPH.get(candidate, [])
                and event.get("status") in {"FAILED", "DEGRADED"}
                and event["_event_epoch"] < candidate_first["_event_epoch"]
            ]
            if not upstream_failures:
                independent.append(candidate)
            else:
                missing_signals.append(candidate)
        return missing_signals, independent

    def _secondary_roots(self, primary: Hypothesis, candidates: list[Hypothesis]) -> list[str]:
        secondaries: list[str] = []
        for candidate in candidates:
            if candidate.service in DEPENDENCY_GRAPH.get(primary.service, []) or candidate.service in DEPENDENTS_GRAPH.get(primary.service, []):
                continue
            if candidate.score >= 0.45:
                secondaries.append(candidate.service)
        return secondaries[:2]

    def _confidence_explanation(self, primary: Hypothesis, alternative: Hypothesis | None) -> str:
        chain = primary.causal_chain[0] if primary.causal_chain else self._display_name(primary.service)
        propagation_text = "clear propagation to dependents" if primary.affected_services else "limited downstream propagation"
        signal_text = (
            "strong error and failure signals" if primary.signal_strength >= 0.7
            else "moderate anomaly signals" if primary.signal_strength >= 0.4
            else "weak anomaly signals"
        )
        noise_text = (
            "low noise" if primary.noise_penalty >= -0.08
            else "noticeable competing noise" if primary.noise_penalty >= -0.2
            else "high noise from conflicting or missing signals"
        )
        explanation = (
            f"Confidence {primary.confidence:.2f}: {self._display_name(primary.service)} appears to be the root cause "
            f"with {propagation_text} along {chain}, {signal_text}, and {noise_text}."
        )
        if alternative:
            margin = round(primary.confidence - alternative.confidence, 3)
            explanation += (
                f" Next-best alternative is {self._display_name(alternative.service)} "
                f"(confidence {alternative.confidence:.2f}, confidence gap {margin:.3f})."
            )
        return explanation

    def _reasoning(
        self,
        *,
        service: str,
        first_event: dict[str, Any],
        affected_services: list[str],
        missing_impact: list[str],
        causal_chain: list[str],
        valid: bool,
        impact_match_score: float,
        independent_failures: list[str],
        events: list[dict[str, Any]],
    ) -> list[str]:
        reasoning = [f"{self._label(service)} failed first"]
        if affected_services:
            for affected in affected_services:
                affected_events = [
                    event
                    for event in events
                    if event["service"] == affected and event.get("status") in {"FAILED", "DEGRADED"}
                ]
                if affected_events:
                    reasoning.append(
                        f"{self._label(affected)} degraded after {self._label(service)} failure"
                    )
            reasoning.append("Observed impact matches dependency graph")
        else:
            reasoning.append("No downstream impact was observed inside the propagation window")
        if missing_impact:
            for missing in missing_impact:
                if missing in independent_failures:
                    reasoning.append(f"{self._label(missing)} has its own anomaly signal and is treated as an independent failure")
                else:
                    reasoning.append(f"{self._label(missing)} has no downstream anomaly signal and is treated as missing_signal")
        if causal_chain:
            reasoning.append(f"Dependency chain: {causal_chain[0]}")
        if not valid:
            reasoning.append("Candidate remains uncertain because it does not explain downstream impact")
        elif impact_match_score < 1.0:
            reasoning.append("Some expected downstream impact is missing, reducing confidence")
        return reasoning

    def _label(self, service: str) -> str:
        return service.replace("-service", "")

    def _incident_id(self, hypothesis: Hypothesis) -> str:
        base_timestamp = hypothesis.first_ingestion_timestamp or hypothesis.first_timestamp or utc_timestamp_ms()
        timestamp_ms = int(parse_timestamp_to_epoch(base_timestamp) * 1000.0)
        return f"inc_{timestamp_ms}_{hypothesis.service}"

    def _incident_key(self, incident_id: str) -> str:
        return f"incident:{incident_id}"

    def _persist_incident(
        self,
        *,
        incident_id: str,
        rca_payload: dict[str, Any] | None,
        timeline_payload: list[dict[str, Any]] | None,
        remediation_payload: dict[str, Any] | None,
        created_at: str | None,
    ) -> None:
        with self._lock:
            existing = self._incident_records.get(incident_id, {})
            incident_record = {
                "incident_id": incident_id,
                "rca": rca_payload if rca_payload is not None else dict(existing.get("rca", {})),
                "timeline": timeline_payload if timeline_payload is not None else list(existing.get("timeline", [])),
                "remediation": remediation_payload if remediation_payload is not None else dict(existing.get("remediation", {})),
                "created_at": created_at or existing.get("created_at") or utc_timestamp_ms(),
            }
            self._incident_records[incident_id] = incident_record
            if incident_id in self._incident_order:
                self._incident_order.remove(incident_id)
            self._incident_order.appendleft(incident_id)
            while len(self._incident_order) > INCIDENT_HISTORY_LIMIT:
                dropped = self._incident_order.pop()
                self._incident_records.pop(dropped, None)

        client = self._redis_client
        if client is None:
            return
        try:
            client.set(self._incident_key(incident_id), json.dumps(incident_record, ensure_ascii=True))
            client.lrem(INCIDENT_INDEX_KEY, 0, incident_id)
            client.lpush(INCIDENT_INDEX_KEY, incident_id)
            overflow = client.lrange(INCIDENT_INDEX_KEY, INCIDENT_HISTORY_LIMIT, -1)
            if overflow:
                for overflow_incident_id in overflow:
                    client.delete(self._incident_key(overflow_incident_id))
            client.ltrim(INCIDENT_INDEX_KEY, 0, INCIDENT_HISTORY_LIMIT - 1)
        except Exception:
            return

    def list_incidents(self) -> list[str]:
        client = self._redis_client
        if client is not None:
            try:
                return [str(item) for item in client.lrange(INCIDENT_INDEX_KEY, 0, INCIDENT_HISTORY_LIMIT - 1)]
            except Exception:
                pass
        with self._lock:
            return list(self._incident_order)

    def get_incident(self, incident_id: str) -> dict[str, Any] | None:
        client = self._redis_client
        if client is not None:
            try:
                payload = client.get(self._incident_key(incident_id))
                if payload:
                    return json.loads(payload)
            except Exception:
                pass
        with self._lock:
            record = self._incident_records.get(incident_id)
            if record is None:
                return None
            return dict(record)

    def _build_timing_snapshot(self, incident_id: str, hypothesis: Hypothesis, evaluated_at: str) -> dict[str, Any]:
        existing = self._incident_timings.get(incident_id, {})
        rca_timestamp = existing.get("rca_computation_timestamp", evaluated_at)
        return {
            "event_ingestion_timestamp": existing.get("event_ingestion_timestamp", hypothesis.first_ingestion_timestamp),
            "anomaly_detection_timestamp": existing.get("anomaly_detection_timestamp", hypothesis.anomaly_detection_timestamp),
            "rca_computation_timestamp": rca_timestamp,
            "detection_to_rca_time_ms": self._duration_ms(
                existing.get("anomaly_detection_timestamp", hypothesis.anomaly_detection_timestamp),
                rca_timestamp,
            ),
            "total_pipeline_time_ms": existing.get("total_pipeline_time_ms"),
            "remediation_triggered_at": existing.get("remediation_triggered_at"),
            "status": existing.get("status", "active"),
        }

    def _sync_incident_timings(self, hypotheses: list[Hypothesis], evaluated_at: str) -> None:
        active_ids: set[str] = set()
        for hypothesis in hypotheses:
            incident_id = self._incident_id(hypothesis)
            active_ids.add(incident_id)
            existing = self._incident_timings.get(incident_id, {})
            rca_timestamp = existing.get("rca_computation_timestamp", evaluated_at)
            self._incident_timings[incident_id] = {
                "event_ingestion_timestamp": existing.get("event_ingestion_timestamp", hypothesis.first_ingestion_timestamp),
                "anomaly_detection_timestamp": existing.get("anomaly_detection_timestamp", hypothesis.anomaly_detection_timestamp),
                "rca_computation_timestamp": rca_timestamp,
                "detection_to_rca_time_ms": self._duration_ms(
                    existing.get("anomaly_detection_timestamp", hypothesis.anomaly_detection_timestamp),
                    rca_timestamp,
                ),
                "remediation_triggered_at": existing.get("remediation_triggered_at"),
                "total_pipeline_time_ms": existing.get("total_pipeline_time_ms"),
                "status": "completed" if existing.get("remediation_triggered_at") else "active",
                "root_service": hypothesis.service,
            }
        with self._lock:
            self._active_incident_ids = active_ids

    def _trigger_remediation_if_needed(self, result: dict[str, Any]) -> str | None:
        incident_id = result.get("incident_id")
        if not incident_id:
            return result.get("timings", {}).get("remediation_triggered_at")
        if incident_id in self._triggered_incidents:
            return self._triggered_incidents[incident_id]
        trigger_timestamp = utc_timestamp_ms()
        trigger_payload = {
            "incident_id": incident_id,
            "timestamp": trigger_timestamp,
            "service": SERVICE_NAME,
            "root_causes": result.get("primary_root_cause", []),
            "status": result.get("status"),
            "confidence": result.get("confidence"),
            "affected_services": result.get("affected_services", []),
        }
        try:
            self.backend.publish(REMEDIATION_TOPIC, trigger_payload)
            self.logger.info(
                json.dumps(
                    {
                        "stage": "remediation_triggered",
                        "incident_id": incident_id,
                        "payload": trigger_payload,
                    },
                    ensure_ascii=True,
                )
            )
        except Exception:
            return None
        self._last_triggered_incident_id = incident_id
        self._triggered_incidents[incident_id] = trigger_timestamp
        if incident_id in self._incident_timings:
            self._incident_timings[incident_id]["remediation_triggered_at"] = trigger_timestamp
            self._incident_timings[incident_id]["total_pipeline_time_ms"] = self._duration_ms(
                self._incident_timings[incident_id].get("event_ingestion_timestamp"),
                trigger_timestamp,
            )
            self._incident_timings[incident_id]["status"] = "completed"
        return trigger_timestamp

    def _duration_ms(self, start_timestamp: str | None, end_timestamp: str | None) -> float | None:
        if not start_timestamp or not end_timestamp:
            return None
        return round(
            max(
                0.0,
                (parse_timestamp_to_epoch(end_timestamp) - parse_timestamp_to_epoch(start_timestamp)) * 1000.0,
            ),
            2,
        )

    def _record_latency_sample(self, result: dict[str, Any]) -> None:
        timings = result.get("timings", {})
        sample = {
            "incident_id": result.get("incident_id"),
            "event_ingestion_timestamp": timings.get("event_ingestion_timestamp"),
            "anomaly_detection_timestamp": timings.get("anomaly_detection_timestamp"),
            "rca_computation_timestamp": timings.get("rca_computation_timestamp"),
            "remediation_triggered_at": timings.get("remediation_triggered_at"),
            "detection_to_rca_time_ms": timings.get("detection_to_rca_time_ms"),
            "total_pipeline_time_ms": timings.get("total_pipeline_time_ms"),
            "status": timings.get("status", "active"),
        }
        with self._lock:
            if self._latency_history and self._latency_history[-1].get("incident_id") == sample["incident_id"]:
                self._latency_history[-1] = sample
            else:
                self._latency_history.append(sample)
        self.logger.info(
            json.dumps(
                {
                    "stage": "incident_latency",
                    "incident_id": sample["incident_id"],
                    "timings": sample,
                    "within_15s": bool(
                        sample["total_pipeline_time_ms"] is not None
                        and sample["total_pipeline_time_ms"] <= PIPELINE_SLA_MS
                    ),
                },
                ensure_ascii=True,
            )
        )

    def _relevant_services(self, latest: dict[str, Any]) -> list[str]:
        services: list[str] = []
        for field in (
            "primary_root_cause",
            "secondary_root_causes",
            "independent_failures",
            "affected_services",
            "missing_impact",
        ):
            for service in latest.get(field, []):
                if service not in services:
                    services.append(service)
        return services

    def _candidate_timeline_events(
        self,
        events: list[dict[str, Any]],
        relevant_services: list[str],
        incident_start_epoch: float,
        incident_end_epoch: float,
    ) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        for event in events:
            if event.get("service") not in relevant_services:
                continue
            event_epoch = event.get("_event_epoch", 0.0)
            if event_epoch < incident_start_epoch or event_epoch > incident_end_epoch:
                continue
            if not self._is_relevant_timeline_event(event):
                continue
            candidates.append(event)
        candidates.sort(
            key=lambda item: (
                item.get("_event_epoch", 0.0),
                parse_timestamp_to_epoch(item.get("processing_timestamp")),
                item.get("service", ""),
                item.get("event_type", ""),
            )
        )
        return candidates

    def _is_relevant_timeline_event(self, event: dict[str, Any]) -> bool:
        if event.get("status") in {"FAILED", "DEGRADED"}:
            return True
        if event.get("event_type") == "ERROR":
            return True
        return float(event.get("latency") or 0.0) > 500.0

    def _timeline_summary(self, event: dict[str, Any], latest: dict[str, Any]) -> str:
        service = event.get("service")
        status = event.get("status")
        event_type = event.get("event_type")
        latency = float(event.get("latency") or 0.0)
        primary_roots = set(latest.get("primary_root_cause", []))
        affected = set(latest.get("affected_services", []))
        independent = set(latest.get("independent_failures", []))
        root_name = self._display_name(next(iter(primary_roots), service))

        if service == "database-service":
            if service in primary_roots:
                if event_type == "ERROR" or status == "FAILED":
                    return "Database failed"
                if latency > 500.0 or status == "DEGRADED":
                    return "Database latency increased"
                return "Database degraded"
            if service in independent:
                return "Database failed independently"

        if service == "cache-service":
            if service in primary_roots:
                if event_type == "ERROR" or status == "FAILED":
                    return "Cache failed"
                if latency > 500.0 or status == "DEGRADED":
                    return "Cache latency increased"
                return "Cache degraded"
            if service in affected:
                return f"Cache failed due to {root_name} issue"
            if service in independent:
                return "Cache failed independently"

        if service == "api-service":
            if service in primary_roots:
                if event_type == "ERROR" or status == "FAILED":
                    return "API failed"
                if latency > 500.0 or status == "DEGRADED":
                    return "API response slowed down"
                return "API degraded"
            if service in affected:
                if "cache-service" in latest.get("affected_services", []) or "cache-service" in latest.get("causal_chain", [""])[0]:
                    return "API response slowed due to cache failure"
                return f"API response slowed due to {root_name} issue"
            if service in independent:
                return "API failed independently"

        if service in primary_roots:
            if event_type == "ERROR" or status == "FAILED":
                return f"{self._display_name(service)} failed"
            if latency > 500.0:
                return f"{self._display_name(service)} latency increased"
            return f"{self._display_name(service)} degraded"
        if service in independent:
            return f"{self._display_name(service)} failed independently"
        if service in affected:
            return f"{self._display_name(service)} was impacted by upstream issue"
        if status == "FAILED":
            return f"{self._display_name(service)} failed"
        if status == "DEGRADED":
            return f"{self._display_name(service)} degraded"
        if latency > 500.0:
            return f"{self._display_name(service)} latency increased"
        return f"{self._display_name(service)} had a relevant event"

    def _timeline_impact_level(self, service: str, latest: dict[str, Any]) -> str:
        if service in set(latest.get("primary_root_cause", [])):
            return "ROOT"
        causal_chain = latest.get("causal_chain", [])
        if not causal_chain:
            return "DOWNSTREAM"
        chain_services = [part.strip() for part in causal_chain[0].split("->")]
        if service in chain_services:
            index = chain_services.index(service)
            return "DOWNSTREAM" if index == 1 else "INDIRECT"
        return "DOWNSTREAM"

    def _display_name(self, service: str | None) -> str:
        if service == "database-service":
            return "Database"
        if service == "cache-service":
            return "Cache"
        if service == "api-service":
            return "API"
        if not service:
            return "Service"
        return service.replace("-service", "").replace("-", " ").title()

    def _timeline_for(self, latest: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
        incident_id = latest.get("incident_id")
        if not incident_id:
            return {
                "incident_id": None,
                "window_seconds": WINDOW_SECONDS,
                "timeline": [],
            }

        relevant_services = self._relevant_services(latest)
        if not relevant_services:
            return {
                "incident_id": incident_id,
                "window_seconds": WINDOW_SECONDS,
                "timeline": [],
            }

        root_services = latest.get("primary_root_cause", [])
        root_timestamps = [
            parse_timestamp_to_epoch(event.get("timestamp"))
            for event in events
            if event.get("service") in root_services and event.get("status") in {"FAILED", "DEGRADED"}
        ]
        incident_end_epoch = parse_timestamp_to_epoch(latest.get("evaluated_at"))
        incident_start_epoch = min(root_timestamps) if root_timestamps else incident_end_epoch - WINDOW_SECONDS
        candidate_events = self._candidate_timeline_events(
            events,
            relevant_services,
            incident_start_epoch,
            incident_end_epoch,
        )

        timeline: list[dict[str, Any]] = []
        selected_services: set[str] = set()
        for event in candidate_events:
            service = event["service"]
            if service in selected_services:
                continue
            timeline.append(
                {
                    "time": event.get("timestamp"),
                    "service": service,
                    "message": self._timeline_summary(event, latest),
                    "root_cause": service in set(latest.get("primary_root_cause", [])),
                    "impact_level": self._timeline_impact_level(service, latest),
                    "event_type": event.get("event_type"),
                    "severity": event.get("status"),
                }
            )
            selected_services.add(service)

        timeline.sort(key=lambda item: (parse_timestamp_to_epoch(item.get("time")), item.get("service", "")))
        return {
            "incident_id": incident_id,
            "window_seconds": WINDOW_SECONDS,
            "causal_chain": latest.get("causal_chain", []),
            "timeline": timeline,
        }

    def rca_timeline(self) -> dict[str, Any]:
        with self._lock:
            latest = dict(self._latest)
            events = list(self._events)
        return self._timeline_for(latest, events)

    def latest(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._latest)

    def debug_latency(self) -> dict[str, Any]:
        with self._lock:
            incident_timings = {
                incident_id: dict(timings)
                for incident_id, timings in self._incident_timings.items()
            }
            active_incident_ids = set(self._active_incident_ids)
        samples = [
            {"incident_id": incident_id, **timings}
            for incident_id, timings in incident_timings.items()
        ]
        durations = [
            sample["total_pipeline_time_ms"]
            for sample in samples
            if sample.get("total_pipeline_time_ms") is not None
        ]
        detection_to_rca = [
            sample["detection_to_rca_time_ms"]
            for sample in samples
            if sample.get("detection_to_rca_time_ms") is not None
        ]
        avg_latency = round(sum(durations) / len(durations), 2) if durations else 0.0
        p95_latency = 0.0
        if durations:
            ordered = sorted(durations)
            index = max(0, min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1)))))
            p95_latency = round(ordered[index], 2)
        last_pipeline_time = durations[-1] if durations else 0.0
        last_detection_to_rca = detection_to_rca[-1] if detection_to_rca else 0.0
        return {
            "samples": len(durations),
            "avg_latency_ms": avg_latency,
            "p95_latency_ms": p95_latency,
            "last_pipeline_time_ms": last_pipeline_time,
            "last_detection_to_rca_time_ms": last_detection_to_rca,
            "active_incidents": len(active_incident_ids),
            "incident_breakdown": [
                {
                    "incident_id": sample["incident_id"],
                    "total_latency_ms": sample.get("total_pipeline_time_ms"),
                    "status": sample.get("status", "active"),
                }
                for sample in sorted(
                    samples,
                    key=lambda item: parse_timestamp_to_epoch(item.get("event_ingestion_timestamp")),
                )
            ],
            "within_15s": all(value <= PIPELINE_SLA_MS for value in durations) if durations else True,
        }


engine = RcaEngine()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield


app = FastAPI(title=SERVICE_NAME, lifespan=lifespan)

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"service": SERVICE_NAME, "status": "ok"}


@app.get("/rca/latest")
async def latest_rca() -> dict[str, Any]:
    return engine.latest()


@app.get("/rca/timeline")
async def rca_timeline() -> dict[str, Any]:
    return engine.rca_timeline()


@app.get("/incidents")
async def incidents() -> dict[str, list[str]]:
    return {"incident_ids": engine.list_incidents()}


@app.get("/incidents/{incident_id}")
async def incident_by_id(incident_id: str) -> dict[str, Any]:
    incident = engine.get_incident(incident_id)
    if incident is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    return incident


@app.get("/debug/latency")
async def debug_latency() -> dict[str, Any]:
    return engine.debug_latency()
