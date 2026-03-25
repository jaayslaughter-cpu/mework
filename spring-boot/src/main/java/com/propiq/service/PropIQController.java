package com.propiq.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * PropIQController — Spring Boot REST endpoints for PropIQ ML operations.
 *
 * Proxies to Python FastAPI orchestrator via PropIQHttpClient.
 * No RabbitMQ or Kafka required.
 *
 * POST /api/propiq/dispatch   trigger live dispatcher (19 agents + Discord)
 * POST /api/propiq/settle     trigger nightly settlement engine
 * GET  /api/propiq/status     full system status from Python orchestrator
 * GET  /api/propiq/record     season W/L record from Postgres
 * GET  /api/propiq/health     liveness probe for the Python service
 */
@RestController
@RequestMapping("/api/propiq")
public class PropIQController {

    private static final Logger log = LoggerFactory.getLogger(PropIQController.class);
    private final PropIQHttpClient httpClient;

    public PropIQController(PropIQHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @PostMapping("/dispatch")
    public ResponseEntity<Map<String, Object>> dispatch() {
        log.info("[PropIQController] Triggering live dispatch");
        Map<String, Object> result = httpClient.triggerDispatch();
        return result.containsKey("error") ? ResponseEntity.status(502).body(result) : ResponseEntity.ok(result);
    }

    @PostMapping("/settle")
    public ResponseEntity<Map<String, Object>> settle() {
        log.info("[PropIQController] Triggering settlement");
        Map<String, Object> result = httpClient.triggerSettle();
        return result.containsKey("error") ? ResponseEntity.status(502).body(result) : ResponseEntity.ok(result);
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> result = httpClient.getStatus();
        return result.containsKey("error") ? ResponseEntity.status(502).body(result) : ResponseEntity.ok(result);
    }

    @GetMapping("/record")
    public ResponseEntity<Map<String, Object>> record() {
        Map<String, Object> result = httpClient.getRecord();
        return result.containsKey("error") ? ResponseEntity.status(502).body(result) : ResponseEntity.ok(result);
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> result = httpClient.getHealth();
        return result.containsKey("error") ? ResponseEntity.status(502).body(result) : ResponseEntity.ok(result);
    }
}
