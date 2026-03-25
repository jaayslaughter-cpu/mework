package com.propiq.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

/**
 * PropIQHttpClient — Spring Boot HTTP client for the Python ML microservice.
 *
 * Replaces RabbitMQ/Kafka with direct HTTP calls to the FastAPI orchestrator.
 * Set ML_SERVICE_URL env var to the mework service private domain on Railway:
 *   ML_SERVICE_URL=http://mework.railway.internal:8080
 */
@Service
public class PropIQHttpClient {

    private static final Logger log = LoggerFactory.getLogger(PropIQHttpClient.class);

    private final RestTemplate restTemplate;
    private final String mlServiceUrl;

    public PropIQHttpClient(@Value("${propiq.ml.service-url}") String mlServiceUrl) {
        this.restTemplate = new RestTemplate();
        this.mlServiceUrl = mlServiceUrl.endsWith("/")
                ? mlServiceUrl.substring(0, mlServiceUrl.length() - 1)
                : mlServiceUrl;
        log.info("[PropIQHttpClient] ML service URL: {}", this.mlServiceUrl);
    }

    /** Trigger the 11 AM live dispatcher (all 19 agents + Discord). */
    @SuppressWarnings("unchecked")
    public Map<String, Object> triggerDispatch() {
        return post("/propiq/dispatch", "triggerDispatch");
    }

    /** Trigger the 2 AM nightly settlement engine (ESPN grading + Discord recap). */
    @SuppressWarnings("unchecked")
    public Map<String, Object> triggerSettle() {
        return post("/propiq/settle", "triggerSettle");
    }

    /** Full system status — scheduler state, prop counts, last run timestamps. */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getStatus() {
        return get("/propiq/status", "getStatus");
    }

    /** Season W/L record from Postgres propiq_season_record table. */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getRecord() {
        return get("/propiq/record", "getRecord");
    }

    /** Liveness probe — returns {status: healthy} when Python is up. */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getHealth() {
        return get("/health", "getHealth");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Map<String, Object> post(String path, String label) {
        String url = mlServiceUrl + path;
        try {
            ResponseEntity<Map> response = restTemplate.postForEntity(url, null, Map.class);
            log.info("[PropIQHttpClient] {} HTTP {}", label, response.getStatusCode());
            return response.getBody();
        } catch (RestClientException e) {
            log.error("[PropIQHttpClient] {} failed: {}", label, e.getMessage());
            return Map.of("error", e.getMessage(), "endpoint", url);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Map<String, Object> get(String path, String label) {
        String url = mlServiceUrl + path;
        try {
            ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);
            log.info("[PropIQHttpClient] {} HTTP {}", label, response.getStatusCode());
            return response.getBody();
        } catch (RestClientException e) {
            log.error("[PropIQHttpClient] {} failed: {}", label, e.getMessage());
            return Map.of("error", e.getMessage(), "endpoint", url);
        }
    }
}
