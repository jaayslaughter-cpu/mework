package com.propiq.model;

import lombok.Data;
import java.util.List;

/** Response from Python ML microservice /api/ml/audit-features */
@Data
public class AuditResponse {
    private double holdoutAccuracy;
    private List<String> validFeatures;
    private List<String> droppedFeatures;
    private String featureImportanceCsv;
}
