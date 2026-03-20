package com.propiq.model;

import lombok.Data;
import java.util.List;
import java.util.Map;

/**
 * Request body for POST /analyze/bet
 *
 * Example:
 * { "players": ["Aaron Judge"], "props": ["O1.5 Hits"],
 *   "odds": {"draftkings": "-115"}, "platform": "PrizePicks" }
 */
@Data
public class BetRequest {
    private List<String> players;
    private List<String> props;
    private Map<String, String> odds;
    private String platform;   // PrizePicks or Underdog
    private String timestamp;
}
