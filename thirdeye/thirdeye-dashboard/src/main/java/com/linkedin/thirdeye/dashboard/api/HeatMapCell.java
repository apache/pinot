package com.linkedin.thirdeye.dashboard.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.linkedin.thirdeye.dashboard.views.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HeatMapCell {
  private final ObjectMapper objectMapper;
  private final String value;
  private final List<Number> stats = new ArrayList<>();
  private final Map<String,Number> statsMap = new HashMap<>();

  public HeatMapCell(ObjectMapper objectMapper, String value) {
    this.objectMapper = objectMapper;
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  public List<Number> getStats() {
    return stats;
  }
  
  public Map<String,Number> getStatsMap(){
    return statsMap;
  }

  public String getStatsJson() throws IOException {
    return objectMapper.writeValueAsString(stats);
  }

  public void addStat(Stat statName, Number statValue) {
    statsMap.put(statName.toString(), statValue);
    stats.add(statValue);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(HeatMapCell.class)
        .add("value", value)
        .add("stats", stats)
        .toString();
  }
}
