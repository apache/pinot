package com.linkedin.thirdeye.dashboard.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HeatMapCell {
  private final ObjectMapper objectMapper;
  private final String value;
  private final List<Number> stats = new ArrayList<>();

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

  public String getStatsJson() throws IOException {
    return objectMapper.writeValueAsString(stats);
  }

  public void addStat(Number stat) {
    stats.add(stat);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(HeatMapCell.class)
        .add("value", value)
        .add("stats", stats)
        .toString();
  }
}
