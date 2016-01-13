package com.linkedin.thirdeye.bootstrap.analysis;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;

public class AnalysisPhaseStats {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private Long minTime;
  private Long maxTime;
  private String inputPath;
  private Map<String, Set<String>> dimensionValues;

  public AnalysisPhaseStats() {
  }

  public long getMinTime() {
    return minTime;
  }

  public Map<String, Set<String>> getDimensionValues() {
    return dimensionValues;
  }

  public void setDimensionValues(Map<String, Set<String>> dimensionValues) {
    this.dimensionValues = dimensionValues;
  }

  public void setMinTime(long minTime) {
    this.minTime = minTime;
  }

  public long getMaxTime() {
    return maxTime;
  }

  public void setMaxTime(long maxTime) {
    this.maxTime = maxTime;
  }

  public String getInputPath() {
    return inputPath;
  }

  public void setInputPath(String inputPath) {
    this.inputPath = inputPath;
  }

  public void update(AnalysisPhaseStats stats) {
    if (minTime == null || stats.getMinTime() < minTime) {
      minTime = stats.getMinTime();
    }

    if (maxTime == null || stats.getMaxTime() > maxTime) {
      maxTime = stats.getMaxTime();
    }

    if (dimensionValues != null) {
      Map<String, Set<String>> tmp = stats.getDimensionValues();
      for (Map.Entry<String, Set<String>> entry : tmp.entrySet()) {
        String dimensionName = entry.getKey();
        Set<String> partialDimensionValues = entry.getValue();
        dimensionValues.get(dimensionName).addAll(partialDimensionValues);
      }
    } else {
      // deep copy the entire map;
      dimensionValues = new HashMap<String, Set<String>>();
      for (Map.Entry<String, Set<String>> entry : stats.dimensionValues.entrySet()) {
        dimensionValues.put(entry.getKey(), new HashSet<String>(entry.getValue()));
      }
    }
  }

  public byte[] toBytes() throws IOException {
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(this);
  }

  public static AnalysisPhaseStats fromBytes(byte[] bytes) throws IOException {
    return OBJECT_MAPPER.readValue(bytes, AnalysisPhaseStats.class);
  }
}
