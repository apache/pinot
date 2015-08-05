package com.linkedin.thirdeye.dashboard.api.custom;

import com.linkedin.thirdeye.dashboard.util.ViewUtils;
import com.sun.jersey.core.util.MultivaluedMapImpl;

import javax.ws.rs.core.MultivaluedMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomDashboardComponentSpec {
  private String name;
  private Type type;
  private List<String> metrics;
  private Map<String, List<String>> dimensions;
  private String groupBy;

  public enum Type {
    FUNNEL,
    TIME_SERIES,
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public List<String> getMetrics() {
    return metrics;
  }

  public void setMetrics(List<String> metrics) {
    this.metrics = metrics;
  }

  public Map<String, List<String>> getDimensions() {
    return dimensions;
  }

  public Map<String, String> getFlattenedDimensions() {
    MultivaluedMap<String, String> multivaluedMap = new MultivaluedMapImpl();
    multivaluedMap.putAll(dimensions);
    return ViewUtils.flattenDisjunctions(multivaluedMap);
  }

  public void setDimensions(Map<String, List<String>> dimensions) {
    this.dimensions = dimensions;
  }

  public String getGroupBy() {
    return groupBy;
  }

  public void setGroupBy(String groupBy) {
    this.groupBy = groupBy;
  }
}
