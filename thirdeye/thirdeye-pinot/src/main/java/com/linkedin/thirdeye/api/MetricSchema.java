package com.linkedin.thirdeye.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Wrapper class to represent the metric schema <code>
 * e.g.
 * </code>
 * @author kgopalak
 */
public class MetricSchema {

  int[] coloffsets;

  private final List<MetricType> types;

  private int rowSize;

  Map<String, Integer> mapping;

  private final List<String> names;

  public MetricSchema(List<String> names, List<MetricType> types) {
    this.names = names;
    mapping = new HashMap<String, Integer>();
    this.types = types;
    int rowSize = 0;
    coloffsets = new int[names.size()];
    for (int i = 0; i < types.size(); i++) {
      coloffsets[i] = rowSize;
      rowSize += types.get(i).byteSize();
      mapping.put(names.get(i), i);
    }
    this.rowSize = rowSize;
  }

  public int getRowSizeInBytes() {
    return rowSize;
  }

  public int getOffset(String name) {
    return coloffsets[mapping.get(name)];
  }

  public int getNumMetrics() {
    return types.size();
  }

  public String getMetricName(int index) {
    return names.get(index);
  }

  public MetricType getMetricType(int index) {
    return types.get(index);
  }

  public MetricType getMetricType(String name) {
    return types.get(mapping.get(name));
  }

  public Integer getMetricIndex(String name) {
    return mapping.get(name);
  }

  public List<MetricType> getTypes() {
    return types;
  }

  public List<String> getNames() {
    return names;
  }

  public static MetricSchema fromMetricSpecs(List<MetricSpec> metricSpecs) {
    List<String> metricNames = new ArrayList<String>(metricSpecs.size());
    List<MetricType> metricTypes = new ArrayList<MetricType>(metricSpecs.size());

    for (MetricSpec metricSpec : metricSpecs) {
      metricNames.add(metricSpec.getName());
      metricTypes.add(metricSpec.getType());
    }

    return new MetricSchema(metricNames, metricTypes);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MetricSchema)) {
      return false;
    }

    MetricSchema s = (MetricSchema) o;

    return names.equals(s.names) && types.equals(s.types);
  }

  @Override
  public int hashCode() {
    return names.hashCode() + 13 * types.hashCode();
  }
}
