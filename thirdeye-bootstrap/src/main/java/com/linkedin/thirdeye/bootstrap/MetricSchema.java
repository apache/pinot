package com.linkedin.thirdeye.bootstrap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Wrapper class to represent the metric schema <code>
 * e.g. 
 * </code>
 * 
 * @author kgopalak
 * 
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
}
