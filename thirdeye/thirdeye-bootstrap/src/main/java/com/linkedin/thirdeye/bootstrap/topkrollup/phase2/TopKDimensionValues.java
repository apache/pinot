package com.linkedin.thirdeye.bootstrap.topkrollup.phase2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TopKDimensionValues {
  private Map<String, Set<String>> topKDimensions;

  public TopKDimensionValues() {
    topKDimensions = new HashMap<String, Set<String>>();

  }

  public Map<String, Set<String>> getTopKDimensions() {
    return topKDimensions;
  }

  public void setTopKDimensions(Map<String, Set<String>> topKDimensions) {
    this.topKDimensions = topKDimensions;
  }

  public void addValue(String dimension, String value) {
    if (topKDimensions.get(dimension) == null) {
      topKDimensions.put(dimension,  new HashSet<String>());
    }
    topKDimensions.get(dimension).add(value);
  }

  public void addMap(TopKDimensionValues valuesFile) {
    Map<String, Set<String>> values = valuesFile.getTopKDimensions();
    for (Entry<String, Set<String>> entry : values.entrySet()) {
      if (topKDimensions.get(entry.getKey()) == null) {
        topKDimensions.put(entry.getKey(), new HashSet<String>());
      }
      topKDimensions.get(entry.getKey()).addAll(entry.getValue());
    }
  }


}
