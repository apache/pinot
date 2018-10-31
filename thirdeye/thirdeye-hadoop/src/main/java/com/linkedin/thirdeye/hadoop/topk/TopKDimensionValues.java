/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.thirdeye.hadoop.topk;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Class to create and manage top k values for every dimension
 */
public class TopKDimensionValues {
  private Map<String, Set<String>> topKDimensions;

  public TopKDimensionValues() {
    topKDimensions = new HashMap<>();
  }

  public Map<String, Set<String>> getTopKDimensions() {
    return topKDimensions;
  }

  public void setTopKDimensions(Map<String, Set<String>> topKDimensions) {
    this.topKDimensions = topKDimensions;
  }

  /**
   * Add a top k value for a dimension
   * @param dimension
   * @param value
   */
  public void addValue(String dimension, String value) {
    if (topKDimensions.get(dimension) == null) {
      topKDimensions.put(dimension, new HashSet<String>());
    }
    topKDimensions.get(dimension).add(value);
  }

  public void addAllValues(String dimension, Set<String> values) {
    if (topKDimensions.get(dimension) == null) {
      topKDimensions.put(dimension, new HashSet<String>());
    }
    topKDimensions.get(dimension).addAll(values);
  }

  /**
   * Add all top k values for all dimensions from a TopKDimensionValues object
   * @param valuesFile
   */
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
