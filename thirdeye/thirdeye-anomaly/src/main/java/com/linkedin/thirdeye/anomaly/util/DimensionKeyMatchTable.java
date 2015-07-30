package com.linkedin.thirdeye.anomaly.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;

/**
 * Returns the value stored corresponding with the compatible match with the most matching fields.
 */
public class DimensionKeyMatchTable<V> {

  /** */
  public static final String MATCH_ALL_DIMENSION = "?";

  private final List<DimensionSpec> dimensionSpecs;

  private final Map<DimensionKey, V> valueMap;

  public DimensionKeyMatchTable(List<DimensionSpec> dimensionSpecs) {
    valueMap = new HashMap<>();
    this.dimensionSpecs = dimensionSpecs;
  }

  /**
   * @param key
   * @param value
   */
  public void put(DimensionKey key, V value) {
    valueMap.put(key, value);
  }

  /**
   * @param key
   * @return
   *  The value associated with the best matching dimension key or null if there are no matches.
   */
  public V get(DimensionKey key) {
    int bestMatchDegree = 0;
    DimensionKey bestMatchKey = null;
    for (DimensionKey keyInMap : valueMap.keySet()) {
      int currMatchDegree = getNumDimensionsMatched(keyInMap, key);
      if (currMatchDegree > bestMatchDegree) {
        bestMatchDegree = currMatchDegree;
        bestMatchKey = keyInMap;
      }
    }

    if (bestMatchKey != null) {
      return valueMap.get(bestMatchKey);
    } else {
      return null;
    }
  }

  public int size() {
    return valueMap.size();
  }

  private int getNumDimensionsMatched(DimensionKey patternKey, DimensionKey valueKey) {
    int degreeOfMatch = 0;
    for (DimensionSpec dimensionSpec : dimensionSpecs) {
      String pattern = patternKey.getDimensionValue(dimensionSpecs, dimensionSpec.getName());
      String value = valueKey.getDimensionValue(dimensionSpecs, dimensionSpec.getName());
      if (pattern.equals(value)) {
        // exact or pattern match
        degreeOfMatch++;
      } else if (pattern.equals(MATCH_ALL_DIMENSION)) {
        // wildcard
        continue;
      } else {
        // not a match
        return -1;
      }
    }
    return degreeOfMatch;
  }

}
