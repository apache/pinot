package com.linkedin.thirdeye.anomaly.util;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Iterator for fixed dimension combinations that operates with O(|D|) space complexity.
 */
public class FixedDimensionIterator {

  private final Map<String, List<String>> fixedDimensions;
  private final List<String> dimensionNames;
  private int[] offsets;
  private int position;
  private int numCombinations;

  public FixedDimensionIterator(Map<String, List<String>> fixedDimensions) {
    position = 0;

    this.fixedDimensions = fixedDimensions;
    this.dimensionNames = new ArrayList<>(fixedDimensions.keySet());

    int numDimensions = dimensionNames.size();
    offsets = new int[numDimensions];
    if (numDimensions > 0) {
      numCombinations = 1;
      for (int i = 0; i < numDimensions; i++) {
        String dimension = dimensionNames.get(i);
        int dimensionSize = fixedDimensions.get(dimension).size();
        offsets[i] = numCombinations;
        numCombinations *= dimensionSize;
      }
    } else {
      // edge case where no dimensions are provided
      numCombinations = 0;
    }
  }

  public boolean hasNext() {
    return position < numCombinations;
  }

  public Map<String, String> next() {
    Map<String, String> dimensionValues = new HashMap<String, String>();

    int relativePosition = position;
    for (int i = dimensionNames.size() - 1; i >= 0; i--) {
      String dimension = dimensionNames.get(i);
      int dimensionIndex = relativePosition / offsets[i];

      dimensionValues.put(dimension, fixedDimensions.get(dimension).get(dimensionIndex));

      relativePosition %= offsets[i];
    }

    position++;
    return dimensionValues;
  }

}
