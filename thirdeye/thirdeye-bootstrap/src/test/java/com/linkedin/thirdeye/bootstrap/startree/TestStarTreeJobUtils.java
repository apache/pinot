package com.linkedin.thirdeye.bootstrap.startree;

import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.DimensionKey;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestStarTreeJobUtils {
  @Test
  public void testFindBestMatch() throws Exception {
    List<String> dimensions = Arrays.asList("A", "B", "C");

    // Forward index
    Map<String, Map<String, Integer>> forwardIndex = new HashMap<String, Map<String, Integer>>();
    for (String dimension : dimensions) {
      forwardIndex.put(dimension, new HashMap<String, Integer>());
      forwardIndex.get(dimension).put(StarTreeConstants.STAR, StarTreeConstants.STAR_VALUE);
      forwardIndex.get(dimension).put(StarTreeConstants.OTHER, StarTreeConstants.OTHER_VALUE);
      forwardIndex.get(dimension).put(dimension + "X", StarTreeConstants.FIRST_VALUE); // will not
                                                                                       // be in
                                                                                       // combos
      for (int i = 0; i < 3; i++) {
        forwardIndex.get(dimension).put(dimension + i, StarTreeConstants.FIRST_VALUE + 1 + i);
      }
    }

    // Some combinations
    List<int[]> combinations = new ArrayList<int[]>();
    combinations.add(getCombination(forwardIndex, dimensions, Arrays.asList("A0", "B0", "C0")));
    combinations.add(getCombination(forwardIndex, dimensions, Arrays.asList("A1", "B1", "C1")));
    combinations.add(getCombination(forwardIndex, dimensions, Arrays.asList("A0", "B2", "C0")));
    combinations.add(getCombination(forwardIndex, dimensions, Arrays.asList("?", "B0", "?")));
    combinations.add(getCombination(forwardIndex, dimensions, Arrays.asList("?", "B2", "C0")));
    combinations.add(getCombination(forwardIndex, dimensions, Arrays.asList("?", "?", "?")));

    // Target matches
    int[] allOther = new int[] {
        StarTreeConstants.OTHER_VALUE, StarTreeConstants.OTHER_VALUE, StarTreeConstants.OTHER_VALUE
    };
    int[] oneOther = new int[] {
        StarTreeConstants.OTHER_VALUE, forwardIndex.get("B").get("B2"),
        forwardIndex.get("C").get("C0")
    };
    int[] twoOther = new int[] {
        StarTreeConstants.OTHER_VALUE, forwardIndex.get("B").get("B0"),
        StarTreeConstants.OTHER_VALUE
    };

    // No match, so should be all other
    int[] res = StarTreeJobUtils.findBestMatch(new DimensionKey(new String[] {
        "AX", "BX", "CX"
    }), dimensions, combinations, forwardIndex);
    Assert.assertEquals(res, allOther);

    // Should be one other
    res = StarTreeJobUtils.findBestMatch(new DimensionKey(new String[] {
        "AX", "B2", "C0"
    }), dimensions, combinations, forwardIndex);
    Assert.assertEquals(res, oneOther);

    // Should be two other
    res = StarTreeJobUtils.findBestMatch(new DimensionKey(new String[] {
        "AX", "B0", "C1"
    }), dimensions, combinations, forwardIndex);
    Assert.assertEquals(res, twoOther);
  }

  private int[] getCombination(Map<String, Map<String, Integer>> forwardIndex,
      List<String> dimensions, List<String> combination) {
    int[] intCombination = new int[combination.size()];

    for (int i = 0; i < combination.size(); i++) {
      intCombination[i] = forwardIndex.get(dimensions.get(i)).get(combination.get(i));
    }

    return intCombination;
  }
}
