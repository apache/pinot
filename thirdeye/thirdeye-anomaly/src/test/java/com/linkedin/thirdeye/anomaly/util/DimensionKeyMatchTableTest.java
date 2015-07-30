package com.linkedin.thirdeye.anomaly.util;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;

/**
 *
 */
public class DimensionKeyMatchTableTest {

  @Test
  public void testInsertionAndLookUp() {
    List<DimensionSpec> dimensionSpecs = new ArrayList<>();
    dimensionSpecs.add(new DimensionSpec("A"));
    dimensionSpecs.add(new DimensionSpec("B"));

    DimensionKeyMatchTable<Integer> matchTable = new DimensionKeyMatchTable<>(dimensionSpecs);
    matchTable.put(new DimensionKey(new String[]{"*","*"}), 1);
    matchTable.put(new DimensionKey(new String[]{"?","*"}), 2);
    matchTable.put(new DimensionKey(new String[]{"*","?"}), 3);

    Assert.assertTrue(matchTable.get(new DimensionKey(new String[]{"*","*"})) == 1);
    Assert.assertTrue(matchTable.get(new DimensionKey(new String[]{"a","*"})) == 2);
    Assert.assertTrue(matchTable.get(new DimensionKey(new String[]{"*","b"})) == 3);
    Assert.assertTrue(matchTable.get(new DimensionKey(new String[]{"z","z"})) == null);
  }

}
