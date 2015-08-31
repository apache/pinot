package com.linkedin.thirdeye.anomaly.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class FixedDimensionIteratorTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(FixedDimensionIteratorTest.class);

  @Test
  public void combinationsTest() throws Exception {
    Map<String, List<String>> fixedDimensions = new HashMap<>();
    fixedDimensions.put("apples", new ArrayList<String>(Arrays.asList("a","b","c","d")));
    fixedDimensions.put("oranges", new ArrayList<String>(Arrays.asList("x","y","z")));
    fixedDimensions.put("pears", new ArrayList<String>(Arrays.asList("1","2")));
    fixedDimensions.put("berries", new ArrayList<String>(Arrays.asList("*","%","&","(",")")));

    int expectedCount = 1;
    for (List<String> value : fixedDimensions.values()) {
      expectedCount *= value.size();
    }

    FixedDimensionIterator it = new FixedDimensionIterator(fixedDimensions);
    Set<String> seen = new HashSet<>();
    int count = 0;
    while (it.hasNext()) {
      // make it sorted
      TreeMap<String, String> curr = new TreeMap<String, String>(it.next());

      StringBuilder sb = new StringBuilder();
      sb.append("[");
      for (Entry<String, String> kv : curr.entrySet()) {
        sb.append(kv.getKey() + "=" + kv.getValue() + ",");
      }
      sb.append("]");
      LOGGER.info(sb.toString());
      Assert.assertFalse(seen.contains(sb.toString()));
      count++;
    }
    Assert.assertEquals(expectedCount, count);
  }

  @Test
  public void emptyIterator() throws Exception {
    Map<String, List<String>> fixedDimensions = new HashMap<>();
    FixedDimensionIterator it = new FixedDimensionIterator(fixedDimensions);
    Assert.assertFalse(it.hasNext());
  }

  @Test
  public void stressTest() throws Exception {
    int numEntries = 5;
    List<String> bigList = new ArrayList<>(numEntries);
    for (int i = 0; i < numEntries; i++) {
      bigList.add("VALUE " + i);
    }

    Map<String, List<String>> fixedDimensions = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      fixedDimensions.put("DIMENSION " + i, new ArrayList<>(bigList));
    }

    ObjectMapper mapper = new ObjectMapper();
    FixedDimensionIterator it = new FixedDimensionIterator(fixedDimensions);
    while (it.hasNext()) {
      LOGGER.info(mapper.writeValueAsString(it.next()));
    }
  }
}
