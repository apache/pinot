/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.broker.routing.intervalst;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.pinot.broker.routing.segmentpruner.interval.Interval;
import org.apache.pinot.broker.routing.segmentpruner.interval.IntervalTree;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IntervalTreeTest {

  @Test
  public void testIntervalTree() {
    Interval interval0 = new Interval(0, 1);
    Interval interval1 = new Interval(2, 18);
    Interval interval2 = new Interval(5, 7);
    Interval interval3 = new Interval(5, 7);
    Interval interval4 = new Interval(5, 15);
    Interval interval5 = new Interval(7, 9);
    Interval interval6 = new Interval(12, 14);
    Interval interval7 = new Interval(17, 17);
    Interval interval8 = new Interval(17, 23);
    Interval interval9 = new Interval(17, 23);
    Interval interval10 = new Interval(17, 23);
    Interval interval11 = new Interval(20, 30);
    Interval interval12 = new Interval(25, 28);

    String name0 = interval0.toString() + ": 0";
    String name1 = interval1.toString() + ": 1";
    String name2 = interval2.toString() + ": 2";
    String name3 = interval3.toString() + ": 3";
    String name4 = interval4.toString() + ": 4";
    String name5 = interval5.toString() + ": 5";
    String name6 = interval6.toString() + ": 6";
    String name7 = interval7.toString() + ": 7";
    String name8 = interval8.toString() + ": 8";
    String name9 = interval9.toString() + ": 9";
    String name10 = interval10.toString() + ": 10";
    String name11 = interval11.toString() + ": 11";
    String name12 = interval12.toString() + ": 12";

    Map<String, Interval> nameToIntervalMap = new HashMap<>();
    nameToIntervalMap.put(name0, interval0);
    nameToIntervalMap.put(name1, interval1);
    nameToIntervalMap.put(name2, interval2);
    nameToIntervalMap.put(name3, interval3);
    nameToIntervalMap.put(name4, interval4);
    nameToIntervalMap.put(name5, interval5);

    nameToIntervalMap.put(name6, interval6);
    nameToIntervalMap.put(name7, interval7);
    nameToIntervalMap.put(name8, interval8);
    nameToIntervalMap.put(name9, interval9);
    nameToIntervalMap.put(name10, interval10);
    nameToIntervalMap.put(name11, interval11);
    nameToIntervalMap.put(name12, interval12);

    IntervalTree<String> intervalTree = new IntervalTree<>(nameToIntervalMap);
    Assert.assertEquals(intervalTree.searchAll(new Interval(40, 40)), List.of());
    Assert.assertEquals(new HashSet<>(intervalTree.searchAll(new Interval(0, 10))),
        new HashSet<>(Arrays.asList(name0, name1, name2, name3, name4, name5)));
    Assert.assertEquals(new HashSet<>(intervalTree.searchAll(new Interval(10, 20))),
        new HashSet<>(Arrays.asList(name1, name4, name6, name7, name8, name9, name10, name11)));
    Assert.assertEquals(new HashSet<>(intervalTree.searchAll(new Interval(20, 30))),
        new HashSet<>(Arrays.asList(name8, name9, name10, name11, name12)));
  }

  @Test
  public void testEmptyTree() {
    IntervalTree<String> intervalTree = new IntervalTree<>(Map.of());
    Assert.assertEquals(intervalTree.searchAll(new Interval(0, Long.MAX_VALUE)), List.of());
    Assert.assertEquals(intervalTree.searchAll(null), List.of());
  }

  @Test
  public void testNullSearchInterval() {
    IntervalTree<String> intervalTree = new IntervalTree<>(Map.of("value", new Interval(5, 10)));
    Assert.assertEquals(intervalTree.searchAll(null), List.of());
  }

  @Test
  public void testAllValuesOnOneInterval() {
    Map<String, Interval> valueToIntervalMap = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      valueToIntervalMap.put("value" + i, new Interval(5, 10));
    }
    IntervalTree<String> intervalTree = new IntervalTree<>(valueToIntervalMap);
    Assert.assertEquals(new HashSet<>(intervalTree.searchAll(new Interval(10, 20))), valueToIntervalMap.keySet());
    Assert.assertEquals(intervalTree.searchAll(new Interval(11, 20)), List.of());
  }

  /// Cross-checks the tree against a brute force scan over randomly generated intervals. The tight value ranges make
  /// many values share an interval, and the `[0, Long.MAX_VALUE]` entry mirrors the interval
  /// [org.apache.pinot.broker.routing.segmentpruner.TimeSegmentPruner] gives a segment with no time range.
  @Test
  public void testAgainstBruteForceSearch() {
    Random random = new Random(0);
    for (int numValues : new int[]{1, 2, 3, 5, 13, 64, 500}) {
      for (int valueRange : new int[]{1, 2, 5, 40, 1000}) {
        Map<String, Interval> valueToIntervalMap = new HashMap<>();
        for (int i = 0; i < numValues; i++) {
          long min = random.nextInt(valueRange);
          valueToIntervalMap.put("value" + i, new Interval(min, min + random.nextInt(valueRange)));
        }
        valueToIntervalMap.put("noTimeRange", new Interval(0, Long.MAX_VALUE));

        IntervalTree<String> intervalTree = new IntervalTree<>(valueToIntervalMap);
        for (int i = 0; i < 100; i++) {
          long min = random.nextInt(valueRange + 2);
          Interval searchInterval = new Interval(min, min + random.nextInt(valueRange + 2));
          Set<String> expected = new HashSet<>();
          for (Map.Entry<String, Interval> entry : valueToIntervalMap.entrySet()) {
            if (entry.getValue().intersects(searchInterval)) {
              expected.add(entry.getKey());
            }
          }
          List<String> actual = intervalTree.searchAll(searchInterval);
          Assert.assertEquals(new HashSet<>(actual), expected, "Mismatch for search interval: " + searchInterval);
          Assert.assertEquals(actual.size(), expected.size(), "Duplicate values for search interval: "
              + searchInterval);
        }
      }
    }
  }
}
