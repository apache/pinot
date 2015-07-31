/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.startree;

import com.google.common.collect.ImmutableList;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;

public class TestLinkedListStarTreeTable {
  private List<Number> metrics;

  @BeforeClass
  public void beforeClass() {
    metrics = new ArrayList<Number>();
    metrics.add(1);
  }

  @DataProvider
  public Object[][] uniqueCombinationExcludesDataProvider() {
    return new Object[][] {
        { null },
        { ImmutableList.of(1) },
        { ImmutableList.of(0, 2) },
        { ImmutableList.of(0, 1, 2) },
    };
  }

  @Test(dataProvider = "uniqueCombinationExcludesDataProvider")
  public void testGetUniqueCombinations(List<Integer> excludedDimensions) {
    StarTreeTable table = new LinkedListStarTreeTable();

    Set<List<Integer>> uniqueCombinations = new HashSet<List<Integer>>();

    for (List<Integer> combination : generateCombinations()) {
      table.append(new StarTreeTableRow(combination, metrics));

      // Add unique one excluding dimensions
      List<Integer> withExclusions = new ArrayList<Integer>(combination);
      for (int i = 0; i < withExclusions.size(); i++) {
        if (excludedDimensions != null && excludedDimensions.contains(i)) {
          withExclusions.set(i, StarTreeIndexNode.all());
        }
      }
      uniqueCombinations.add(withExclusions);
    }

    Iterator<StarTreeTableRow> itr = table.getUniqueCombinations(excludedDimensions);
    int actualCombinations = 0;
    while (itr.hasNext()) {
      itr.next();
      actualCombinations++;
    }

    Assert.assertEquals(actualCombinations, uniqueCombinations.size());
  }

  @DataProvider
  public Object[][] groupByDataProvider() {
    return new Object[][] {
        { 0 },
        { 1 },
        { 2 }
    };
  }

  @Test(dataProvider = "groupByDataProvider")
  public void testGroupBy(int groupByDimension) {
    StarTreeTable table = new LinkedListStarTreeTable();

    Map<Integer, Integer> rawCounts = new HashMap<Integer, Integer>();
    Map<Integer, Set<List<Integer>>> uniqueCombinations = new HashMap<Integer, Set<List<Integer>>>();
    for (List<Integer> combination : generateCombinations()) {
      Integer value = combination.get(groupByDimension);

      table.append(new StarTreeTableRow(combination, metrics));

      // Combinations with this value
      Set<List<Integer>> combinationSet = uniqueCombinations.get(value);
      if (combinationSet == null) {
        combinationSet = new HashSet<List<Integer>>();
        uniqueCombinations.put(value, combinationSet);
      }
      combinationSet.add(combination);

      // Raw records with this value
      Integer count = rawCounts.get(value);
      if (count == null) {
        count = 0;
      }
      rawCounts.put(value, count + 1);
    }

    StarTreeTableGroupByStats result = table.groupBy(groupByDimension);
    Assert.assertEquals(result.getValues(), rawCounts.keySet());
    for (Map.Entry<Integer, Integer> entry : rawCounts.entrySet()) {
      Integer value = entry.getKey();
      Integer rawCount = entry.getValue();
      Integer uniqueCount = uniqueCombinations.get(value).size();
      Assert.assertEquals(result.getRawCount(value), rawCount);
      Assert.assertEquals(result.getUniqueCount(value), uniqueCount);
    }
  }

  @Test
  public void testGroupByWithSort() {
    StarTreeTable table = new LinkedListStarTreeTable();
    for (List<Integer> combination : generateCombinations()) {
      table.append(new StarTreeTableRow(combination, metrics));
    }

    // Sort based on 0
    table.sort(ImmutableList.of(0));
    StarTreeTableGroupByStats result = table.groupBy(0);
    Assert.assertEquals(result.getMinRecordId(0), Integer.valueOf(0));
    Assert.assertEquals(result.getMinRecordId(1), Integer.valueOf(64));

    // Sort based on dimensions 0, 1 -> [0, 0], [0, 2], [1, 1], [1, 3] prefix values
    table.sort(ImmutableList.of(0, 1));
    Iterator<StarTreeTableRow> itr = table.getAllCombinations();
    result = table.groupBy(1);
    Assert.assertEquals(result.getMinRecordId(0), Integer.valueOf(0));
    Assert.assertEquals(result.getMinRecordId(2), Integer.valueOf(32));
    Assert.assertEquals(result.getMinRecordId(1), Integer.valueOf(64));
    Assert.assertEquals(result.getMinRecordId(3), Integer.valueOf(96));
  }

  @Test
  public void testView() {
    StarTreeTable table = new LinkedListStarTreeTable();
    for (List<Integer> combination : generateCombinations()) {
      table.append(new StarTreeTableRow(combination, metrics));
    }

    // Sort based on 0, 1
    table.sort(ImmutableList.of(0, 1));

    // Get a view up until 64
    StarTreeTable view = table.view(0, 64);
    Assert.assertEquals(view.size(), 64);

    // All values of dimension 0 should be 0
    Iterator<StarTreeTableRow> itr = view.getAllCombinations();
    while (itr.hasNext()) {
      List<Integer> row = itr.next().getDimensions();
      Assert.assertEquals(row.get(0), Integer.valueOf(0));
    }

    // Get a view from 64 to end
    view = table.view(64, 64);
    Assert.assertEquals(view.size(), 64);

    // All values of dimension 0 should be 1
    itr = view.getAllCombinations();
    while (itr.hasNext()) {
      List<Integer> row = itr.next().getDimensions();
      Assert.assertEquals(row.get(0), Integer.valueOf(1));
    }

    // Get a sub-view of that view
    view = view.view(0, 32);
    Assert.assertEquals(view.size(), 32);

    // All values of dimension 1 should be 1
    itr = view.getAllCombinations();
    while (itr.hasNext()) {
      List<Integer> row = itr.next().getDimensions();
      Assert.assertEquals(row.get(1), Integer.valueOf(1));
    }
  }

  @Test
  public void testAggregation() {
    StarTreeTable table = new LinkedListStarTreeTable();
    for (List<Integer> combination : generateCombinations()) {
      table.append(new StarTreeTableRow(combination, metrics));
    }

    // Across all dimensions
    Iterator<StarTreeTableRow> itr = table.getUniqueCombinations(Arrays.asList(0, 1, 2));
    StarTreeTableRow row = itr.next();
    Assert.assertFalse(itr.hasNext());
    Assert.assertEquals(row.getMetrics().get(0).intValue(), 128);

    // Across one dimension
    itr = table.getUniqueCombinations(Arrays.asList(1, 2));
    for (int i = 0; i < 2; i++) {
      row = itr.next();
      Assert.assertEquals(row.getMetrics().get(0).intValue(), 64);
    }
    Assert.assertFalse(itr.hasNext());
  }

  private List<List<Integer>> generateCombinations() {
    List<List<Integer>> list = new LinkedList<List<Integer>>();
    for (int i = 0; i < 128; i++) {
      List<Integer> combination = ImmutableList.of(
          i % 2,
          i % 4,
          i % 8
      );
      list.add(combination);
    }
    return list;
  }
}
