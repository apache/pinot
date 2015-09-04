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
import com.linkedin.pinot.common.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;

public class TestStarTreeBuilder {
  private List<Number> metrics;
  private List<FieldSpec.DataType> dimensionTypes;
  private List<FieldSpec.DataType> metricTypes;

  @BeforeClass
  public void beforeClass() {
    metrics = new ArrayList<Number>();
    metrics.add(1);
    dimensionTypes = ImmutableList.of(FieldSpec.DataType.INT, FieldSpec.DataType.INT, FieldSpec.DataType.INT);
    metricTypes = ImmutableList.of(FieldSpec.DataType.LONG);
  }

  @DataProvider
  public Object[][] starTreeBuilderDataProvider() {
    return new Object[][] {
        // Basic
        { createDefaultBuilder(ImmutableList.of(0, 1, 2), 4,
            new LinkedListStarTreeTable(dimensionTypes, metricTypes)) },
        // Reverse order
        { createDefaultBuilder(ImmutableList.of(2, 1, 0), 4,
            new LinkedListStarTreeTable(dimensionTypes, metricTypes)) },
        // A larger max leaf records
        { createDefaultBuilder(ImmutableList.of(2, 1, 0), 16,
            new LinkedListStarTreeTable(dimensionTypes, metricTypes)) }
    };
  }

  private StarTreeBuilder createDefaultBuilder(
      List<Integer> splitOrder,
      int maxLeafRecords,
      StarTreeTable table) {
    StarTreeBuilder builder = new DefaultStarTreeBuilder();
    builder.init(splitOrder, maxLeafRecords, table);
    List<List<Integer>> combinations = generateCombinations();
    for (List<Integer> combination : combinations) {
      builder.append(new StarTreeTableRow(combination, metrics));
    }
    builder.build();
    return builder;
  }

  @Test(dataProvider = "starTreeBuilderDataProvider")
  public void testTotalRawDocumentCount(StarTreeBuilder builder) {
    Assert.assertEquals(builder.getTotalRawDocumentCount(), 128);
  }

  @Test(dataProvider = "starTreeBuilderDataProvider")
  public void testAggregateDocumentCount(StarTreeBuilder builder) {
    Set<StarTreeIndexNode> set = new HashSet<StarTreeIndexNode>();
    collectAggregateLeafNodes(builder.getTree(), set);

    int totalAggregateDocumentCount = 0;
    for (StarTreeIndexNode node : set) {
      StarTreeTableRange range = builder.getDocumentIdRange(node.getNodeId());
      totalAggregateDocumentCount += range.getDocumentCount();
    }

    Assert.assertEquals(builder.getTotalAggregateDocumentCount(), totalAggregateDocumentCount);
  }

  @Test(dataProvider = "starTreeBuilderDataProvider")
  public void testTableConstraints(StarTreeBuilder builder) {
    checkSubTable(builder, builder.getTree());
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

  private void collectAggregateLeafNodes(StarTreeIndexNode node, Set<StarTreeIndexNode> set) {
    if (node.isLeaf()) {
      if (node.getPathValues().containsValue(StarTreeIndexNode.all())) {
        set.add(node);
      }
    } else {
      for (StarTreeIndexNode child : node.getChildren().values()) {
        collectAggregateLeafNodes(child, set);
      }
    }
  }

  private void checkSubTable(StarTreeBuilder builder, StarTreeIndexNode node) {
    if (node.isLeaf()) {
      StarTreeTableRange range = builder.getDocumentIdRange(node.getNodeId());
      StarTreeTable subTable = builder.getTable().view(range.getStartDocumentId(), range.getDocumentCount());

      // Get sorted sub table according to path dimensions
      List<List<Integer>> sortedValues = new ArrayList<List<Integer>>();
      Iterator<StarTreeTableRow> itr = subTable.getAllCombinations();
      while (itr.hasNext()) {
        List<Integer> copy = new ArrayList<>(itr.next().getDimensions());
        sortedValues.add(copy);
      }
      final List<Integer> pathDimensions = node.getPathDimensions();
      Collections.sort(sortedValues, new Comparator<List<Integer>>() {
        @Override
        public int compare(List<Integer> o1, List<Integer> o2) {
          for (Integer dimension : pathDimensions) {
            Integer v1 = o1.get(dimension);
            Integer v2 = o2.get(dimension);
            if (!v1.equals(v2)) {
              return v1.compareTo(v2);
            }
          }
          return 0;
        }
      });

      // Iterate again and ensure that all the values are same
      itr = subTable.getAllCombinations();
      int idx = 0;
      while (itr.hasNext()) {
        Assert.assertEquals(itr.next().getDimensions(), sortedValues.get(idx++));
      }

      // Check that all values have the prefix
      Map<Integer, Integer> pathValues = node.getPathValues();
      itr = subTable.getAllCombinations();
      while (itr.hasNext()) {
        List<Integer> next = itr.next().getDimensions();
        for (Map.Entry<Integer, Integer> entry : pathValues.entrySet()) {
          Assert.assertEquals(next.get(entry.getKey()), entry.getValue());
        }
      }

      // Ensure there are no more than max leaf records if not fully split
      if (pathDimensions.size() < builder.getSplitOrder().size()) {
        Assert.assertTrue(sortedValues.size() <= builder.getMaxLeafRecords(),
            sortedValues.size() + " is > " + builder.getMaxLeafRecords());
      }
    } else {
      for (StarTreeIndexNode child : node.getChildren().values()) {
        checkSubTable(builder, child);
      }
    }
  }
}
