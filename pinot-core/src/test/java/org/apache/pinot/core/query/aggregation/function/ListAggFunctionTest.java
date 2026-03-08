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
package org.apache.pinot.core.query.aggregation.function;

import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.array.ListAggDistinctFunction;
import org.apache.pinot.core.query.aggregation.function.array.ListAggFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ListAggFunctionTest extends AbstractAggregationFunctionTest {

  private static class TestStringSVBlock extends SyntheticBlockValSets.Base {
    private final String[] _values;

    TestStringSVBlock(String[] values) {
      _values = values;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Override
    public String[] getStringValuesSV() {
      return _values;
    }

    @Override
    public FieldSpec.DataType getValueType() {
      return FieldSpec.DataType.STRING;
    }
  }

  private static class TestStringMVBlock extends SyntheticBlockValSets.Base {
    private final String[][] _values;

    TestStringMVBlock(String[][] values) {
      _values = values;
    }

    @Override
    public boolean isSingleValue() {
      return false;
    }

    @Override
    public String[][] getStringValuesMV() {
      return _values;
    }

    @Override
    public FieldSpec.DataType getValueType() {
      return FieldSpec.DataType.STRING;
    }
  }

  @Test
  public void testListAggAggregate() {
    ListAggFunction fn = new ListAggFunction(ExpressionContext.forIdentifier("myField"), ",", false);
    AggregationResultHolder holder = fn.createAggregationResultHolder();
    fn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestStringMVBlock(new String[][]{{"A", "B"}, {"C"}})));
    fn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestStringMVBlock(new String[][]{{"B"}, {"D"}})));
    String result = fn.extractFinalResult(holder.getResult());
    assertEquals(result, "A,B,C,B,D");
  }

  @Test
  public void testListAggAggregateSV() {
    ListAggFunction fn = new ListAggFunction(ExpressionContext.forIdentifier("svField"), "|", false);
    AggregationResultHolder holder = fn.createAggregationResultHolder();
    fn.aggregate(3, holder,
        Map.of(ExpressionContext.forIdentifier("svField"), new TestStringSVBlock(new String[]{"A", "B", "C"})));
    fn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("svField"), new TestStringSVBlock(new String[]{"B", "D"})));
    String result = fn.extractFinalResult(holder.getResult());
    assertEquals(result, "A|B|C|B|D");
  }

  @Test
  public void testListAggDistinctAggregate() {
    ListAggDistinctFunction fn =
        new ListAggDistinctFunction(ExpressionContext.forIdentifier("myField"), ",", false);
    AggregationResultHolder holder = fn.createAggregationResultHolder();
    fn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestStringMVBlock(new String[][]{{"A", "B"}, {"C"}})));
    fn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestStringMVBlock(new String[][]{{"B"}, {"A"}})));
    String result = fn.extractFinalResult(holder.getResult());
    assertEquals(result, "A,B,C");
  }

  @Test
  public void testListAggDistinctAggregateSV() {
    ListAggDistinctFunction fn =
        new ListAggDistinctFunction(ExpressionContext.forIdentifier("svField"), ",", false);
    AggregationResultHolder holder = fn.createAggregationResultHolder();
    fn.aggregate(3, holder,
        Map.of(ExpressionContext.forIdentifier("svField"), new TestStringSVBlock(new String[]{"A", "B", "C"})));
    fn.aggregate(3, holder,
        Map.of(ExpressionContext.forIdentifier("svField"), new TestStringSVBlock(new String[]{"B", "A", "D"})));
    String result = fn.extractFinalResult(holder.getResult());
    assertEquals(result, "A,B,C,D");
  }

  @Test
  public void testGroupByPaths() {
    ListAggFunction fn = new ListAggFunction(ExpressionContext.forIdentifier("myField"), ";", false);
    GroupByResultHolder gb = new ObjectGroupByResultHolder(4, 4);
    fn.aggregateGroupBySV(2, new int[]{0, 1}, gb,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestStringMVBlock(new String[][]{{"X"}, {"Y", "Z"}})));
    assertEquals(fn.extractFinalResult(gb.getResult(0)), "X");
    assertEquals(fn.extractFinalResult(gb.getResult(1)), "Y;Z");
  }

  @Test
  public void testGroupByMVKeysOnMVColumn() {
    ListAggFunction fn = new ListAggFunction(ExpressionContext.forIdentifier("mvField"), ":", false);
    GroupByResultHolder gb = new ObjectGroupByResultHolder(4, 4);
    int[][] groupKeysArray = new int[][]{{0, 1}, {1}};
    fn.aggregateGroupByMV(2, groupKeysArray, gb,
        Map.of(ExpressionContext.forIdentifier("mvField"), new TestStringMVBlock(new String[][]{{"A"}, {"B", "C"}})));
    assertEquals(fn.extractFinalResult(gb.getResult(0)), "A");
    assertEquals(fn.extractFinalResult(gb.getResult(1)), "A:B:C");
  }
}
