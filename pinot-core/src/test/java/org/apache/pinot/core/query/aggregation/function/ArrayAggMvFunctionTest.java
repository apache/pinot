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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggDistinctDoubleFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggDistinctFloatFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggDistinctIntFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggDistinctLongFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggDistinctStringFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggDoubleFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggFloatFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggIntFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggLongFunction;
import org.apache.pinot.core.query.aggregation.function.array.ArrayAggStringFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ArrayAggMvFunctionTest extends AbstractAggregationFunctionTest {

  private static class TestDoubleMVBlock extends SyntheticBlockValSets.Base {
    private final double[][] _values;

    TestDoubleMVBlock(double[][] values) {
      _values = values;
    }

    @Override
    public boolean isSingleValue() {
      return false;
    }

    @Override
    public double[][] getDoubleValuesMV() {
      return _values;
    }

    @Override
    public FieldSpec.DataType getValueType() {
      return FieldSpec.DataType.DOUBLE;
    }
  }

  private static class TestLongMVBlock extends SyntheticBlockValSets.Base {
    private final long[][] _values;

    TestLongMVBlock(long[][] values) {
      _values = values;
    }

    @Override
    public boolean isSingleValue() {
      return false;
    }

    @Override
    public long[][] getLongValuesMV() {
      return _values;
    }

    @Override
    public FieldSpec.DataType getValueType() {
      return FieldSpec.DataType.LONG;
    }
  }

  private static class TestIntMVBlock extends SyntheticBlockValSets.Base {
    private final int[][] _values;

    TestIntMVBlock(int[][] values) {
      _values = values;
    }

    @Override
    public boolean isSingleValue() {
      return false;
    }

    @Override
    public int[][] getIntValuesMV() {
      return _values;
    }

    @Override
    public FieldSpec.DataType getValueType() {
      return FieldSpec.DataType.INT;
    }
  }

  private static class TestFloatMVBlock extends SyntheticBlockValSets.Base {
    private final float[][] _values;

    TestFloatMVBlock(float[][] values) {
      _values = values;
    }

    @Override
    public boolean isSingleValue() {
      return false;
    }

    @Override
    public float[][] getFloatValuesMV() {
      return _values;
    }

    @Override
    public FieldSpec.DataType getValueType() {
      return FieldSpec.DataType.FLOAT;
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
  public void testDoubleArrayAggMvMultipleBlocks() {
    ArrayAggDistinctDoubleFunction distinctFn =
        new ArrayAggDistinctDoubleFunction(ExpressionContext.forIdentifier("myField"), false);
    AggregationResultHolder holder = distinctFn.createAggregationResultHolder();

    distinctFn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestDoubleMVBlock(new double[][]{{1.0, 2.0}, {2.0}})));
    distinctFn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestDoubleMVBlock(new double[][]{{2.0, 3.0}, {3.0}})));
    DoubleOpenHashSet distinct = holder.getResult();
    assertEquals(distinct.size(), 3);

    ArrayAggDoubleFunction fn = new ArrayAggDoubleFunction(ExpressionContext.forIdentifier("myField"), false);
    holder = fn.createAggregationResultHolder();
    fn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestDoubleMVBlock(new double[][]{{1.0, 2.0}, {2.0}})));
    fn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestDoubleMVBlock(new double[][]{{2.0, 3.0}, {3.0}})));
    DoubleArrayList result = holder.getResult();
    assertEquals(result.size(), 6);

    // round-trip ser/de
    AggregationFunction.SerializedIntermediateResult ser = fn.serializeIntermediateResult(result);
    DoubleArrayList deser = ObjectSerDeUtils.deserialize(ser.getBytes(), ObjectSerDeUtils.ObjectType.DoubleArrayList);
    assertEquals(deser.size(), 6);
  }

  @Test
  public void testLongArrayAggMvMultipleBlocks() {
    ArrayAggDistinctLongFunction distinctFn = new ArrayAggDistinctLongFunction(
        ExpressionContext.forIdentifier("myField"), FieldSpec.DataType.LONG, false);
    AggregationResultHolder holder = distinctFn.createAggregationResultHolder();

    distinctFn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestLongMVBlock(new long[][]{{1L, 2L}, {2L}})));
    distinctFn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestLongMVBlock(new long[][]{{2L, 3L}, {3L}})));
    LongOpenHashSet distinct = holder.getResult();
    assertEquals(distinct.size(), 3);

    ArrayAggLongFunction fn = new ArrayAggLongFunction(ExpressionContext.forIdentifier("myField"),
        FieldSpec.DataType.LONG, false);
    holder = fn.createAggregationResultHolder();
    fn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestLongMVBlock(new long[][]{{1L, 2L}, {2L}})));
    fn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestLongMVBlock(new long[][]{{2L, 3L}, {3L}})));
    LongArrayList result = holder.getResult();
    assertEquals(result.size(), 6);

    // group-by path sanity
    GroupByResultHolder gbHolder = new ObjectGroupByResultHolder(4, 4);
    fn.aggregateGroupBySV(2, new int[]{0, 1}, gbHolder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestLongMVBlock(new long[][]{{5L}, {6L, 7L}})));
    assertEquals(((LongArrayList) gbHolder.getResult(0)).size(), 1);
    assertEquals(((LongArrayList) gbHolder.getResult(1)).size(), 2);
  }

  @Test
  public void testIntArrayAggMvMultipleBlocks() {
    ArrayAggDistinctIntFunction distinctFn = new ArrayAggDistinctIntFunction(
        ExpressionContext.forIdentifier("myField"), FieldSpec.DataType.INT, false);
    AggregationResultHolder holder = distinctFn.createAggregationResultHolder();

    distinctFn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestIntMVBlock(new int[][]{{1, 2}, {2}})));
    distinctFn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestIntMVBlock(new int[][]{{2, 3}, {3}})));
    IntOpenHashSet distinct = holder.getResult();
    assertEquals(distinct.size(), 3);

    ArrayAggIntFunction fn = new ArrayAggIntFunction(ExpressionContext.forIdentifier("myField"),
        FieldSpec.DataType.INT, false);
    holder = fn.createAggregationResultHolder();
    fn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestIntMVBlock(new int[][]{{1, 2}, {2}})));
    fn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestIntMVBlock(new int[][]{{2, 3}, {3}})));
    IntArrayList result = holder.getResult();
    assertEquals(result.size(), 6);
  }

  @Test
  public void testFloatArrayAggMvMultipleBlocks() {
    ArrayAggDistinctFloatFunction distinctFn =
        new ArrayAggDistinctFloatFunction(ExpressionContext.forIdentifier("myField"), false);
    AggregationResultHolder holder = distinctFn.createAggregationResultHolder();

    distinctFn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestFloatMVBlock(new float[][]{{1.0f, 2.0f}, {2.0f}})));
    distinctFn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestFloatMVBlock(new float[][]{{2.0f, 3.0f}, {3.0f}})));
    FloatOpenHashSet distinct = holder.getResult();
    assertEquals(distinct.size(), 3);

    ArrayAggFloatFunction fn = new ArrayAggFloatFunction(ExpressionContext.forIdentifier("myField"), false);
    holder = fn.createAggregationResultHolder();
    fn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestFloatMVBlock(new float[][]{{1.0f, 2.0f}, {2.0f}})));
    fn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestFloatMVBlock(new float[][]{{2.0f, 3.0f}, {3.0f}})));
    FloatArrayList result = holder.getResult();
    assertEquals(result.size(), 6);
  }

  @Test
  public void testStringArrayAggMvMultipleBlocks() {
    ArrayAggDistinctStringFunction distinctFn =
        new ArrayAggDistinctStringFunction(ExpressionContext.forIdentifier("myField"), false);
    AggregationResultHolder holder = distinctFn.createAggregationResultHolder();

    distinctFn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestStringMVBlock(new String[][]{{"A", "B"}, {"B"}})));
    distinctFn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestStringMVBlock(new String[][]{{"B", "C"}, {"C"}})));
    ObjectOpenHashSet<String> distinct = holder.getResult();
    assertEquals(distinct.size(), 3);

    ArrayAggStringFunction fn = new ArrayAggStringFunction(ExpressionContext.forIdentifier("myField"), false);
    holder = fn.createAggregationResultHolder();
    fn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestStringMVBlock(new String[][]{{"A", "B"}, {"B"}})));
    fn.aggregate(2, holder,
        Map.of(ExpressionContext.forIdentifier("myField"), new TestStringMVBlock(new String[][]{{"B", "C"}, {"C"}})));
    ObjectArrayList<String> result = holder.getResult();
    assertEquals(result.size(), 6);
  }
}
