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
package org.apache.pinot.core.query.distinct;

import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.distinct.dictionary.DictionaryBasedMultiColumnDistinctExecutor;
import org.apache.pinot.core.query.distinct.raw.IntDistinctExecutor;
import org.apache.pinot.core.query.distinct.raw.RawMultiColumnDistinctExecutor;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class DistinctExecutorEarlyTerminationTest {

  @Test
  public void testRawSingleColumnExecutorStopsWithinBlock() {
    ExpressionContext expression = ExpressionContext.forIdentifier("c1");
    IntDistinctExecutor executor =
        new IntDistinctExecutor(expression, DataType.INT, /*limit*/ 10, false, null);
    executor.setMaxRowsToProcess(3);
    int[] values = new int[]{10, 11, 12, 13, 14};
    ValueBlock block = new SimpleValueBlock(5, Map.of(expression, new IntBlockValSet(values)));

    executor.process(block);

    assertEquals(executor.getRemainingRowsToProcess(), 0, "row budget should be exhausted");
    assertEquals(executor.getNumDistinctRowsCollected(), 3, "should only read up to the row budget");
  }

  @Test
  public void testRawMultiColumnExecutorStopsWithinBlock() {
    ExpressionContext col1 = ExpressionContext.forIdentifier("c1");
    ExpressionContext col2 = ExpressionContext.forIdentifier("c2");
    List<ExpressionContext> expressions = List.of(col1, col2);
    DataSchema schema = new DataSchema(new String[]{"c1", "c2"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT});
    RawMultiColumnDistinctExecutor executor =
        new RawMultiColumnDistinctExecutor(expressions, false, schema, 10, false, null);
    executor.setMaxRowsToProcess(2);

    Map<ExpressionContext, BlockValSet> blockValSets = Map.of(
        col1, new IntBlockValSet(new int[]{0, 1, 2, 3, 4}),
        col2, new IntBlockValSet(new int[]{10, 11, 12, 13, 14})
    );
    ValueBlock block = new SimpleValueBlock(5, blockValSets);

    executor.process(block);
    DistinctTable result = executor.getResult();

    assertEquals(executor.getRemainingRowsToProcess(), 0);
    assertEquals(result.size(), 2);
  }

  @Test
  public void testDictionaryBasedMultiColumnExecutorStopsWithinBlock() {
    ExpressionContext col1 = ExpressionContext.forIdentifier("c1");
    ExpressionContext col2 = ExpressionContext.forIdentifier("c2");
    List<ExpressionContext> expressions = List.of(col1, col2);
    DataSchema schema = new DataSchema(new String[]{"c1", "c2"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT});
    Dictionary dictionary = new SimpleIntDictionary(new int[]{0, 1, 2, 3, 4});
    DictionaryBasedMultiColumnDistinctExecutor executor =
        new DictionaryBasedMultiColumnDistinctExecutor(expressions, false, schema,
            List.of(dictionary, dictionary), 10, false, null);
    executor.setMaxRowsToProcess(4);

    Map<ExpressionContext, BlockValSet> blockValSets = Map.of(
        col1, new DictionaryBlockValSet(new int[]{0, 1, 2, 3, 4}),
        col2, new DictionaryBlockValSet(new int[]{4, 3, 2, 1, 0})
    );
    ValueBlock block = new SimpleValueBlock(5, blockValSets);

    executor.process(block);
    DistinctTable result = executor.getResult();

    assertEquals(executor.getRemainingRowsToProcess(), 0);
    assertEquals(result.size(), 4);
  }

  @Test
  public void testRawSingleColumnExecutorStopsAfterNoChangeWithinBlock() {
    ExpressionContext expression = ExpressionContext.forIdentifier("c1");
    IntDistinctExecutor executor =
        new IntDistinctExecutor(expression, DataType.INT, /*limit*/ 10, false, null);
    executor.setNumRowsWithoutChangeInDistinct(2);
    int[] values = new int[]{10, 10, 10, 11};
    ValueBlock block = new SimpleValueBlock(4, Map.of(expression, new IntBlockValSet(values)));

    boolean satisfied = executor.process(block);

    assertTrue(satisfied, "should terminate when no-change budget is hit within a block");
    assertTrue(executor.isNumRowsWithoutChangeLimitReached());
    assertEquals(executor.getNumRowsProcessed(), 3);
    assertEquals(executor.getNumDistinctRowsCollected(), 1);
  }

  @Test
  public void testRawMultiColumnExecutorStopsAfterNoChangeWithinBlock() {
    ExpressionContext col1 = ExpressionContext.forIdentifier("c1");
    ExpressionContext col2 = ExpressionContext.forIdentifier("c2");
    List<ExpressionContext> expressions = List.of(col1, col2);
    DataSchema schema = new DataSchema(new String[]{"c1", "c2"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT});
    RawMultiColumnDistinctExecutor executor =
        new RawMultiColumnDistinctExecutor(expressions, false, schema, 10, false, null);
    executor.setNumRowsWithoutChangeInDistinct(2);

    Map<ExpressionContext, BlockValSet> blockValSets = Map.of(
        col1, new IntBlockValSet(new int[]{0, 0, 0, 1}),
        col2, new IntBlockValSet(new int[]{10, 10, 10, 11})
    );
    ValueBlock block = new SimpleValueBlock(4, blockValSets);

    boolean satisfied = executor.process(block);
    DistinctTable result = executor.getResult();

    assertTrue(satisfied);
    assertTrue(executor.isNumRowsWithoutChangeLimitReached());
    assertEquals(executor.getNumRowsProcessed(), 3);
    assertEquals(result.size(), 1);
  }

  @Test
  public void testDictionaryBasedMultiColumnExecutorStopsAfterNoChangeWithinBlock() {
    ExpressionContext col1 = ExpressionContext.forIdentifier("c1");
    ExpressionContext col2 = ExpressionContext.forIdentifier("c2");
    List<ExpressionContext> expressions = List.of(col1, col2);
    DataSchema schema = new DataSchema(new String[]{"c1", "c2"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT});
    Dictionary dictionary = new SimpleIntDictionary(new int[]{0, 1, 2, 3, 4});
    DictionaryBasedMultiColumnDistinctExecutor executor =
        new DictionaryBasedMultiColumnDistinctExecutor(expressions, false, schema,
            List.of(dictionary, dictionary), 10, false, null);
    executor.setNumRowsWithoutChangeInDistinct(2);

    Map<ExpressionContext, BlockValSet> blockValSets = Map.of(
        col1, new DictionaryBlockValSet(new int[]{0, 0, 0, 1}),
        col2, new DictionaryBlockValSet(new int[]{4, 4, 4, 3})
    );
    ValueBlock block = new SimpleValueBlock(4, blockValSets);

    boolean satisfied = executor.process(block);
    DistinctTable result = executor.getResult();

    assertTrue(satisfied);
    assertTrue(executor.isNumRowsWithoutChangeLimitReached());
    assertEquals(executor.getNumRowsProcessed(), 3);
    assertEquals(result.size(), 1);
  }

  @Test
  public void testRawSingleColumnExecutorStopsWhenTimeBudgetConsumed() {
    ExpressionContext expression = ExpressionContext.forIdentifier("c1");
    IntDistinctExecutor executor =
        new IntDistinctExecutor(expression, DataType.INT, /*limit*/ 10, false, null);
    // Force an immediate timeout to exercise time-budget early termination.
    executor.setRemainingTimeNanos(0);
    int[] values = new int[]{1, 2, 3, 4, 5};
    ValueBlock block = new SimpleValueBlock(values.length, Map.of(expression, new IntBlockValSet(values)));

    boolean satisfied = executor.process(block);

    assertTrue(satisfied, "should stop immediately when time budget is exhausted");
    assertEquals(executor.getNumRowsProcessed(), 0);
    assertEquals(executor.getResult().size(), 0);
  }

  /**
   * {@link ValueBlock} implementation backed by a simple map.
   */
  private static class SimpleValueBlock implements ValueBlock {
    private final int _numDocs;
    private final Map<ExpressionContext, BlockValSet> _blockValSets;
    private final Map<String, BlockValSet> _blockValSetsByName;

    SimpleValueBlock(int numDocs, Map<ExpressionContext, BlockValSet> blockValSets) {
      _numDocs = numDocs;
      _blockValSets = blockValSets;
      _blockValSetsByName = blockValSets.entrySet().stream()
          .collect(Collectors.toUnmodifiableMap(entry -> entry.getKey().toString(), Map.Entry::getValue));
    }

    @Override
    public int getNumDocs() {
      return _numDocs;
    }

    @Nullable
    @Override
    public int[] getDocIds() {
      return null;
    }

    @Override
    public BlockValSet getBlockValueSet(ExpressionContext expression) {
      return _blockValSets.get(expression);
    }

    @Override
    public BlockValSet getBlockValueSet(String column) {
      return _blockValSetsByName.get(column);
    }

    @Override
    public BlockValSet getBlockValueSet(String[] paths) {
      return _blockValSetsByName.get(paths[0]);
    }
  }

  private static class IntBlockValSet implements BlockValSet {
    private final int[] _values;

    IntBlockValSet(int[] values) {
      _values = values;
    }

    @Nullable
    @Override
    public org.roaringbitmap.RoaringBitmap getNullBitmap() {
      return null;
    }

    @Override
    public DataType getValueType() {
      return DataType.INT;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Nullable
    @Override
    public Dictionary getDictionary() {
      return null;
    }

    @Override
    public int[] getDictionaryIdsSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[] getIntValuesSV() {
      return _values;
    }

    @Override
    public long[] getLongValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[] getFloatValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public double[] getDoubleValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal[] getBigDecimalValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String[] getStringValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[][] getBytesValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[][] getDictionaryIdsMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[][] getIntValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long[][] getLongValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[][] getFloatValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public double[][] getDoubleValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String[][] getStringValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[][][] getBytesValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[] getNumMVEntries() {
      throw new UnsupportedOperationException();
    }
  }

  private static class DictionaryBlockValSet implements BlockValSet {
    private final int[] _dictIds;

    DictionaryBlockValSet(int[] dictIds) {
      _dictIds = dictIds;
    }

    @Nullable
    @Override
    public org.roaringbitmap.RoaringBitmap getNullBitmap() {
      return null;
    }

    @Override
    public DataType getValueType() {
      return DataType.INT;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Nullable
    @Override
    public Dictionary getDictionary() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[] getDictionaryIdsSV() {
      return _dictIds;
    }

    @Override
    public int[] getIntValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long[] getLongValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[] getFloatValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public double[] getDoubleValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal[] getBigDecimalValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String[] getStringValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[][] getBytesValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[][] getDictionaryIdsMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[][] getIntValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long[][] getLongValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[][] getFloatValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public double[][] getDoubleValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String[][] getStringValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[][][] getBytesValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[] getNumMVEntries() {
      throw new UnsupportedOperationException();
    }
  }

  private static class SimpleIntDictionary implements Dictionary {
    private final int[] _values;

    SimpleIntDictionary(int[] values) {
      _values = values;
    }

    @Override
    public boolean isSorted() {
      return true;
    }

    @Override
    public DataType getValueType() {
      return DataType.INT;
    }

    @Override
    public int length() {
      return _values.length;
    }

    @Override
    public int indexOf(String stringValue) {
      return Arrays.binarySearch(_values, Integer.parseInt(stringValue));
    }

    @Override
    public int insertionIndexOf(String stringValue) {
      return Arrays.binarySearch(_values, Integer.parseInt(stringValue));
    }

    @Override
    public IntSet getDictIdsInRange(String lower, String upper, boolean includeLower, boolean includeUpper) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int compare(int dictId1, int dictId2) {
      return Integer.compare(_values[dictId1], _values[dictId2]);
    }

    @Override
    public Comparable getMinVal() {
      return _values[0];
    }

    @Override
    public Comparable getMaxVal() {
      return _values[_values.length - 1];
    }

    @Override
    public Object getSortedValues() {
      return _values.clone();
    }

    @Override
    public Object get(int dictId) {
      return _values[dictId];
    }

    @Override
    public int getIntValue(int dictId) {
      return _values[dictId];
    }

    @Override
    public long getLongValue(int dictId) {
      return _values[dictId];
    }

    @Override
    public float getFloatValue(int dictId) {
      return _values[dictId];
    }

    @Override
    public double getDoubleValue(int dictId) {
      return _values[dictId];
    }

    @Override
    public BigDecimal getBigDecimalValue(int dictId) {
      return BigDecimal.valueOf(_values[dictId]);
    }

    @Override
    public String getStringValue(int dictId) {
      return Integer.toString(_values[dictId]);
    }

    @Override
    public int indexOf(int intValue) {
      return Arrays.binarySearch(_values, intValue);
    }

    @Override
    public void close()
        throws IOException {
      // Nothing to close
    }
  }
}
