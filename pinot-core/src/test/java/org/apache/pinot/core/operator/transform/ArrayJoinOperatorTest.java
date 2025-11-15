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
package org.apache.pinot.core.operator.transform;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.ArrayJoinType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.DocIdOrderedOperator.DocIdOrder;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.request.context.ArrayJoinContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;


public class ArrayJoinOperatorTest {
  private static final ExpressionContext MV_COL = ExpressionContext.forIdentifier("mvRawCol1");
  private static final ExpressionContext SV_COL = ExpressionContext.forIdentifier("svCol");

  @Test
  public void testArrayJoinExpansion() {
    ArrayJoinContext arrayJoinContext =
        new ArrayJoinContext(ArrayJoinType.INNER,
            List.of(new ArrayJoinContext.Operand(MV_COL, null)));
    QueryContext queryContext = new QueryContext.Builder()
        .setTableName("testTable")
        .setSelectExpressions(List.of(MV_COL, SV_COL))
        .setArrayJoinContexts(List.of(arrayJoinContext))
        .build();

    TestValueBlock valueBlock = new TestValueBlock();
    TestProjectOperator projectOperator = new TestProjectOperator(valueBlock);
    ArrayJoinOperator arrayJoinOperator = new ArrayJoinOperator(queryContext, projectOperator);

    ValueBlock outputBlock = arrayJoinOperator.nextBlock();
    assertEquals(outputBlock.getNumDocs(), 3);

    int[] mvValues = outputBlock.getBlockValueSet(MV_COL).getIntValuesSV();
    assertEquals(mvValues, new int[]{1, 2, 3});

    int[] svValues = outputBlock.getBlockValueSet(SV_COL).getIntValuesSV();
    assertEquals(svValues, new int[]{10, 10, 20});

    assertSame(arrayJoinOperator.getArrayJoinContexts(), queryContext.getArrayJoinContexts());
    assertEquals(arrayJoinOperator.toExplainString(), "ARRAY_JOIN");
    assertSame(arrayJoinOperator.withOrder(DocIdOrder.ASC), arrayJoinOperator);
  }

  private static class TestProjectOperator extends BaseProjectOperator<ValueBlock> {
    private final ValueBlock _valueBlock;
    private boolean _returnedBlock;

    TestProjectOperator(ValueBlock valueBlock) {
      _valueBlock = valueBlock;
    }

    @Override
    protected ValueBlock getNextBlock() {
      if (_returnedBlock) {
        return null;
      }
      _returnedBlock = true;
      return _valueBlock;
    }

    @Override
    public List<Operator> getChildOperators() {
      return Collections.emptyList();
    }

    @Override
    public String toExplainString() {
      return "TEST_PROJECT";
    }

    @Override
    public ExecutionStatistics getExecutionStatistics() {
      return new ExecutionStatistics(0, 0, 0, 0);
    }

    @Override
    public boolean isCompatibleWith(DocIdOrder order) {
      return true;
    }

    @Override
    public Map<String, ColumnContext> getSourceColumnContextMap() {
      return Collections.emptyMap();
    }

    @Override
    public ColumnContext getResultColumnContext(ExpressionContext expression) {
      if (MV_COL.equals(expression)) {
        return ColumnContext.forDataType(DataType.INT, false);
      }
      if (SV_COL.equals(expression)) {
        return ColumnContext.forDataType(DataType.INT, true);
      }
      return ColumnContext.forDataType(DataType.INT, true);
    }

    @Override
    public BaseProjectOperator<ValueBlock> withOrder(DocIdOrder newOrder) {
      return this;
    }
  }

  private static class TestValueBlock implements ValueBlock {
    private static final int[][] MV_VALUES = new int[][]{{1, 2}, {3}};
    private static final int[] MV_NUM_ENTRIES = new int[]{2, 1};
    private static final int[] SV_VALUES = new int[]{10, 20};

    @Override
    public int getNumDocs() {
      return 2;
    }

    @Override
    public int[] getDocIds() {
      return new int[]{0, 1};
    }

    @Override
    public BlockValSet getBlockValueSet(ExpressionContext expression) {
      if (MV_COL.equals(expression)) {
        return new TestMvIntBlockValSet(MV_VALUES, MV_NUM_ENTRIES);
      }
      if (SV_COL.equals(expression)) {
        return new TestSvIntBlockValSet(SV_VALUES);
      }
      return new TestSvIntBlockValSet(new int[]{0, 0});
    }

    @Override
    public BlockValSet getBlockValueSet(String column) {
      return getBlockValueSet(ExpressionContext.forIdentifier(column));
    }

    @Override
    public BlockValSet getBlockValueSet(String[] paths) {
      throw new UnsupportedOperationException();
    }
  }

  private static class TestMvIntBlockValSet implements BlockValSet {
    private final int[][] _values;
    private final int[] _numEntries;

    TestMvIntBlockValSet(int[][] values, int[] numEntries) {
      _values = values;
      _numEntries = numEntries;
    }

    @Override
    public RoaringBitmap getNullBitmap() {
      return null;
    }

    @Override
    public DataType getValueType() {
      return DataType.INT;
    }

    @Override
    public boolean isSingleValue() {
      return false;
    }

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
      return _values;
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
      return _numEntries;
    }
  }

  private static class TestSvIntBlockValSet implements BlockValSet {
    private final int[] _values;

    TestSvIntBlockValSet(int[] values) {
      _values = values;
    }

    @Override
    public RoaringBitmap getNullBitmap() {
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
}
