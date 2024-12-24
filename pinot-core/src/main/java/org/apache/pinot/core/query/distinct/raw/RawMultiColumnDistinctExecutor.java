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
package org.apache.pinot.core.query.distinct.raw;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.RowBasedBlockValueFetcher;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.distinct.DistinctExecutorUtils;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.core.query.distinct.table.MultiColumnDistinctTable;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;


/**
 * {@link DistinctExecutor} for multiple columns where some columns are raw (non-dictionary-encoded).
 */
public class RawMultiColumnDistinctExecutor implements DistinctExecutor {
  private final List<ExpressionContext> _expressions;
  private final boolean _hasMVExpression;
  private final boolean _nullHandlingEnabled;
  private final MultiColumnDistinctTable _distinctTable;

  public RawMultiColumnDistinctExecutor(List<ExpressionContext> expressions, boolean hasMVExpression,
      DataSchema dataSchema, int limit, boolean nullHandlingEnabled,
      @Nullable List<OrderByExpressionContext> orderByExpressions) {
    _expressions = expressions;
    _hasMVExpression = hasMVExpression;
    _nullHandlingEnabled = nullHandlingEnabled;
    _distinctTable = new MultiColumnDistinctTable(dataSchema, limit, nullHandlingEnabled, orderByExpressions);
  }

  @Override
  public boolean process(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int numExpressions = _expressions.size();
    if (!_hasMVExpression) {
      BlockValSet[] blockValSets = new BlockValSet[numExpressions];
      for (int i = 0; i < numExpressions; i++) {
        blockValSets[i] = valueBlock.getBlockValueSet(_expressions.get(i));
      }
      RoaringBitmap[] nullBitmaps = new RoaringBitmap[numExpressions];
      boolean hasNullValue = false;
      if (_nullHandlingEnabled) {
        for (int i = 0; i < numExpressions; i++) {
          RoaringBitmap nullBitmap = blockValSets[i].getNullBitmap();
          if (nullBitmap != null && !nullBitmap.isEmpty()) {
            nullBitmaps[i] = nullBitmap;
            hasNullValue = true;
          }
        }
      }
      RowBasedBlockValueFetcher valueFetcher = new RowBasedBlockValueFetcher(blockValSets);
      if (hasNullValue) {
        Object[][] values = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
          values[i] = valueFetcher.getRow(i);
        }
        for (int i = 0; i < numExpressions; i++) {
          RoaringBitmap nullBitmap = nullBitmaps[i];
          if (nullBitmap != null && !nullBitmap.isEmpty()) {
            int finalI = i;
            nullBitmap.forEach((IntConsumer) j -> values[j][finalI] = null);
          }
        }
        for (int i = 0; i < numDocs; i++) {
          Record record = new Record(values[i]);
          if (_distinctTable.hasOrderBy()) {
            _distinctTable.addWithOrderBy(record);
          } else {
            if (_distinctTable.addWithoutOrderBy(record)) {
              return true;
            }
          }
        }
      } else {
        for (int i = 0; i < numDocs; i++) {
          Record record = new Record(valueFetcher.getRow(i));
          if (_distinctTable.hasOrderBy()) {
            _distinctTable.addWithOrderBy(record);
          } else {
            if (_distinctTable.addWithoutOrderBy(record)) {
              return true;
            }
          }
        }
      }
    } else {
      Object[][] svValues = new Object[numExpressions][];
      Object[][][] mvValues = new Object[numExpressions][][];
      for (int i = 0; i < numExpressions; i++) {
        BlockValSet blockValueSet = valueBlock.getBlockValueSet(_expressions.get(i));
        if (blockValueSet.isSingleValue()) {
          svValues[i] = getSVValues(blockValueSet, numDocs);
        } else {
          mvValues[i] = getMVValues(blockValueSet, numDocs);
        }
      }
      for (int i = 0; i < numDocs; i++) {
        Object[][] records = DistinctExecutorUtils.getRecords(svValues, mvValues, i);
        for (Object[] record : records) {
          if (_distinctTable.hasOrderBy()) {
            _distinctTable.addWithOrderBy(new Record(record));
          } else {
            if (_distinctTable.addWithoutOrderBy(new Record(record))) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  private Object[] getSVValues(BlockValSet blockValueSet, int numDocs) {
    Object[] values;
    DataType valueType = blockValueSet.getValueType();
    switch (valueType.getStoredType()) {
      case INT:
        int[] intValues = blockValueSet.getIntValuesSV();
        values = new Object[numDocs];
        for (int i = 0; i < numDocs; i++) {
          values[i] = intValues[i];
        }
        break;
      case LONG:
        long[] longValues = blockValueSet.getLongValuesSV();
        values = new Object[numDocs];
        for (int i = 0; i < numDocs; i++) {
          values[i] = longValues[i];
        }
        break;
      case FLOAT:
        float[] floatValues = blockValueSet.getFloatValuesSV();
        values = new Object[numDocs];
        for (int i = 0; i < numDocs; i++) {
          values[i] = floatValues[i];
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValueSet.getDoubleValuesSV();
        values = new Object[numDocs];
        for (int i = 0; i < numDocs; i++) {
          values[i] = doubleValues[i];
        }
        break;
      case BIG_DECIMAL:
        BigDecimal[] bigDecimalValues = blockValueSet.getBigDecimalValuesSV();
        values = bigDecimalValues.length == numDocs ? bigDecimalValues : Arrays.copyOf(bigDecimalValues, numDocs);
        break;
      case STRING:
        String[] stringValues = blockValueSet.getStringValuesSV();
        values = stringValues.length == numDocs ? stringValues : Arrays.copyOf(stringValues, numDocs);
        break;
      case BYTES:
        byte[][] bytesValues = blockValueSet.getBytesValuesSV();
        values = new Object[numDocs];
        for (int i = 0; i < numDocs; i++) {
          values[i] = new ByteArray(bytesValues[i]);
        }
        break;
      default:
        throw new IllegalStateException("Unsupported value type: " + valueType + " for single-value column");
    }
    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValueSet.getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        nullBitmap.forEach((IntConsumer) i -> values[i] = null);
      }
    }
    return values;
  }

  // TODO(https://github.com/apache/pinot/issues/10882): support NULL for multi-value
  private Object[][] getMVValues(BlockValSet blockValueSet, int numDocs) {
    Object[][] values;
    DataType valueType = blockValueSet.getValueType();
    switch (valueType.getStoredType()) {
      case INT:
        int[][] intValues = blockValueSet.getIntValuesMV();
        values = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
          values[i] = ArrayUtils.toObject(intValues[i]);
        }
        break;
      case LONG:
        long[][] longValues = blockValueSet.getLongValuesMV();
        values = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
          values[i] = ArrayUtils.toObject(longValues[i]);
        }
        break;
      case FLOAT:
        float[][] floatValues = blockValueSet.getFloatValuesMV();
        values = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
          values[i] = ArrayUtils.toObject(floatValues[i]);
        }
        break;
      case DOUBLE:
        double[][] doubleValues = blockValueSet.getDoubleValuesMV();
        values = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
          values[i] = ArrayUtils.toObject(doubleValues[i]);
        }
        break;
      case STRING:
        String[][] stringValues = blockValueSet.getStringValuesMV();
        values = stringValues.length == numDocs ? stringValues : Arrays.copyOf(stringValues, numDocs);
        break;
      case BYTES:
        byte[][][] bytesValuesMV = blockValueSet.getBytesValuesMV();
        values = new Object[numDocs][];
        for (int i = 0; i < numDocs; i++) {
          byte[][] bytesValues = bytesValuesMV[i];
          values[i] = new Object[bytesValues.length];
          for (int j = 0; j < bytesValues.length; j++) {
            values[i][j] = new ByteArray(bytesValues[j]);
          }
        }
        break;
      default:
        throw new IllegalStateException("Unsupported value type: " + valueType + " for multi-value column");
    }
    return values;
  }

  @Override
  public DistinctTable getResult() {
    return _distinctTable;
  }
}
