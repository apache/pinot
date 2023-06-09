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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.RowBasedBlockValueFetcher;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.core.query.distinct.DistinctExecutorUtils;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


/**
 * {@link DistinctExecutor} for multiple columns where some columns are raw (non-dictionary-encoded).
 */
public class RawMultiColumnDistinctExecutor implements DistinctExecutor {
  private final List<ExpressionContext> _expressions;
  private final boolean _hasMVExpression;
  private final DistinctTable _distinctTable;
  private final boolean _nullHandlingEnabled;

  public RawMultiColumnDistinctExecutor(List<ExpressionContext> expressions, boolean hasMVExpression,
      List<DataType> dataTypes, @Nullable List<OrderByExpressionContext> orderByExpressions,
      boolean nullHandlingEnabled, int limit) {
    _expressions = expressions;
    _hasMVExpression = hasMVExpression;
    _nullHandlingEnabled = nullHandlingEnabled;

    int numExpressions = expressions.size();
    String[] columnNames = new String[numExpressions];
    ColumnDataType[] columnDataTypes = new ColumnDataType[numExpressions];
    for (int i = 0; i < numExpressions; i++) {
      columnNames[i] = expressions.get(i).toString();
      columnDataTypes[i] = ColumnDataType.fromDataTypeSV(dataTypes.get(i));
    }
    DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
    _distinctTable = new DistinctTable(dataSchema, orderByExpressions, limit, _nullHandlingEnabled);
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
      if (_nullHandlingEnabled) {
        for (int i = 0; i < numExpressions; i++) {
          nullBitmaps[i] = blockValSets[i].getNullBitmap();
        }
      }
      RowBasedBlockValueFetcher valueFetcher = new RowBasedBlockValueFetcher(blockValSets);
      for (int docId = 0; docId < numDocs; docId++) {
        Record record = new Record(valueFetcher.getRow(docId));
        if (_nullHandlingEnabled) {
          for (int i = 0; i < numExpressions; i++) {
            if (nullBitmaps[i] != null && nullBitmaps[i].contains(docId)) {
              record.getValues()[i] = null;
            }
          }
        }
        if (_distinctTable.hasOrderBy()) {
          _distinctTable.addWithOrderBy(record);
        } else {
          if (_distinctTable.addWithoutOrderBy(record)) {
            return true;
          }
        }
      }
    } else {
      // TODO(https://github.com/apache/pinot/issues/10882): support NULL for multi-value
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
    DataType storedType = blockValueSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[] intValues = blockValueSet.getIntValuesSV();
        values = new Object[numDocs];
        for (int j = 0; j < numDocs; j++) {
          values[j] = intValues[j];
        }
        return values;
      case LONG:
        long[] longValues = blockValueSet.getLongValuesSV();
        values = new Object[numDocs];
        for (int j = 0; j < numDocs; j++) {
          values[j] = longValues[j];
        }
        return values;
      case FLOAT:
        float[] floatValues = blockValueSet.getFloatValuesSV();
        values = new Object[numDocs];
        for (int j = 0; j < numDocs; j++) {
          values[j] = floatValues[j];
        }
        return values;
      case DOUBLE:
        double[] doubleValues = blockValueSet.getDoubleValuesSV();
        values = new Object[numDocs];
        for (int j = 0; j < numDocs; j++) {
          values[j] = doubleValues[j];
        }
        return values;
      case BIG_DECIMAL:
        return blockValueSet.getBigDecimalValuesSV();
      case STRING:
        return blockValueSet.getStringValuesSV();
      case BYTES:
        byte[][] bytesValues = blockValueSet.getBytesValuesSV();
        values = new Object[numDocs];
        for (int j = 0; j < numDocs; j++) {
          values[j] = new ByteArray(bytesValues[j]);
        }
        return values;
      default:
        throw new IllegalStateException("Unsupported value type: " + storedType + " for single-value column");
    }
  }

  private Object[][] getMVValues(BlockValSet blockValueSet, int numDocs) {
    Object[][] values;
    DataType storedType = blockValueSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[][] intValues = blockValueSet.getIntValuesMV();
        values = new Object[numDocs][];
        for (int j = 0; j < numDocs; j++) {
          values[j] = ArrayUtils.toObject(intValues[j]);
        }
        return values;
      case LONG:
        long[][] longValues = blockValueSet.getLongValuesMV();
        values = new Object[numDocs][];
        for (int j = 0; j < numDocs; j++) {
          values[j] = ArrayUtils.toObject(longValues[j]);
        }
        return values;
      case FLOAT:
        float[][] floatValues = blockValueSet.getFloatValuesMV();
        values = new Object[numDocs][];
        for (int j = 0; j < numDocs; j++) {
          values[j] = ArrayUtils.toObject(floatValues[j]);
        }
        return values;
      case DOUBLE:
        double[][] doubleValues = blockValueSet.getDoubleValuesMV();
        values = new Object[numDocs][];
        for (int j = 0; j < numDocs; j++) {
          values[j] = ArrayUtils.toObject(doubleValues[j]);
        }
        return values;
      case STRING:
        return blockValueSet.getStringValuesMV();
      default:
        throw new IllegalStateException("Unsupported value type: " + storedType + " for multi-value column");
    }
  }

  @Override
  public DistinctTable getResult() {
    return _distinctTable;
  }
}
