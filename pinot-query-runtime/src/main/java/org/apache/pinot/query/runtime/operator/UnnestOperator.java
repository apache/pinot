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
package org.apache.pinot.query.runtime.operator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.UnnestNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.operands.TransformOperandFactory;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * UnnestOperator expands array/collection values per input row into zero or more output rows.
 * Supports multiple arrays, aligning them by index (like a zip operation).
 * If arrays have different lengths, shorter arrays are padded with null values.
 * The output schema is provided by the associated UnnestNode's data schema.
 */
public class UnnestOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(UnnestOperator.class);
  private static final String EXPLAIN_NAME = "UNNEST";

  private final MultiStageOperator _input;
  private final List<TransformOperand> _arrayExprOperands;
  private final DataSchema _resultSchema;
  private final boolean _withOrdinality;
  private final List<Integer> _elementIndexes;
  private final int _ordinalityIndex;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  public UnnestOperator(OpChainExecutionContext context, MultiStageOperator input, DataSchema inputSchema,
      UnnestNode node) {
    super(context);
    _input = input;
    List<RexExpression> arrayExprs = node.getArrayExprs();
    _arrayExprOperands = new ArrayList<>(arrayExprs.size());
    for (RexExpression arrayExpr : arrayExprs) {
      _arrayExprOperands.add(TransformOperandFactory.getTransformOperand(arrayExpr, inputSchema));
    }
    _resultSchema = node.getDataSchema();
    _withOrdinality = node.isWithOrdinality();
    _elementIndexes = node.getElementIndexes();
    _ordinalityIndex = node.getOrdinalityIndex();
  }

  @Override
  public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
    _statMap.merge(StatKey.ALLOCATED_MEMORY_BYTES, memoryUsedBytes);
    _statMap.merge(StatKey.GC_TIME_MS, gcTimeMs);
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return List.of(_input);
  }

  @Override
  public Type getOperatorType() {
    return Type.UNNEST;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected MseBlock getNextBlock() {
    MseBlock block = _input.nextBlock();
    if (block.isEos()) {
      return block;
    }
    MseBlock.Data dataBlock = (MseBlock.Data) block;
    List<Object[]> inputRows = dataBlock.asRowHeap().getRows();
    List<Object[]> outRows = new ArrayList<>();

    for (Object[] row : inputRows) {
      // Extract all arrays from the input row
      List<List<Object>> arrays = new ArrayList<>();
      for (TransformOperand operand : _arrayExprOperands) {
        Object value = operand.apply(row);
        List<Object> elements = extractArrayElements(value);
        arrays.add(elements);
      }
      // Align arrays by index (zip operation)
      alignArraysByIndex(row, arrays, outRows);
    }

    return new RowHeapDataBlock(outRows, _resultSchema);
  }

  private List<Object> extractArrayElements(Object value) {
    List<Object> elements = new ArrayList<>();
    if (value == null) {
      return elements;
    }
    if (value instanceof List) {
      elements.addAll((List<?>) value);
    } else if (value.getClass().isArray()) {
      if (value instanceof int[]) {
        int[] arr = (int[]) value;
        for (int v : arr) {
          elements.add(v);
        }
      } else if (value instanceof long[]) {
        long[] arr = (long[]) value;
        for (long v : arr) {
          elements.add(v);
        }
      } else if (value instanceof double[]) {
        double[] arr = (double[]) value;
        for (double v : arr) {
          elements.add(v);
        }
      } else if (value instanceof boolean[]) {
        boolean[] arr = (boolean[]) value;
        for (boolean v : arr) {
          elements.add(v);
        }
      } else if (value instanceof char[]) {
        char[] arr = (char[]) value;
        for (char v : arr) {
          elements.add(v);
        }
      } else if (value instanceof short[]) {
        short[] arr = (short[]) value;
        for (short v : arr) {
          elements.add(v);
        }
      } else if (value instanceof byte[]) {
        byte[] arr = (byte[]) value;
        for (byte v : arr) {
          elements.add(v);
        }
      } else if (value instanceof String[]) {
        String[] arr = (String[]) value;
        Collections.addAll(elements, arr);
      } else if (value instanceof Object[]) {
        Object[] arr = (Object[]) value;
        Collections.addAll(elements, arr);
      } else {
        int length = java.lang.reflect.Array.getLength(value);
        for (int i = 0; i < length; i++) {
          elements.add(java.lang.reflect.Array.get(value, i));
        }
      }
    } else {
      // If not array-like, treat as a single element
      elements.add(value);
    }
    return elements;
  }

  private void alignArraysByIndex(Object[] inputRow, List<List<Object>> arrays, List<Object[]> outRows) {
    // Find the maximum length among all arrays
    int maxLength = 0;
    for (List<Object> array : arrays) {
      maxLength = Math.max(maxLength, array.size());
    }

    // If all arrays are empty, skip this row
    if (maxLength == 0) {
      return;
    }

    // Get default null values for each element column from the result schema
    int base = inputRow.length;
    List<Object> defaultNullValues = new ArrayList<>();
    for (int i = 0; i < arrays.size(); i++) {
      int elemPos;
      if (i < _elementIndexes.size() && _elementIndexes.get(i) >= 0
          && _elementIndexes.get(i) < _resultSchema.size()) {
        elemPos = _elementIndexes.get(i);
      } else {
        elemPos = base + i;
      }
      DataSchema.ColumnDataType columnDataType = _resultSchema.getColumnDataType(elemPos);
      defaultNullValues.add(columnDataType.getNullPlaceholder());
    }

    // Align arrays by index: for each position, take element from each array or use null
    int ordinality = 1;
    for (int idx = 0; idx < maxLength; idx++) {
      List<Object> alignedElements = new ArrayList<>();
      for (int arrIdx = 0; arrIdx < arrays.size(); arrIdx++) {
        List<Object> array = arrays.get(arrIdx);
        if (idx < array.size()) {
          alignedElements.add(array.get(idx));
        } else {
          // Use default null value for shorter arrays
          alignedElements.add(defaultNullValues.get(arrIdx));
        }
      }
      outRows.add(appendElements(inputRow, alignedElements, ordinality++));
    }
  }

  private Object[] appendElements(Object[] inputRow, List<Object> elements, int ordinality) {
    int outSize = _resultSchema.size();
    Object[] out = new Object[outSize];
    // Copy left columns at beginning
    System.arraycopy(inputRow, 0, out, 0, inputRow.length);
    // Determine positions for elements. Track next free slot after the copied input row.
    int base = inputRow.length;
    int nextFreePos = base;
    for (int i = 0; i < elements.size(); i++) {
      int elemPos = -1;
      if (i < _elementIndexes.size()) {
        int configuredPos = _elementIndexes.get(i);
        if (configuredPos >= 0 && configuredPos < outSize) {
          elemPos = configuredPos;
        }
      }
      if (elemPos < 0) {
        if (nextFreePos >= outSize) {
          // No room to write this element; skip because downstream schema does not include it.
          continue;
        }
        elemPos = nextFreePos++;
      }
      out[elemPos] = elements.get(i);
    }
    if (_withOrdinality) {
      int ordPos = (_ordinalityIndex >= 0 && _ordinalityIndex < outSize) ? _ordinalityIndex : nextFreePos;
      if (ordPos < outSize) {
        out[ordPos] = ordinality;
      }
    }
    return out;
  }

  @Override
  protected StatMap<?> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  public enum StatKey implements StatMap.Key {
    EXECUTION_TIME_MS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    EMITTED_ROWS(StatMap.Type.LONG),
    /**
     * Allocated memory in bytes for this operator or its children in the same stage.
     */
    ALLOCATED_MEMORY_BYTES(StatMap.Type.LONG),
    /**
     * Time spent on GC while this operator or its children in the same stage were running.
     */
    GC_TIME_MS(StatMap.Type.LONG);

    private final StatMap.Type _type;

    StatKey(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }
}
