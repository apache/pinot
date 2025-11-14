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
import java.util.List;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.UnnestNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.operands.TransformOperandFactory;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * UnnestOperator expands an array/collection value per input row into zero or more output rows.
 * The output schema is provided by the associated UnnestNode's data schema.
 */
public class UnnestOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(UnnestOperator.class);
  private static final String EXPLAIN_NAME = "UNNEST";

  private final MultiStageOperator _input;
  private final TransformOperand _arrayExprOperand;
  private final DataSchema _resultSchema;
  private final boolean _withOrdinality;
  private final int _elementIndex;
  private final int _ordinalityIndex;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  public UnnestOperator(OpChainExecutionContext context, MultiStageOperator input, DataSchema inputSchema,
      UnnestNode node) {
    super(context);
    _input = input;
    _arrayExprOperand = TransformOperandFactory.getTransformOperand(node.getArrayExpr(), inputSchema);
    _resultSchema = node.getDataSchema();
    _withOrdinality = node.isWithOrdinality();
    _elementIndex = node.getElementIndex();
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
      Object value = _arrayExprOperand.apply(row);
      int ord = 1;
      if (value == null) {
        continue;
      }
      if (value instanceof List) {
        for (Object element : (List<?>) value) {
          outRows.add(appendElement(row, element, ord++));
        }
      } else if (value.getClass().isArray()) {
        int length = java.lang.reflect.Array.getLength(value);
        for (int i = 0; i < length; i++) {
          Object element = java.lang.reflect.Array.get(value, i);
          outRows.add(appendElement(row, element, ord++));
        }
      } else {
        // If not array-like, treat as a single element
        outRows.add(appendElement(row, value, ord));
      }
    }

    return new RowHeapDataBlock(outRows, _resultSchema);
  }

  private Object[] appendElement(Object[] inputRow, Object element, int ordinality) {
    int outSize = _resultSchema.size();
    Object[] out = new Object[outSize];
    // Copy left columns at beginning
    System.arraycopy(inputRow, 0, out, 0, inputRow.length);
    // Determine positions
    int base = inputRow.length;
    int elemPos = (_elementIndex >= 0 && _elementIndex < outSize) ? _elementIndex : base;
    out[elemPos] = element;
    if (_withOrdinality) {
      int ordPos = (_ordinalityIndex >= 0 && _ordinalityIndex < outSize) ? _ordinalityIndex
          : (elemPos == base ? base + 1 : base);
      out[ordPos] = ordinality;
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
