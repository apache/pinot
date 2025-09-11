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
package org.apache.pinot.query.runtime.operator.set;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.ExplainPlanRows;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * Set operator, which supports UNION, INTERSECT and EXCEPT.
 * This has two child operators, and the left child operator is the one that is used to construct the result.
 * The right child operator is used to construct a set of rows that are used to filter the left child operator.
 * The right child operator is consumed in a blocking manner, and the left child operator is consumed in a non-blocking
 * UnionOperator: The right child operator is consumed in a blocking manner.
 */
public abstract class SetOperator extends MultiStageOperator {

  protected final MultiStageOperator _leftChildOperator;
  protected final MultiStageOperator _rightChildOperator;
  protected final DataSchema _dataSchema;

  private boolean _isRightChildOperatorProcessed;
  private MseBlock.Eos _eos;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  public SetOperator(OpChainExecutionContext opChainExecutionContext, List<MultiStageOperator> inputOperators,
      DataSchema dataSchema) {
    super(opChainExecutionContext);
    _dataSchema = dataSchema;
    Preconditions.checkState(inputOperators.size() == 2, "Set operator should have 2 child operators");
    _leftChildOperator = inputOperators.get(0);
    _rightChildOperator = inputOperators.get(1);
    _isRightChildOperatorProcessed = false;
  }

  @Override
  public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
    _statMap.merge(StatKey.ALLOCATED_MEMORY_BYTES, memoryUsedBytes);
    _statMap.merge(StatKey.GC_TIME_MS, gcTimeMs);
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return List.of(_leftChildOperator, _rightChildOperator);
  }

  @Override
  public void prepareForExplainPlan(ExplainPlanRows explainPlanRows) {
    super.prepareForExplainPlan(explainPlanRows);
  }

  @Override
  public void explainPlan(ExplainPlanRows explainPlanRows, int[] globalId, int parentId) {
    super.explainPlan(explainPlanRows, globalId, parentId);
  }

  @Override
  public IndexSegment getIndexSegment() {
    return super.getIndexSegment();
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return super.getExecutionStatistics();
  }

  @Override
  protected MseBlock getNextBlock() {
    if (_eos != null) {
      return _eos;
    }

    if (!_isRightChildOperatorProcessed) {
      MseBlock mseBlock = processRightOperator();

      if (mseBlock.isData()) {
        return mseBlock;
      } else if (mseBlock.isError()) {
        _eos = (MseBlock.Eos) mseBlock;
        return _eos;
      } else if (mseBlock.isSuccess()) {
        // If it's a regular EOS block, we continue to process the left child operator.
        _isRightChildOperatorProcessed = true;
      }
    }

    MseBlock mseBlock = processLeftOperator();
    if (mseBlock.isEos()) {
      _eos = (MseBlock.Eos) mseBlock;
      return _eos;
    } else {
      return mseBlock;
    }
  }

  protected abstract MseBlock processLeftOperator();

  protected abstract MseBlock processRightOperator();

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
    EMITTED_ROWS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
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
