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

import com.google.common.base.Preconditions;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.ArrowBlockConverter;
import org.apache.pinot.query.runtime.blocks.HeldBlocks;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.memory.ArrowQueryContext;
import org.apache.pinot.query.runtime.operator.BaseJoinOperator.StatKey;
import org.apache.pinot.query.runtime.operator.join.ArrowHashTable;
import org.apache.pinot.query.runtime.operator.join.ArrowJoinKeys;
import org.apache.pinot.query.runtime.operator.join.ArrowJoinOutput;
import org.apache.pinot.query.runtime.operator.join.ArrowJoinSupport;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.JoinOverFlowMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Thread-confined native INNER/LEFT/SEMI/ANTI equi-join on one fixed-width key.
 * Build blocks are transferred into operator ownership until close; probe and output buffers are bounded batches.
 * Unknown consumers receive independent heap output, while {@link ArrowBlockSource} consumers can opt into Arrow.
 */
public final class ArrowHashJoinOperator extends MultiStageOperator implements ArrowBlockSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowHashJoinOperator.class);

  private final MultiStageOperator _leftInput;
  private final MultiStageOperator _rightInput;
  private final DataSchema _rightSchema;
  private final DataSchema _resultSchema;
  private final JoinRelType _joinType;
  private final int _leftKey;
  private final int _rightKey;
  private final ColumnDataType _keyType;
  private final int _maxRowsInJoin;
  private final JoinOverFlowMode _joinOverflowMode;
  private final HeldBlocks _buildState = new HeldBlocks();
  private final List<ArrowBlock> _buildBlocks = _buildState.blocks();
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);
  private final long[] _keys = new long[ArrowJoinKeys.BATCH_SIZE];
  private final int[] _matches = new int[ArrowJoinKeys.BATCH_SIZE];
  private final int[] _leftRows = new int[ArrowJoinOutput.MAX_ROWS_PER_BLOCK];
  private final int[] _rightBlockIds = new int[ArrowJoinOutput.MAX_ROWS_PER_BLOCK];
  private final int[] _rightRows = new int[ArrowJoinOutput.MAX_ROWS_PER_BLOCK];

  @Nullable
  private ArrowQueryContext _arrowContext;
  @Nullable
  private ArrowHashTable _table;
  @Nullable
  private ArrowJoinOutput _output;
  @Nullable
  private ArrowBlock _probe;
  @Nullable
  private FieldVector _probeKey;
  @Nullable
  private MseBlock.Eos _eos;
  private int _probeRow;
  private int _matchStart;
  private int _matchCount;
  private int _nextRightRow = -1;
  private int _probeOutputRows;
  private boolean _arrowOutputEnabled;
  private boolean _closed;

  public ArrowHashJoinOperator(OpChainExecutionContext context, MultiStageOperator leftInput, DataSchema leftSchema,
      MultiStageOperator rightInput, DataSchema rightSchema, JoinNode node) {
    super(context);
    Preconditions.checkArgument(context.isArrowEnabled() && ArrowJoinSupport.supports(node, leftSchema, rightSchema),
        "Unsupported native Arrow join");
    _leftInput = leftInput;
    _rightInput = rightInput;
    _rightSchema = rightSchema;
    _resultSchema = node.getDataSchema();
    _joinType = node.getJoinType();
    _leftKey = node.getLeftKeys().get(0);
    _rightKey = node.getRightKeys().get(0);
    _keyType = leftSchema.getColumnDataType(_leftKey);
    _maxRowsInJoin = BaseJoinOperator.getMaxRowsInJoin(context.getOpChainMetadata(), node.getNodeHint());
    _joinOverflowMode = BaseJoinOperator.getJoinOverflowMode(context.getOpChainMetadata(), node.getNodeHint());
    Preconditions.checkArgument(_maxRowsInJoin >= 0, "maxRowsInJoin must be non-negative");
    if (leftInput instanceof ArrowBlockSource) {
      ((ArrowBlockSource) leftInput).enableArrowOutput();
    }
    if (rightInput instanceof ArrowBlockSource) {
      ((ArrowBlockSource) rightInput).enableArrowOutput();
    }
  }

  @Override
  public void enableArrowOutput() {
    _arrowOutputEnabled = true;
  }

  @Override
  protected MseBlock getNextBlock() {
    if (_closed) {
      return SuccessMseBlock.INSTANCE;
    }
    try {
      if (_eos != null) {
        releaseBuild();
        return _eos;
      }
      if (_isEarlyTerminated) {
        releaseProbe();
        MseBlock.Eos rightEos = _table == null ? drain(_rightInput) : SuccessMseBlock.INSTANCE;
        MseBlock.Eos leftEos = drain(_leftInput);
        _eos = rightEos.isError() ? rightEos : leftEos;
        releaseBuild();
        return _eos;
      }
      if (_table == null) {
        build();
      }
      return probe();
    } catch (RuntimeException | Error e) {
      try {
        releaseProbe();
      } catch (RuntimeException releaseError) {
        e.addSuppressed(releaseError);
      }
      if (e instanceof OutOfMemoryException) {
        throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
            "Arrow hash join exceeded the allocator memory limit", e);
      }
      throw e;
    }
  }

  private void build() {
    long startTime = System.currentTimeMillis();
    int numRows = 0;
    try {
      _arrowContext = _context.getOrCreateArrowContext();
      while (true) {
        MseBlock block = _rightInput.nextBlock();
        if (block.isEos()) {
          if (block.isError()) {
            _eos = (MseBlock.Eos) block;
            return;
          }
          checkTerminationAndSampleUsage();
          _table = new ArrowHashTable(_buildBlocks, _rightKey, _keyType, numRows,
              _joinType == JoinRelType.INNER || _joinType == JoinRelType.LEFT,
              this::checkTerminationAndSampleUsage);
          _output = new ArrowJoinOutput(_resultSchema, _rightSchema, _buildBlocks, _arrowContext,
              this::checkTerminationAndSampleUsage);
          _statMap.merge(StatKey.MAX_ROWS_IN_JOIN, numRows);
          return;
        }
        MseBlock.Data data = (MseBlock.Data) block;
        ArrowBlock owned = data instanceof ArrowBlock ? (ArrowBlock) data : null;
        try {
          int rows = data.getNumRows();
          int acceptedRows = Math.min(rows, _maxRowsInJoin - numRows);
          if (acceptedRows < rows) {
            if (_joinOverflowMode == JoinOverFlowMode.THROW) {
              _statMap.merge(StatKey.MAX_ROWS_IN_JOIN, (long) numRows + rows);
              BaseJoinOperator.throwForJoinRowLimitExceeded(
                  "Cannot build in memory hash table for join operator, reached number of rows limit: "
                      + _maxRowsInJoin);
            }
            _statMap.merge(StatKey.MAX_ROWS_IN_JOIN_REACHED, true);
            _rightInput.earlyTerminate();
          }
          if (acceptedRows > 0) {
            if (owned == null) {
              if (acceptedRows != rows) {
                data = new RowHeapDataBlock(data.asRowHeap().getRows().subList(0, acceptedRows), data.getDataSchema());
              }
              owned = ArrowBlockConverter.toArrowBlock(data, _arrowContext);
            } else if (acceptedRows != rows) {
              ArrowBlock prefix =
                  ArrowJoinOutput.copyPrefix(owned, acceptedRows, _arrowContext, this::checkTerminationAndSampleUsage);
              ArrowBlock original = owned;
              owned = prefix;
              original.release();
            }
            _buildState.holdTransferred(owned);
            owned = null;
            numRows += acceptedRows;
          }
        } finally {
          if (owned != null) {
            owned.release();
          }
        }
        checkTerminationAndSampleUsage();
      }
    } finally {
      _statMap.merge(StatKey.TIME_BUILDING_HASH_TABLE_MS, System.currentTimeMillis() - startTime);
    }
  }

  private MseBlock probe() {
    while (_eos == null) {
      if (_probe == null && !readProbe()) {
        break;
      }
      int count = 0;
      boolean unmatched = false;
      while (count < ArrowJoinOutput.MAX_ROWS_PER_BLOCK && nextPair(count)) {
        if (_probeOutputRows == _maxRowsInJoin) {
          _statMap.merge(StatKey.MAX_ROWS_IN_JOIN, _probeOutputRows);
          if (_joinOverflowMode == JoinOverFlowMode.THROW) {
            BaseJoinOperator.throwForJoinRowLimitExceeded(
                "Cannot process join, reached number of rows limit: " + _maxRowsInJoin);
          }
          _statMap.merge(StatKey.MAX_ROWS_IN_JOIN_REACHED, true);
          _leftInput.earlyTerminate();
          _eos = drain(_leftInput);
          break;
        }
        unmatched |= _rightRows[count] < 0;
        count++;
        _probeOutputRows++;
      }
      boolean finished = _eos != null || _probeRow == _probe.getNumRows();
      if (count != 0) {
        ArrowBlock result = _output.gather(_leftRows, _rightBlockIds, _rightRows, count, unmatched);
        boolean transferred = false;
        try {
          checkTerminationAndSampleUsage();
          if (finished) {
            releaseProbe();
            if (_eos != null) {
              releaseBuild();
            }
          }
          if (_arrowOutputEnabled) {
            transferred = true;
            return result;
          }
          return result.asRowHeap();
        } finally {
          if (!transferred) {
            result.release();
          }
        }
      }
      releaseProbe();
    }
    releaseBuild();
    return _eos;
  }

  private boolean readProbe() {
    while (true) {
      MseBlock block = _leftInput.nextBlock();
      if (block.isEos()) {
        _eos = (MseBlock.Eos) block;
        return false;
      }
      _probe = ArrowBlockConverter.toArrowBlock((MseBlock.Data) block, _arrowContext);
      if (_probe.getNumRows() == 0) {
        releaseProbe();
        checkTerminationAndSampleUsage();
        continue;
      }
      _probeKey = _probe.getDataBlock().getRoot().getVector(_leftKey);
      _probeRow = 0;
      _matchStart = 0;
      _matchCount = 0;
      _nextRightRow = -1;
      _probeOutputRows = 0;
      _output.setProbe(_probe);
      return true;
    }
  }

  private boolean nextPair(int outputRow) {
    while (_probeRow < _probe.getNumRows()) {
      if (_nextRightRow >= 0) {
        emitPair(outputRow, _nextRightRow);
        _nextRightRow = _table.next(_nextRightRow);
        if (_nextRightRow < 0) {
          _probeRow++;
        }
        return true;
      }
      if (_probeRow == _matchStart + _matchCount) {
        checkTerminationAndSampleUsage();
        _matchStart = _probeRow;
        _matchCount = Math.min(ArrowJoinKeys.BATCH_SIZE, _probe.getNumRows() - _probeRow);
        ArrowJoinKeys.read(_probeKey, _keyType, _probeRow, _matchCount, _keys);
        for (int i = 0; i < _matchCount; i++) {
          _matches[i] = _probeKey.isNull(_probeRow + i) ? -1 : _table.lookup(_keys[i]);
        }
      }
      int rightRow = _matches[_probeRow - _matchStart];
      if (_joinType == JoinRelType.SEMI || _joinType == JoinRelType.ANTI) {
        boolean include = (rightRow >= 0) == (_joinType == JoinRelType.SEMI);
        if (include) {
          emitPair(outputRow, -1);
        }
        _probeRow++;
        if (include) {
          return true;
        }
      } else if (rightRow >= 0) {
        emitPair(outputRow, rightRow);
        _nextRightRow = _table.next(rightRow);
        if (_nextRightRow < 0) {
          _probeRow++;
        }
        return true;
      } else {
        boolean include = _joinType == JoinRelType.LEFT;
        if (include) {
          emitPair(outputRow, -1);
        }
        _probeRow++;
        if (include) {
          return true;
        }
      }
    }
    return false;
  }

  private void emitPair(int outputRow, int rightRow) {
    _leftRows[outputRow] = _probeRow;
    _rightBlockIds[outputRow] = rightRow < 0 ? 0 : _table.blockId(rightRow);
    _rightRows[outputRow] = rightRow < 0 ? -1 : _table.rowInBlock(rightRow);
  }

  private MseBlock.Eos drain(MultiStageOperator input) {
    while (true) {
      MseBlock block = input.nextBlock();
      if (block.isEos()) {
        return (MseBlock.Eos) block;
      }
      if (block instanceof ArrowBlock) {
        ((ArrowBlock) block).release();
      }
      checkTerminationAndSampleUsage();
    }
  }

  private void releaseProbe() {
    ArrowBlock probe = _probe;
    _probe = null;
    _probeKey = null;
    if (_output != null) {
      _output.clearProbe();
    }
    if (probe != null) {
      probe.release();
    }
  }

  private void releaseBuild() {
    try {
      _buildState.releaseAll();
    } finally {
      _table = null;
      _output = null;
    }
  }

  @Override
  public void close() {
    if (_closed) {
      return;
    }
    _closed = true;
    try {
      releaseProbe();
    } finally {
      try {
        releaseBuild();
      } finally {
        super.close();
      }
    }
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return List.of(_leftInput, _rightInput);
  }

  @Override
  public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
    _statMap.merge(StatKey.ALLOCATED_MEMORY_BYTES, memoryUsedBytes);
    _statMap.merge(StatKey.GC_TIME_MS, gcTimeMs);
  }

  @Override
  public StatMap<StatKey> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  @Override
  public Type getOperatorType() {
    return Type.HASH_JOIN;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public String toExplainString() {
    return "ARROW_HASH_JOIN";
  }
}
