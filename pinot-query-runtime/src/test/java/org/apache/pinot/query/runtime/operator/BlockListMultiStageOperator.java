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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// A [MultiStageOperator] whose [#nextBlock] returns the content of a list of [MseBlock]s, useful for tests.
public class BlockListMultiStageOperator extends MultiStageOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockListMultiStageOperator.class);
  private final Iterator<? extends MseBlock> _blocks;
  private final StatMap<LiteralValueOperator.StatKey> _statMap = new StatMap<>(LiteralValueOperator.StatKey.class);

  public BlockListMultiStageOperator(OpChainExecutionContext context, Iterable<? extends MseBlock> blocks) {
    super(context);
    _blocks = blocks.iterator();
  }

  public BlockListMultiStageOperator(OpChainExecutionContext context, MseBlock... blocks) {
    super(context);
    _blocks = Arrays.asList(blocks).iterator();
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public Type getOperatorType() {
    return Type.LITERAL;
  }

  @Override
  public void registerExecution(long time, int numRows) {
    _statMap.merge(LiteralValueOperator.StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(LiteralValueOperator.StatKey.EMITTED_ROWS, numRows);
  }

  @Override
  protected MseBlock getNextBlock()
      throws Exception {
    if (_blocks.hasNext()) {
      return _blocks.next();
    } else {
      return SuccessMseBlock.INSTANCE;
    }
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return List.of();
  }

  @Override
  protected StatMap<?> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  @Nullable
  @Override
  public String toExplainString() {
    return "BLOCK_LIST";
  }

  public static class Builder {
    private final OpChainExecutionContext _context;
    private final DataSchema _schema;
    private final List<MseBlock> _blocks = new ArrayList<>();
    private final List<Object[]> _tempRows = new ArrayList<>();
    private boolean _spy;

    public Builder(DataSchema schema) {
      _context = OperatorTestUtil.getTracingContext();
      _schema = schema;
    }

    public Builder(OpChainExecutionContext context, DataSchema schema) {
      _context = context;
      _schema = schema;
    }

    public Builder addBlock(Object[]... rows) {
      Preconditions.checkState(_tempRows.isEmpty(), "Cannot call addBlock while adding rows");
      _blocks.add(OperatorTestUtil.block(_schema, rows));
      return this;
    }

    public Builder addBlock(MseBlock block) {
      Preconditions.checkState(_tempRows.isEmpty(), "Cannot call addBlock while adding rows");
      _blocks.add(block);
      return this;
    }

    public Builder addRow(Object... rows) {
      _tempRows.add(rows);
      return this;
    }

    public Builder finishBlock() {
      Preconditions.checkState(!_tempRows.isEmpty(), "Cannot call finishBlock without adding rows");
      _blocks.add(OperatorTestUtil.block(_schema, _tempRows.toArray(new Object[0][])));
      _tempRows.clear();
      return this;
    }

    /// If called, the operator will be wrapped with [Mockito#spy]
    public Builder spied() {
      _spy = true;
      return this;
    }

    public BlockListMultiStageOperator buildWithError(ErrorMseBlock error) {
      if (!_tempRows.isEmpty()) {
        finishBlock();
      }
      _blocks.add(error);
      BlockListMultiStageOperator result = new BlockListMultiStageOperator(_context, _blocks);
      return _spy ? Mockito.spy(result) : result;
    }

    public BlockListMultiStageOperator buildWithEos() {
      if (!_tempRows.isEmpty()) {
        finishBlock();
      }
      BlockListMultiStageOperator result = new BlockListMultiStageOperator(_context, _blocks);
      return _spy ? Mockito.spy(result) : result;
    }
  }
}
