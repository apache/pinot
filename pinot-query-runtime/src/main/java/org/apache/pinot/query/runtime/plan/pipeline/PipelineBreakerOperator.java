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
package org.apache.pinot.query.runtime.plan.pipeline;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PipelineBreakerOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipelineBreakerOperator.class);
  private static final String EXPLAIN_NAME = "PIPELINE_BREAKER";

  private final Map<Integer, MultiStageOperator> _workerMap;

  private Map<Integer, List<MseBlock>> _resultMap;
  private ErrorMseBlock _errorBlock;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  public PipelineBreakerOperator(OpChainExecutionContext context, Map<Integer, MultiStageOperator> workerMap) {
    super(context);
    _workerMap = workerMap;
    _resultMap = new HashMap<>();
    for (int workerKey : workerMap.keySet()) {
      _resultMap.put(workerKey, new ArrayList<>());
    }
  }

  @Override
  public void registerExecution(long time, int numRows) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public Type getOperatorType() {
    return Type.PIPELINE_BREAKER;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  public Map<Integer, List<MseBlock>> getResultMap() {
    return _resultMap;
  }

  @Nullable
  public ErrorMseBlock getErrorBlock() {
    return _errorBlock;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected MseBlock getNextBlock() {
    if (_errorBlock != null) {
      return _errorBlock;
    }
    // NOTE: Put an empty list for each worker in case there is no data block returned from that worker
    if (_workerMap.size() == 1) {
      Map.Entry<Integer, MultiStageOperator> entry = _workerMap.entrySet().iterator().next();
      List<MseBlock> dataBlocks = new ArrayList<>();
      _resultMap = Collections.singletonMap(entry.getKey(), dataBlocks);
      Operator<MseBlock> operator = entry.getValue();
      MseBlock block = operator.nextBlock();
      while (block.isData()) {
        dataBlocks.add(block);
        block = operator.nextBlock();
      }
      if (block.isError()) {
        _errorBlock = ((ErrorMseBlock) block);
        return block;
      }
    } else {
      _resultMap = new HashMap<>();
      for (int workerKey : _workerMap.keySet()) {
        _resultMap.put(workerKey, new ArrayList<>());
      }
      // Keep polling from every operator in round-robin fashion
      Queue<Map.Entry<Integer, MultiStageOperator>> entries = new ArrayDeque<>(_workerMap.entrySet());
      while (!entries.isEmpty()) {
        Map.Entry<Integer, MultiStageOperator> entry = entries.poll();
        MseBlock block = entry.getValue().nextBlock();
        if (block.isError()) {
          _errorBlock = ((ErrorMseBlock) block);
          return block;
        }
        if (block.isData()) {
          _resultMap.get(entry.getKey()).add(block);
          entries.offer(entry); // add it again to the queue to keep polling
        } else {
          // do nothing on success
          assert block.isSuccess();
        }
      }
    }
    return SuccessMseBlock.INSTANCE;
  }

  @Override
  protected MultiStageQueryStats calculateUpstreamStats() {
    return _workerMap.values().stream()
        .map(MultiStageOperator::calculateStats)
        .reduce((s1, s2) -> {
          s1.mergeUpstream(s2);
          return s1;
        })
        .orElseThrow(() -> new IllegalStateException("No stats found for pipeline breaker"));
  }

  @Override
  protected StatMap<?> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  @Override
  public void close() {
    for (MultiStageOperator operator : _workerMap.values()) {
      operator.close();
    }
  }

  public enum StatKey implements StatMap.Key {
    EXECUTION_TIME_MS(StatMap.Type.LONG),
    EMITTED_ROWS(StatMap.Type.LONG);
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
