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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.OpChainStats;


/**
 * execution result encapsulation for {@link PipelineBreakerExecutor}.
 */
public class PipelineBreakerResult {
  private final Map<PlanNode, Integer> _nodeIdMap;
  private final Map<Integer, List<TransferableBlock>> _resultMap;
  private final TransferableBlock _errorBlock;
  private final OpChainStats _opChainStats;

  public PipelineBreakerResult(Map<PlanNode, Integer> nodeIdMap, Map<Integer, List<TransferableBlock>> resultMap,
      @Nullable TransferableBlock errorBlock, @Nullable OpChainStats opChainStats) {
    _nodeIdMap = nodeIdMap;
    _resultMap = resultMap;
    _errorBlock = errorBlock;
    _opChainStats = opChainStats;
  }

  public Map<PlanNode, Integer> getNodeIdMap() {
    return _nodeIdMap;
  }

  public Map<Integer, List<TransferableBlock>> getResultMap() {
    return _resultMap;
  }

  @Nullable
  public TransferableBlock getErrorBlock() {
    return _errorBlock;
  }

  @Nullable
  public OpChainStats getOpChainStats() {
    return _opChainStats;
  }
}
