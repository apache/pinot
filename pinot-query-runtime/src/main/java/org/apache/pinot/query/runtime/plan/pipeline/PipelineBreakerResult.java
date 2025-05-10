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
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;


/**
 * execution result encapsulation for {@link PipelineBreakerExecutor}.
 */
public class PipelineBreakerResult {
  private final Map<PlanNode, Integer> _nodeIdMap;
  private final Map<Integer, List<MseBlock>> _resultMap;
  private final ErrorMseBlock _errorBlock;
  private final MultiStageQueryStats _multiStageQueryStats;

  public PipelineBreakerResult(Map<PlanNode, Integer> nodeIdMap, Map<Integer, List<MseBlock>> resultMap,
      @Nullable ErrorMseBlock errorBlock, @Nullable MultiStageQueryStats multiStageQueryStats) {
    _nodeIdMap = nodeIdMap;
    _resultMap = resultMap;
    _errorBlock = errorBlock;
    _multiStageQueryStats = multiStageQueryStats;
  }

  public Map<PlanNode, Integer> getNodeIdMap() {
    return _nodeIdMap;
  }

  public Map<Integer, List<MseBlock>> getResultMap() {
    return _resultMap;
  }

  @Nullable
  public ErrorMseBlock getErrorBlock() {
    return _errorBlock;
  }

  @Nullable
  public MultiStageQueryStats getStageQueryStats() {
    return _multiStageQueryStats;
  }
}
