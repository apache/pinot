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
package org.apache.pinot.core.plan;

import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * A common wrapper for the segment-level plan node.
 * NOTE: This is only used if <code>pinot.server.query.executor.enable.prefetch</code> is true
 * This PlanNode differs from the other PlanNodes in the following way:
 * This PlanNode does not invoke a <code>run</code> on the childOperator in its run method.
 * Instead, it passes the childPlanNode as is, to the {@link AcquireReleaseColumnsSegmentOperator},
 * and it is that operator's responsibility to run the childPlanNode and get the childOperator before execution.
 * The reason this is done is the planners access segment buffers,
 * and we need to acquire the segment before any access is made to the buffers.
 */
public class AcquireReleaseColumnsSegmentPlanNode implements PlanNode {

  private final PlanNode _childPlanNode;
  private final IndexSegment _indexSegment;
  private final FetchContext _fetchContext;

  public AcquireReleaseColumnsSegmentPlanNode(PlanNode childPlanNode, IndexSegment indexSegment,
      FetchContext fetchContext) {
    _childPlanNode = childPlanNode;
    _indexSegment = indexSegment;
    _fetchContext = fetchContext;
  }

  /**
   * Doesn't run the childPlan,
   * but instead just creates a {@link AcquireReleaseColumnsSegmentOperator} and passes the plan to it
   */
  @Override
  public AcquireReleaseColumnsSegmentOperator run() {
    return new AcquireReleaseColumnsSegmentOperator(_childPlanNode, _indexSegment, _fetchContext);
  }
}
