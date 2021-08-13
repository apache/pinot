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

import java.util.Set;
import org.apache.pinot.core.operator.SegmentOperator;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * A common wrapper for the segment-level plan node.
 */
public class SegmentPlanNode implements PlanNode {

  private final PlanNode _childPlanNode;
  private final IndexSegment _indexSegment;
  private final Set<String> _columns;

  public SegmentPlanNode(PlanNode childPlanNode, IndexSegment indexSegment, Set<String> columns) {
    _childPlanNode = childPlanNode;
    _indexSegment = indexSegment;
    _columns = columns;
  }

  @Override
  public SegmentOperator run() {
    return new SegmentOperator(_childPlanNode.run(), _indexSegment, _columns);
  }
}
