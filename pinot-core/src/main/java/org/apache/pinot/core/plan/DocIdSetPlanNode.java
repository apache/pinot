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

import com.google.common.base.Preconditions;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;


public class DocIdSetPlanNode implements PlanNode {
  public static int MAX_DOC_PER_CALL = 10000;

  private final FilterPlanNode _filterPlanNode;
  private final int _maxDocPerCall;

  public DocIdSetPlanNode(IndexSegment indexSegment, QueryContext queryContext, int maxDocPerCall) {
    Preconditions.checkState(maxDocPerCall > 0 && maxDocPerCall <= MAX_DOC_PER_CALL);
    _filterPlanNode = new FilterPlanNode(indexSegment, queryContext);
    _maxDocPerCall = maxDocPerCall;
  }

  @Override
  public DocIdSetOperator run() {
    return new DocIdSetOperator(_filterPlanNode.run(), _maxDocPerCall);
  }
}
