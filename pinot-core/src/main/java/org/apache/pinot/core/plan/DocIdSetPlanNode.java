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

import javax.annotation.Nullable;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;


public class DocIdSetPlanNode implements PlanNode {
  public static final int MAX_DOC_PER_CALL = 10000;

  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;
  private final int _maxDocPerCall;
  private final BaseFilterOperator _filterOperator;

  public DocIdSetPlanNode(IndexSegment indexSegment, QueryContext queryContext, int maxDocPerCall,
      @Nullable BaseFilterOperator filterOperator) {
    assert maxDocPerCall > 0 && maxDocPerCall <= MAX_DOC_PER_CALL;

    _indexSegment = indexSegment;
    _queryContext = queryContext;
    _maxDocPerCall = maxDocPerCall;
    _filterOperator = filterOperator;
  }

  @Override
  public DocIdSetOperator run() {
    return new DocIdSetOperator(
        _filterOperator != null ? _filterOperator : new FilterPlanNode(_indexSegment, _queryContext).run(),
        _maxDocPerCall);
  }
}
