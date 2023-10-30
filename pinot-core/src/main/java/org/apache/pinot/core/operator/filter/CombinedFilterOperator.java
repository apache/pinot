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
package org.apache.pinot.core.operator.filter;

import java.util.Arrays;
import java.util.List;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.docidsets.AndDocIdSet;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.trace.Tracing;


/**
 * A combined filter operator consisting of one main filter operator and one sub filter operator. The result block is
 * the AND result of the main and sub filter.
 */
public class CombinedFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_COMBINED";

  private final BaseFilterOperator _mainFilterOperator;
  private final BaseFilterOperator _subFilterOperator;
  private final QueryContext _queryContext;

  public CombinedFilterOperator(BaseFilterOperator mainFilterOperator, BaseFilterOperator subFilterOperator,
      QueryContext queryContext) {
    // This filter operator does not support AND/OR/NOT operations.
    super(0, queryContext.isNullHandlingEnabled());
    _queryContext = queryContext;
    assert !mainFilterOperator.isResultEmpty() && !mainFilterOperator.isResultMatchingAll()
        && !subFilterOperator.isResultEmpty() && !subFilterOperator.isResultMatchingAll();
    _mainFilterOperator = mainFilterOperator;
    _subFilterOperator = subFilterOperator;
  }

  @Override
  public List<BaseFilterOperator> getChildOperators() {
    return Arrays.asList(_mainFilterOperator, _subFilterOperator);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected BlockDocIdSet getTrues() {
    Tracing.activeRecording().setNumChildren(2);
    BlockDocIdSet mainFilterDocIdSet = _mainFilterOperator.nextBlock().getNonScanFilterBLockDocIdSet();
    BlockDocIdSet subFilterDocIdSet = _subFilterOperator.nextBlock().getBlockDocIdSet();
    return new AndDocIdSet(Arrays.asList(mainFilterDocIdSet, subFilterDocIdSet), _queryContext.getQueryOptions());
  }
}
