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

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.docidsets.MVScanDocIdSet;
import org.apache.pinot.core.operator.docidsets.SVScanDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;


public class ScanBasedFilterOperator extends BaseColumnFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_FULL_SCAN";

  private final PredicateEvaluator _predicateEvaluator;
  private final int _batchSize;

  public ScanBasedFilterOperator(QueryContext queryContext, PredicateEvaluator predicateEvaluator,
      DataSource dataSource, int numDocs) {
    this(queryContext, predicateEvaluator, dataSource, numDocs, BlockDocIdIterator.OPTIMAL_ITERATOR_BATCH_SIZE);
  }

  public ScanBasedFilterOperator(QueryContext queryContext, PredicateEvaluator predicateEvaluator,
      DataSource dataSource, int numDocs, int batchSize) {
    super(queryContext, dataSource, numDocs);
    _predicateEvaluator = predicateEvaluator;
    Preconditions.checkState(_dataSource.getForwardIndex() != null,
        "Forward index disabled for column: %s, scan based filtering not supported!",
        _dataSource.getDataSourceMetadata().getFieldSpec().getName());
    _batchSize = batchSize;
  }

  @Override
  protected BlockDocIdSet getNextBlockWithoutNullHandling() {
    DataSourceMetadata dataSourceMetadata = _dataSource.getDataSourceMetadata();
    if (dataSourceMetadata.isSingleValue()) {
      return new SVScanDocIdSet(_predicateEvaluator, _dataSource, _numDocs, false, _batchSize);
    } else {
      return new MVScanDocIdSet(_predicateEvaluator, _dataSource, _numDocs);
    }
  }


  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder =
        new StringBuilder(EXPLAIN_NAME).append("(operator:").append(_predicateEvaluator.getPredicateType());
    stringBuilder.append(",predicate:").append(_predicateEvaluator.getPredicate().toString());
    return stringBuilder.append(')').toString();
  }

  /**
   * Returns the metadata of the data source associated with the scan filter.
   * TODO: Replace this with a priority method for all filter operators
   */
  public DataSourceMetadata getDataSourceMetadata() {
    return _dataSource.getDataSourceMetadata();
  }
}
