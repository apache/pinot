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
package org.apache.pinot.query.runtime.timeseries;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.timeseries.TimeSeriesOperatorUtils;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.logger.ServerQueryLogger;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;


public class LeafTimeSeriesOperator extends BaseTimeSeriesOperator {
  private final TimeSeriesExecutionContext _context;
  private final ServerQueryRequest _request;
  private final QueryExecutor _queryExecutor;
  private final ExecutorService _executorService;
  private final ServerQueryLogger _queryLogger;

  public LeafTimeSeriesOperator(TimeSeriesExecutionContext context, ServerQueryRequest serverQueryRequest,
      QueryExecutor queryExecutor, ExecutorService executorService) {
    super(Collections.emptyList());
    _context = context;
    _request = serverQueryRequest;
    _queryExecutor = queryExecutor;
    _executorService = executorService;
    _queryLogger = ServerQueryLogger.getInstance();
  }

  @Override
  public TimeSeriesBlock getNextBlock() {
    Preconditions.checkNotNull(_queryExecutor, "Leaf time series operator has not been initialized");
    InstanceResponseBlock instanceResponseBlock = _queryExecutor.execute(_request, _executorService);
    if (MapUtils.isNotEmpty(instanceResponseBlock.getExceptions())) {
      return buildErrorBlock(instanceResponseBlock, "Error running time-series query");
    }
    assert instanceResponseBlock.getResultsBlock() instanceof GroupByResultsBlock;
    _queryLogger.logQuery(_request, instanceResponseBlock, "TimeSeries");

    if (instanceResponseBlock.getResultsBlock() instanceof GroupByResultsBlock) {
      return TimeSeriesOperatorUtils.buildTimeSeriesBlock(_context.getInitialTimeBuckets(),
          (GroupByResultsBlock) instanceResponseBlock.getResultsBlock(), instanceResponseBlock.getResponseMetadata());
    } else if (instanceResponseBlock.getResultsBlock() instanceof AggregationResultsBlock) {
      return TimeSeriesOperatorUtils.buildTimeSeriesBlock(_context.getInitialTimeBuckets(),
          (AggregationResultsBlock) instanceResponseBlock.getResultsBlock(),
          instanceResponseBlock.getResponseMetadata());
    } else if (instanceResponseBlock.getResultsBlock() == null) {
      return buildErrorBlock(instanceResponseBlock, "Found null results block in time-series query");
    } else {
      return buildErrorBlock(instanceResponseBlock,
          String.format("Unknown results block: %s", instanceResponseBlock.getResultsBlock().getClass().getName()));
    }
  }

  private TimeSeriesBlock buildErrorBlock(InstanceResponseBlock instanceResponseBlock, String baseMessage) {
    TimeSeriesBlock errorBlock = new TimeSeriesBlock(_context.getInitialTimeBuckets(), new java.util.HashMap<>(),
        instanceResponseBlock.getResponseMetadata());
    if (MapUtils.isNotEmpty(instanceResponseBlock.getExceptions())) {
      for (Map.Entry<Integer, String> entry : instanceResponseBlock.getExceptions().entrySet()) {
        QueryException qe = new QueryException(QueryErrorCode.fromErrorCode(entry.getKey()), entry.getValue());
        errorBlock.addToExceptions(qe);
      }
    } else {
      QueryException qe = new QueryException(QueryErrorCode.QUERY_EXECUTION, baseMessage);
      errorBlock.addToExceptions(qe);
    }
    return errorBlock;
  }

  @Override
  public String getExplainName() {
    return "TIME_SERIES_LEAF_STAGE_OPERATOR";
  }
}
