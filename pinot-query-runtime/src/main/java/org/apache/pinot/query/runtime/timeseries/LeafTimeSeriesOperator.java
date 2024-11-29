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
import java.util.concurrent.ExecutorService;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.TimeSeriesResultsBlock;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.logger.ServerQueryLogger;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;


public class LeafTimeSeriesOperator extends BaseTimeSeriesOperator {
  private final ServerQueryRequest _request;
  private final QueryExecutor _queryExecutor;
  private final ExecutorService _executorService;
  private final ServerQueryLogger _queryLogger;

  public LeafTimeSeriesOperator(ServerQueryRequest serverQueryRequest, QueryExecutor queryExecutor,
      ExecutorService executorService) {
    super(Collections.emptyList());
    _request = serverQueryRequest;
    _queryExecutor = queryExecutor;
    _executorService = executorService;
    _queryLogger = ServerQueryLogger.getInstance();
  }

  @Override
  public TimeSeriesBlock getNextBlock() {
    Preconditions.checkNotNull(_queryExecutor, "Leaf time series operator has not been initialized");
    InstanceResponseBlock instanceResponseBlock = _queryExecutor.execute(_request, _executorService);
    assert instanceResponseBlock.getResultsBlock() instanceof TimeSeriesResultsBlock;
    _queryLogger.logQuery(_request, instanceResponseBlock, "TimeSeries");
    if (MapUtils.isNotEmpty(instanceResponseBlock.getExceptions())) {
      // TODO: Return error in the TimeSeriesBlock instead?
      String oneException = instanceResponseBlock.getExceptions().values().iterator().next();
      throw new RuntimeException(oneException);
    }
    TimeSeriesResultsBlock timeSeriesResultsBlock =
        ((TimeSeriesResultsBlock) instanceResponseBlock.getResultsBlock());
    return timeSeriesResultsBlock.getTimeSeriesBuilderBlock().build();
  }

  @Override
  public String getExplainName() {
    return "TIME_SERIES_LEAF_STAGE_OPERATOR";
  }
}
