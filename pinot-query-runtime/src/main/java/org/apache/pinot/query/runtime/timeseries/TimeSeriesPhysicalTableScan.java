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

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;


public class TimeSeriesPhysicalTableScan extends BaseTimeSeriesPlanNode {
  private final TimeSeriesExecutionContext _context;
  private final ServerQueryRequest _request;
  private final QueryExecutor _queryExecutor;
  private final ExecutorService _executorService;

  public TimeSeriesPhysicalTableScan(
      TimeSeriesExecutionContext context,
      String id,
      ServerQueryRequest serverQueryRequest,
      QueryExecutor queryExecutor,
      ExecutorService executorService) {
    super(id, Collections.emptyList());
    _context = context;
    _request = serverQueryRequest;
    _queryExecutor = queryExecutor;
    _executorService = executorService;
  }

  public ServerQueryRequest getServerQueryRequest() {
    return _request;
  }

  public QueryExecutor getQueryExecutor() {
    return _queryExecutor;
  }

  public ExecutorService getExecutorService() {
    return _executorService;
  }

  public String getKlass() {
    return TimeSeriesPhysicalTableScan.class.getName();
  }

  @Override
  public String getExplainName() {
    return "PHYSICAL_TABLE_SCAN";
  }

  @Override
  public BaseTimeSeriesOperator run() {
    return new LeafTimeSeriesOperator(_context, _request, _queryExecutor, _executorService);
  }
}
