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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.logger.ServerQueryLogger;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
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
    _queryLogger.logQuery(_request, instanceResponseBlock, "TimeSeries");
    if (MapUtils.isNotEmpty(instanceResponseBlock.getExceptions())) {
      String message = instanceResponseBlock.getExceptions().values().iterator().next();
      throw new RuntimeException("Error running time-series query: " + message);
    }
    if (instanceResponseBlock.getResultsBlock() instanceof GroupByResultsBlock) {
      return handleGroupByResultsBlock((GroupByResultsBlock) instanceResponseBlock.getResultsBlock());
    } else if (instanceResponseBlock.getResultsBlock() instanceof AggregationResultsBlock) {
      return handleAggregationResultsBlock((AggregationResultsBlock) instanceResponseBlock.getResultsBlock());
    } else if (instanceResponseBlock.getResultsBlock() == null) {
      throw new IllegalStateException("Found null results block in time-series query");
    } else {
      throw new UnsupportedOperationException(String.format("Unknown results block: %s",
          instanceResponseBlock.getResultsBlock().getClass().getName()));
    }
  }

  @Override
  public String getExplainName() {
    return "TIME_SERIES_LEAF_STAGE_OPERATOR";
  }

  private TimeSeriesBlock handleGroupByResultsBlock(GroupByResultsBlock groupByResultsBlock) {
    if (groupByResultsBlock.getNumRows() == 0) {
      return new TimeSeriesBlock(_context.getInitialTimeBuckets(), new HashMap<>());
    }
    if (groupByResultsBlock.isNumGroupsLimitReached()) {
      throw new IllegalStateException(String.format("Series limit reached. Number of series: %s",
          groupByResultsBlock.getNumRows()));
    }
    Map<Long, List<TimeSeries>> timeSeriesMap = new HashMap<>(groupByResultsBlock.getNumRows());
    List<String> tagNames = getTagNamesFromDataSchema(Objects.requireNonNull(groupByResultsBlock.getDataSchema(),
        "DataSchema is null in leaf stage of time-series query"));
    Iterator<Record> recordIterator = groupByResultsBlock.getTable().iterator();
    while (recordIterator.hasNext()) {
      Record record = recordIterator.next();
      Object[] recordValues = record.getValues();
      Object[] tagValues = new Object[recordValues.length - 1];
      for (int index = 0; index + 1 < recordValues.length; index++) {
        tagValues[index] = recordValues[index] == null ? "null" : recordValues[index].toString();
      }
      Double[] doubleValues = (Double[]) recordValues[recordValues.length - 1];
      long seriesHash = TimeSeries.hash(tagValues);
      List<TimeSeries> timeSeriesList = new ArrayList<>(1);
      timeSeriesList.add(new TimeSeries(Long.toString(seriesHash), null, _context.getInitialTimeBuckets(),
          doubleValues, tagNames, tagValues));
      timeSeriesMap.put(seriesHash, timeSeriesList);
    }
    return new TimeSeriesBlock(_context.getInitialTimeBuckets(), timeSeriesMap);
  }

  private TimeSeriesBlock handleAggregationResultsBlock(AggregationResultsBlock aggregationResultsBlock) {
    if (aggregationResultsBlock.getResults() == null) {
      return new TimeSeriesBlock(_context.getInitialTimeBuckets(), new HashMap<>());
    }
    Double[] doubleValues = (Double[]) aggregationResultsBlock.getResults().get(0);
    long seriesHash = TimeSeries.hash(new Object[0]);
    List<TimeSeries> timeSeriesList = new ArrayList<>(1);
    timeSeriesList.add(new TimeSeries(Long.toString(seriesHash), null, _context.getInitialTimeBuckets(),
        doubleValues, Collections.emptyList(), new Object[0]));
    Map<Long, List<TimeSeries>> timeSeriesMap = new HashMap<>();
    timeSeriesMap.put(seriesHash, timeSeriesList);
    return new TimeSeriesBlock(_context.getInitialTimeBuckets(), timeSeriesMap);
  }

  private List<String> getTagNamesFromDataSchema(DataSchema dataSchema) {
    String[] columnNames = dataSchema.getColumnNames();
    int numTags = columnNames.length - 1;
    List<String> tagNames = new ArrayList<>(numTags);
    for (int index = 0; index < numTags; index++) {
      tagNames.add(columnNames[index]);
    }
    return tagNames;
  }
}
