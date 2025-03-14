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
package org.apache.pinot.core.operator.timeseries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.BaseTimeSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;


public class TimeSeriesOperatorUtils {
  private TimeSeriesOperatorUtils() {
  }

  public static TimeSeriesBlock buildTimeSeriesBlock(TimeBuckets timeBuckets,
      GroupByResultsBlock groupByResultsBlock) {
    if (groupByResultsBlock.getNumRows() == 0) {
      return new TimeSeriesBlock(timeBuckets, new HashMap<>());
    }
    // TODO: Check isNumGroupsLimitReached and isNumGroupsWarningLimitReached, and propagate it somehow to the caller.
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
      BaseTimeSeriesBuilder seriesBuilder = (BaseTimeSeriesBuilder) recordValues[recordValues.length - 1];
      long seriesHash = TimeSeries.hash(tagValues);
      List<TimeSeries> timeSeriesList = new ArrayList<>(1);
      timeSeriesList.add(seriesBuilder.buildWithTagOverrides(tagNames, tagValues));
      timeSeriesMap.put(seriesHash, timeSeriesList);
    }
    return new TimeSeriesBlock(timeBuckets, timeSeriesMap);
  }

  public static TimeSeriesBlock buildTimeSeriesBlock(TimeBuckets timeBuckets,
      AggregationResultsBlock aggregationResultsBlock) {
    if (aggregationResultsBlock.getResults() == null) {
      return new TimeSeriesBlock(timeBuckets, new HashMap<>());
    }
    BaseTimeSeriesBuilder seriesBuilder = (BaseTimeSeriesBuilder) aggregationResultsBlock.getResults().get(0);
    long seriesHash = TimeSeries.hash(new Object[0]);
    List<TimeSeries> timeSeriesList = new ArrayList<>(1);
    timeSeriesList.add(seriesBuilder.buildWithTagOverrides(Collections.emptyList(), new Object[]{}));
    Map<Long, List<TimeSeries>> timeSeriesMap = new HashMap<>();
    timeSeriesMap.put(seriesHash, timeSeriesList);
    return new TimeSeriesBlock(timeBuckets, timeSeriesMap);
  }

  private static List<String> getTagNamesFromDataSchema(DataSchema dataSchema) {
    String[] columnNames = dataSchema.getColumnNames();
    int numTags = columnNames.length - 1;
    List<String> tagNames = new ArrayList<>(numTags);
    for (int index = 0; index < numTags; index++) {
      tagNames.add(columnNames[index]);
    }
    return tagNames;
  }
}
