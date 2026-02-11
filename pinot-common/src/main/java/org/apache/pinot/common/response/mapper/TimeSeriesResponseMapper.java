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
package org.apache.pinot.common.response.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;


public class TimeSeriesResponseMapper {

  private static final String TS_COLUMN = "ts";
  private static final String VALUES_COLUMN = "values";
  private static final String NAME_COLUMN = "__name__";

  private TimeSeriesResponseMapper() {
  }

  /**
   * Creates a BrokerResponseNativeV2 from a TimeSeriesBlock.
   * This method converts the time series data into a format compatible with the broker response.
   */
  public static BrokerResponse toBrokerResponse(TimeSeriesBlock timeSeriesBlock) {
    BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
    if (timeSeriesBlock == null) {
      throw new IllegalArgumentException("timeSeriesBlock must not be null");
    }
    if (timeSeriesBlock.getTimeBuckets() == null) {
      throw new UnsupportedOperationException("Non-bucketed series block not supported yet");
    }
    // Convert TimeSeriesBlock to ResultTable format
    DataSchema dataSchema = deriveDataSchemaFromTimeSeriesBlock(timeSeriesBlock);
    List<Object[]> rows = deriveRowsFromTimeSeriesBlock(timeSeriesBlock, dataSchema.getColumnNames());

    ResultTable resultTable = new ResultTable(dataSchema, rows);
    brokerResponse.setResultTable(resultTable);
    for (QueryException exception : timeSeriesBlock.getExceptions()) {
      brokerResponse.addException(QueryProcessingException.fromQueryException(exception));
    }
    setStats(brokerResponse, timeSeriesBlock.getMetadata());
    return brokerResponse;
  }

  public static BrokerResponse toBrokerResponse(QueryException e) {
    BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
    brokerResponse.addException(QueryProcessingException.fromQueryException(e));
    return brokerResponse;
  }

  private static DataSchema deriveDataSchemaFromTimeSeriesBlock(TimeSeriesBlock timeSeriesBlock) {
    List<String> columnNames = new ArrayList<>(List.of(TS_COLUMN, VALUES_COLUMN, NAME_COLUMN));
    List<DataSchema.ColumnDataType> columnTypes = new ArrayList<>(List.of(
      DataSchema.ColumnDataType.LONG_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY,
      DataSchema.ColumnDataType.STRING));

    // Add tag columns if any series exist
    if (!timeSeriesBlock.getSeriesMap().isEmpty()) {
      // Get the first series to determine tag columns
      TimeSeries firstSeries = timeSeriesBlock.getSeriesMap().values().iterator().next().get(0);
      firstSeries.getTagKeyValuesAsMap().forEach((key, value) -> {
        if (!columnNames.contains(key)) {
          columnNames.add(key);
          columnTypes.add(DataSchema.ColumnDataType.STRING);
        }
      });
    }

    return new DataSchema(columnNames.toArray(new String[0]),
      columnTypes.toArray(new DataSchema.ColumnDataType[0]));
  }

  private static List<Object[]> deriveRowsFromTimeSeriesBlock(TimeSeriesBlock timeSeriesBlock,
    String[] columnNames) {
    List<Object[]> rows = new ArrayList<>();
    if (columnNames.length == 0) {
      return rows;
    }

    Long[] timeValues = timeSeriesBlock.getTimeBuckets().getTimeBuckets();
    for (var listOfTimeSeries : timeSeriesBlock.getSeriesMap().values()) {
      for (TimeSeries timeSeries : listOfTimeSeries) {
        Object[] row = new Object[columnNames.length];
        int index = 0;

        for (String columnName : columnNames) {
          if (TS_COLUMN.equals(columnName)) {
            row[index] = timeValues;
          } else if (VALUES_COLUMN.equals(columnName)) {
            Double[] values = new Double[timeValues.length];
            for (int i = 0; i < timeValues.length; i++) {
              Object nullableValue = timeSeries.getDoubleValues()[i];
              values[i] = nullableValue == null ? null : Double.valueOf(String.valueOf(nullableValue));
            }
            row[index] = values;
          } else if (NAME_COLUMN.equals(columnName)) {
            row[index] = timeSeries.getTagsSerialized();
          } else {
            row[index] = timeSeries.getTagKeyValuesAsMap().getOrDefault(columnName, null);
          }
          index++;
        }
        rows.add(row);
      }
    }
    return rows;
  }

  private static void setStats(BrokerResponseNativeV2 brokerResponse, Map<String, String> metadata) {
    if (metadata == null || metadata.isEmpty()) {
      return;
    }

    StatMap<BrokerResponseNativeV2.StatKey> map = new StatMap<>(BrokerResponseNativeV2.StatKey.class);
    map.merge(BrokerResponseNativeV2.StatKey.NUM_DOCS_SCANNED, getLongMetadataValue(metadata,
        DataTable.MetadataKey.NUM_DOCS_SCANNED));
    map.merge(BrokerResponseNativeV2.StatKey.NUM_ENTRIES_SCANNED_IN_FILTER,
        getLongMetadataValue(metadata, DataTable.MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER));
    map.merge(BrokerResponseNativeV2.StatKey.NUM_ENTRIES_SCANNED_POST_FILTER,
        getLongMetadataValue(metadata, DataTable.MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER));
    map.merge(BrokerResponseNativeV2.StatKey.NUM_SEGMENTS_QUERIED,
        getIntMetadataValue(metadata, DataTable.MetadataKey.NUM_SEGMENTS_QUERIED));
    map.merge(BrokerResponseNativeV2.StatKey.NUM_SEGMENTS_PROCESSED,
        getIntMetadataValue(metadata, DataTable.MetadataKey.NUM_SEGMENTS_PROCESSED));
    map.merge(BrokerResponseNativeV2.StatKey.NUM_SEGMENTS_MATCHED,
        getIntMetadataValue(metadata, DataTable.MetadataKey.NUM_SEGMENTS_MATCHED));
    map.merge(BrokerResponseNativeV2.StatKey.NUM_CONSUMING_SEGMENTS_MATCHED,
        getIntMetadataValue(metadata, DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_MATCHED));
    map.merge(BrokerResponseNativeV2.StatKey.NUM_CONSUMING_SEGMENTS_PROCESSED,
        getIntMetadataValue(metadata, DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_PROCESSED));
    map.merge(BrokerResponseNativeV2.StatKey.NUM_CONSUMING_SEGMENTS_QUERIED,
        getIntMetadataValue(metadata, DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_QUERIED));
    map.merge(BrokerResponseNativeV2.StatKey.TOTAL_DOCS,
        getLongMetadataValue(metadata, DataTable.MetadataKey.TOTAL_DOCS));
    map.merge(BrokerResponseNativeV2.StatKey.MIN_CONSUMING_FRESHNESS_TIME_MS,
        getLongMetadataValue(metadata, DataTable.MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS));
    map.merge(BrokerResponseNativeV2.StatKey.NUM_SEGMENTS_PRUNED_BY_SERVER,
        getIntMetadataValue(metadata, DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_SERVER));
    map.merge(BrokerResponseNativeV2.StatKey.NUM_SEGMENTS_PRUNED_INVALID,
        getIntMetadataValue(metadata, DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_INVALID));
    map.merge(BrokerResponseNativeV2.StatKey.NUM_SEGMENTS_PRUNED_BY_LIMIT,
        getIntMetadataValue(metadata, DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_LIMIT));
    map.merge(BrokerResponseNativeV2.StatKey.NUM_SEGMENTS_PRUNED_BY_VALUE,
        getIntMetadataValue(metadata, DataTable.MetadataKey.NUM_SEGMENTS_PRUNED_BY_VALUE));
    brokerResponse.addBrokerStats(map);
    brokerResponse.setTimeUsedMs(getLongMetadataValue(metadata, DataTable.MetadataKey.TIME_USED_MS));
  }

  public static void setStatsInRequestContext(RequestContext requestContext, Map<String, String> metadata) {
    if (metadata == null || metadata.isEmpty() || requestContext == null) {
      return;
    }
    requestContext.setNumDocsScanned(getLongMetadataValue(metadata,
        DataTable.MetadataKey.NUM_DOCS_SCANNED));
    requestContext.setNumEntriesScannedInFilter(getLongMetadataValue(metadata,
        DataTable.MetadataKey.NUM_ENTRIES_SCANNED_IN_FILTER));
    requestContext.setNumEntriesScannedPostFilter(getLongMetadataValue(metadata,
        DataTable.MetadataKey.NUM_ENTRIES_SCANNED_POST_FILTER));
    requestContext.setTotalDocs(getLongMetadataValue(metadata,
        DataTable.MetadataKey.TOTAL_DOCS));
    requestContext.setNumSegmentsQueried(getIntMetadataValue(metadata,
        DataTable.MetadataKey.NUM_SEGMENTS_QUERIED));
    requestContext.setNumSegmentsProcessed(getIntMetadataValue(metadata,
        DataTable.MetadataKey.NUM_SEGMENTS_PROCESSED));
    requestContext.setNumSegmentsMatched(getIntMetadataValue(metadata,
        DataTable.MetadataKey.NUM_SEGMENTS_MATCHED));
    requestContext.setNumConsumingSegmentsMatched(getIntMetadataValue(metadata,
        DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_MATCHED));
    requestContext.setNumConsumingSegmentsProcessed(getIntMetadataValue(metadata,
        DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_PROCESSED));
    requestContext.setNumConsumingSegmentsQueried(getIntMetadataValue(metadata,
        DataTable.MetadataKey.NUM_CONSUMING_SEGMENTS_QUERIED));
  }

  private static long getLongMetadataValue(Map<String, String> metadata, DataTable.MetadataKey key) {
    return Long.parseLong(metadata.getOrDefault(key.getName(), "0"));
  }

  private static int getIntMetadataValue(Map<String, String> metadata, DataTable.MetadataKey key) {
    return Integer.parseInt(metadata.getOrDefault(key.getName(), "0"));
  }
}
