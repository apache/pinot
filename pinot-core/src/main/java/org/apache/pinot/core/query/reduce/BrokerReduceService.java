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
package org.apache.pinot.core.query.reduce;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.QueryOptions;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * The <code>BrokerReduceService</code> class provides service to reduce data tables gathered from multiple servers
 * to {@link BrokerResponseNative}.
 */
@ThreadSafe
public class BrokerReduceService {

  public BrokerResponseNative reduceOnDataTable(BrokerRequest brokerRequest,
      Map<ServerRoutingInstance, DataTable> dataTableMap, @Nullable BrokerMetrics brokerMetrics) {
    if (dataTableMap.size() == 0) {
      // Empty response.
      return BrokerResponseNative.empty();
    }

    BrokerResponseNative brokerResponseNative = new BrokerResponseNative();
    List<QueryProcessingException> processingExceptions = brokerResponseNative.getProcessingExceptions();
    long numDocsScanned = 0L;
    long numEntriesScannedInFilter = 0L;
    long numEntriesScannedPostFilter = 0L;
    long numSegmentsQueried = 0L;
    long numSegmentsProcessed = 0L;
    long numSegmentsMatched = 0L;
    long numConsumingSegmentsProcessed = 0L;
    long minConsumingFreshnessTimeMs = Long.MAX_VALUE;
    long numTotalDocs = 0L;
    boolean numGroupsLimitReached = false;

    // Cache a data schema from data tables (try to cache one with data rows associated with it).
    DataSchema cachedDataSchema = null;

    // Process server response metadata.
    Iterator<Map.Entry<ServerRoutingInstance, DataTable>> iterator = dataTableMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<ServerRoutingInstance, DataTable> entry = iterator.next();
      DataTable dataTable = entry.getValue();
      Map<String, String> metadata = dataTable.getMetadata();

      // Reduce on trace info.
      if (brokerRequest.isEnableTrace()) {
        brokerResponseNative.getTraceInfo()
            .put(entry.getKey().getHostname(), metadata.get(DataTable.TRACE_INFO_METADATA_KEY));
      }

      // Reduce on exceptions.
      for (String key : metadata.keySet()) {
        if (key.startsWith(DataTable.EXCEPTION_METADATA_KEY)) {
          processingExceptions.add(new QueryProcessingException(Integer.parseInt(key.substring(9)), metadata.get(key)));
        }
      }

      // Reduce on execution statistics.
      String numDocsScannedString = metadata.get(DataTable.NUM_DOCS_SCANNED_METADATA_KEY);
      if (numDocsScannedString != null) {
        numDocsScanned += Long.parseLong(numDocsScannedString);
      }
      String numEntriesScannedInFilterString = metadata.get(DataTable.NUM_ENTRIES_SCANNED_IN_FILTER_METADATA_KEY);
      if (numEntriesScannedInFilterString != null) {
        numEntriesScannedInFilter += Long.parseLong(numEntriesScannedInFilterString);
      }
      String numEntriesScannedPostFilterString = metadata.get(DataTable.NUM_ENTRIES_SCANNED_POST_FILTER_METADATA_KEY);
      if (numEntriesScannedPostFilterString != null) {
        numEntriesScannedPostFilter += Long.parseLong(numEntriesScannedPostFilterString);
      }
      String numSegmentsQueriedString = metadata.get(DataTable.NUM_SEGMENTS_QUERIED);
      if (numSegmentsQueriedString != null) {
        numSegmentsQueried += Long.parseLong(numSegmentsQueriedString);
      }

      String numSegmentsProcessedString = metadata.get(DataTable.NUM_SEGMENTS_PROCESSED);
      if (numSegmentsProcessedString != null) {
        numSegmentsProcessed += Long.parseLong(numSegmentsProcessedString);
      }
      String numSegmentsMatchedString = metadata.get(DataTable.NUM_SEGMENTS_MATCHED);
      if (numSegmentsMatchedString != null) {
        numSegmentsMatched += Long.parseLong(numSegmentsMatchedString);
      }

      String numConsumingString = metadata.get(DataTable.NUM_CONSUMING_SEGMENTS_PROCESSED);
      if (numConsumingString != null) {
        numConsumingSegmentsProcessed += Long.parseLong(numConsumingString);
      }

      String minConsumingFreshnessTimeMsString = metadata.get(DataTable.MIN_CONSUMING_FRESHNESS_TIME_MS);
      if (minConsumingFreshnessTimeMsString != null) {
        minConsumingFreshnessTimeMs =
            Math.min(Long.parseLong(minConsumingFreshnessTimeMsString), minConsumingFreshnessTimeMs);
      }

      String numTotalDocsString = metadata.get(DataTable.TOTAL_DOCS_METADATA_KEY);
      if (numTotalDocsString != null) {
        numTotalDocs += Long.parseLong(numTotalDocsString);
      }
      numGroupsLimitReached |= Boolean.parseBoolean(metadata.get(DataTable.NUM_GROUPS_LIMIT_REACHED_KEY));

      // After processing the metadata, remove data tables without data rows inside.
      DataSchema dataSchema = dataTable.getDataSchema();
      if (dataSchema == null) {
        iterator.remove();
      } else {
        // Try to cache a data table with data rows inside, or cache one with data schema inside.
        if (dataTable.getNumberOfRows() == 0) {
          if (cachedDataSchema == null) {
            cachedDataSchema = dataSchema;
          }
          iterator.remove();
        } else {
          cachedDataSchema = dataSchema;
        }
      }
    }

    // Set execution statistics.
    brokerResponseNative.setNumDocsScanned(numDocsScanned);
    brokerResponseNative.setNumEntriesScannedInFilter(numEntriesScannedInFilter);
    brokerResponseNative.setNumEntriesScannedPostFilter(numEntriesScannedPostFilter);
    brokerResponseNative.setNumSegmentsQueried(numSegmentsQueried);
    brokerResponseNative.setNumSegmentsProcessed(numSegmentsProcessed);
    brokerResponseNative.setNumSegmentsMatched(numSegmentsMatched);
    brokerResponseNative.setTotalDocs(numTotalDocs);
    brokerResponseNative.setNumGroupsLimitReached(numGroupsLimitReached);
    if (numConsumingSegmentsProcessed > 0) {
      brokerResponseNative.setNumConsumingSegmentsQueried(numConsumingSegmentsProcessed);
      brokerResponseNative.setMinConsumingFreshnessTimeMs(minConsumingFreshnessTimeMs);
    }

    // Update broker metrics.
    String tableName = brokerRequest.getQuerySource().getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    if (brokerMetrics != null) {
      brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.DOCUMENTS_SCANNED, numDocsScanned);
      brokerMetrics
          .addMeteredTableValue(rawTableName, BrokerMeter.ENTRIES_SCANNED_IN_FILTER, numEntriesScannedInFilter);
      brokerMetrics
          .addMeteredTableValue(rawTableName, BrokerMeter.ENTRIES_SCANNED_POST_FILTER, numEntriesScannedPostFilter);

      if (numConsumingSegmentsProcessed > 0 && minConsumingFreshnessTimeMs > 0) {
        brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.FRESHNESS_LAG_MS,
            System.currentTimeMillis() - minConsumingFreshnessTimeMs, TimeUnit.MILLISECONDS);
      }
    }

    // NOTE: When there is no cached data schema, that means all servers encountered exception. In such case, return the
    //       response with metadata only.
    if (cachedDataSchema == null) {
      return brokerResponseNative;
    }

    DataTableReducer dataTableReducer = ResultReducerFactory.getResultReducer(brokerRequest);
    dataTableReducer
        .reduceAndSetResults(tableName, cachedDataSchema, dataTableMap, brokerResponseNative, brokerMetrics);
    updateAliasToSchemaName(brokerRequest, brokerResponseNative);
    return brokerResponseNative;
  }

  private static void updateAliasToSchemaName(BrokerRequest brokerRequest, BrokerResponseNative brokerResponseNative) {
    if (brokerRequest.getPinotQuery() == null) {
      return;
    }
    QueryOptions queryOptions = new QueryOptions(brokerRequest.getQueryOptions());
    if (!queryOptions.isResponseFormatSQL()) {
      return;
    }
    DataSchema dataSchema = brokerResponseNative.getResultTable().getDataSchema();
    List<Expression> selectList = brokerRequest.getPinotQuery().getSelectList();
    String[] columnNames = dataSchema.getColumnNames();
    int selectListSize = selectList.size();
    // For query like `SELECT *`, we skip alias update.
    if (columnNames.length != selectListSize) {
      return;
    }
    for (int i = 0; i < selectListSize; i++) {
      Function selectFunc = selectList.get(i).getFunctionCall();
      if (selectFunc != null && selectFunc.getOperator().equalsIgnoreCase(SqlKind.AS.toString())) {
        columnNames[i] = selectFunc.getOperands().get(1).getIdentifier().getName();
      }
    }
  }
}
