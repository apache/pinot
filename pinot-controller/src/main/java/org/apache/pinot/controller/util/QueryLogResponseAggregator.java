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
package org.apache.pinot.controller.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.QueryLogSystemTableUtils;
import org.apache.pinot.spi.exception.QueryErrorCode;


/**
 * Helper to merge {@link BrokerResponseNative} objects produced by broker-local system.query_log tables.
 */
public final class QueryLogResponseAggregator {
  private QueryLogResponseAggregator() {
  }

  public static BrokerResponseNative aggregate(List<BrokerResponseNative> responses) {
    BrokerResponseNative aggregated = new BrokerResponseNative();
    if (responses == null || responses.isEmpty()) {
      aggregated.setTablesQueried(Collections.singleton(QueryLogSystemTableUtils.FULL_TABLE_NAME));
      return aggregated;
    }

    List<QueryProcessingException> exceptions = new ArrayList<>();
    List<Object[]> rows = new ArrayList<>();
    DataSchema resultSchema = null;
    long totalDocsScanned = 0;
    long totalEntriesScannedInFilter = 0;
    long totalEntriesScannedPostFilter = 0;
    long maxTimeUsedMs = 0;
    int respondedCount = 0;

    for (BrokerResponseNative response : responses) {
      if (response == null) {
        continue;
      }
      respondedCount++;
      ResultTable resultTable = response.getResultTable();
      if (resultTable != null) {
        DataSchema schema = resultTable.getDataSchema();
        if (resultSchema == null) {
          resultSchema = schema;
        } else if (!resultSchema.equals(schema)) {
          exceptions.add(new QueryProcessingException(QueryErrorCode.INTERNAL,
              "Mismatched schemas returned from brokers for system.query_log"));
          continue;
        }
        rows.addAll(resultTable.getRows());
      }
      if (response.getExceptionsSize() > 0) {
        exceptions.addAll(response.getExceptions());
      }
      totalDocsScanned += response.getNumDocsScanned();
      totalEntriesScannedInFilter += response.getNumEntriesScannedInFilter();
      totalEntriesScannedPostFilter += response.getNumEntriesScannedPostFilter();
      maxTimeUsedMs = Math.max(maxTimeUsedMs, response.getTimeUsedMs());
    }

    if (resultSchema != null) {
      aggregated.setResultTable(new ResultTable(resultSchema, rows));
    }
    aggregated.setNumDocsScanned(totalDocsScanned);
    aggregated.setNumEntriesScannedInFilter(totalEntriesScannedInFilter);
    aggregated.setNumEntriesScannedPostFilter(totalEntriesScannedPostFilter);
    aggregated.setNumServersResponded(respondedCount);
    aggregated.setTimeUsedMs(maxTimeUsedMs);
    aggregated.setExceptions(exceptions);
    aggregated.setTablesQueried(Collections.singleton(QueryLogSystemTableUtils.FULL_TABLE_NAME));
    return aggregated;
  }
}
