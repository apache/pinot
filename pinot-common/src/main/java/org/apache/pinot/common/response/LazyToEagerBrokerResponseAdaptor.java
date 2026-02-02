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
package org.apache.pinot.common.response;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.utils.JsonUtils;

/// An adaptor that converts a lazy [StreamingBrokerResponse] into an eager [BrokerResponse].
///
/// These object can be build using the static [#of] method which consumes the data from the streaming response.
public class LazyToEagerBrokerResponseAdaptor implements BrokerResponse {
  private final DataSchema _dataSchema;
  private final StreamingBrokerResponse.Metainfo _metainfo;
  private final List<Object[]> _rows;
  private final ObjectNode _json;

  private LazyToEagerBrokerResponseAdaptor(DataSchema dataSchema, StreamingBrokerResponse.Metainfo metainfo,
      List<Object[]> rows) {
    _dataSchema = dataSchema;
    _metainfo = metainfo;
    _rows = rows;
    _json = metainfo.asJson();
  }

  /// Builds an eager [BrokerResponse] by consuming all the data from the given [StreamingBrokerResponse].
  ///
  /// This method may block while consuming the data. It can also consume a significant amount of memory depending
  /// on the size of the response.
  /// The method can also be interrupted while consuming the data, in which case the thread's interrupt
  /// status is set and a [RuntimeException] is thrown.
  public static BrokerResponse of(StreamingBrokerResponse streamingResponse) {
    DataSchema dataSchema = streamingResponse.getDataSchema();

    StreamingBrokerResponse.Metainfo metainfo;
    List<Object[]> rows;
    try {
      if (dataSchema == null) {
        metainfo = streamingResponse.getMetaInfo();
        rows = new ArrayList<>();
      } else {
        int width = dataSchema.size();
        rows = new ArrayList<>();
        metainfo = streamingResponse.consumeData(data -> {
          Object[] row = new Object[width];
          for (int colIdx = 0; colIdx < dataSchema.size(); colIdx++) {
            row[colIdx] = data.get(colIdx);
          }
          rows.add(row);
        });
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Thread interrupted while converting to eager broker response", e);
    }
    return new LazyToEagerBrokerResponseAdaptor(dataSchema, metainfo, rows);
  }

  @Nullable
  @Override
  public ResultTable getResultTable() {
    return new ResultTable(_dataSchema, _rows);
  }

  @Override
  public void setResultTable(@Nullable ResultTable resultTable) {
    throw new UnsupportedOperationException("Cannot set result table on early broker response");
  }

  @Override
  public int getNumRowsResultSet() {
    return _rows.size();
  }

  @Override
  public boolean isPartialResult() {
    JsonNode partialResults = _json.get("partialResults");
    return partialResults != null && partialResults.asBoolean();
  }

  @Override
  public List<QueryProcessingException> getExceptions() {
    return _metainfo.getExceptions();
  }

  @Override
  public boolean isGroupsTrimmed() {
    JsonNode groupsTrimmed = _json.get("groupsTrimmed");
    return groupsTrimmed != null && groupsTrimmed.asBoolean();
  }

  @Override
  public boolean isNumGroupsLimitReached() {
    JsonNode numGroupsLimitReached = _json.get("numGroupsLimitReached");
    return numGroupsLimitReached != null && numGroupsLimitReached.asBoolean();
  }

  @Override
  public boolean isNumGroupsWarningLimitReached() {
    JsonNode numGroupsWarningLimitReached = _json.get("numGroupsWarningLimitReached");
    return numGroupsWarningLimitReached != null
        && numGroupsWarningLimitReached.asBoolean();
  }

  @Override
  public boolean isMaxRowsInJoinReached() {
    JsonNode maxRowsInJoinReached = _json.get("maxRowsInJoinReached");
    return maxRowsInJoinReached != null && maxRowsInJoinReached.asBoolean();
  }

  @Override
  public boolean isMaxRowsInWindowReached() {
    JsonNode maxRowsInWindowReached = _json.get("maxRowsInWindowReached");
    return maxRowsInWindowReached != null && maxRowsInWindowReached.asBoolean();
  }

  @Override
  public long getTimeUsedMs() {
    JsonNode timeUsedMs = _json.get("timeUsedMs");
    return timeUsedMs != null ? timeUsedMs.asLong() : 0;
  }

  @Override
  public String getRequestId() {
    JsonNode requestId = _json.get("requestId");
    return requestId != null ? requestId.asText() : "";
  }

  @Override
  public void setRequestId(String requestId) {
    throw new UnsupportedOperationException("Cannot set request ID on early broker response");
  }

  @Override
  public String getClientRequestId() {
    JsonNode clientRequestId = _json.get("clientRequestId");
    return clientRequestId != null ? clientRequestId.asText() : "";
  }

  @Override
  public void setClientRequestId(String clientRequestId) {
    throw new UnsupportedOperationException("Cannot set client request ID on early broker response");
  }

  @Override
  public String getBrokerId() {
    JsonNode brokerId = _json.get("brokerId");
    return brokerId != null ? brokerId.asText() : "";
  }

  @Override
  public void setBrokerId(String brokerId) {
    throw new UnsupportedOperationException("Cannot set broker ID on early broker response");
  }

  @Override
  public long getNumDocsScanned() {
    JsonNode numDocsScanned = _json.get("numDocsScanned");
    return numDocsScanned != null ? numDocsScanned.asLong() : 0;
  }

  @Override
  public long getTotalDocs() {
    JsonNode totalDocs = _json.get("totalDocs");
    return totalDocs != null ? totalDocs.asLong() : 0;
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    JsonNode numEntriesScannedInFilter = _json.get("numEntriesScannedInFilter");
    return numEntriesScannedInFilter != null ? numEntriesScannedInFilter.asLong() : 0;
  }

  @Override
  public long getNumEntriesScannedPostFilter() {
    JsonNode numEntriesScannedPostFilter = _json.get("numEntriesScannedPostFilter");
    return numEntriesScannedPostFilter != null ? numEntriesScannedPostFilter.asLong() : 0;
  }

  @Override
  public int getNumServersQueried() {
    JsonNode numServersQueried = _json.get("numServersQueried");
    return numServersQueried != null ? numServersQueried.asInt() : 0;
  }

  @Override
  public int getNumServersResponded() {
    JsonNode numServersResponded = _json.get("numServersResponded");
    return numServersResponded != null ? numServersResponded.asInt() : 0;
  }

  @Override
  public long getNumSegmentsQueried() {
    JsonNode numSegmentsQueried = _json.get("numSegmentsQueried");
    return numSegmentsQueried != null ? numSegmentsQueried.asLong() : 0;
  }

  @Override
  public long getNumSegmentsProcessed() {
    JsonNode numSegmentsProcessed = _json.get("numSegmentsProcessed");
    return numSegmentsProcessed != null ? numSegmentsProcessed.asLong() : 0;
  }

  @Override
  public long getNumSegmentsMatched() {
    JsonNode numSegmentsMatched = _json.get("numSegmentsMatched");
    return numSegmentsMatched != null ? numSegmentsMatched.asLong() : 0;
  }

  @Override
  public long getNumConsumingSegmentsQueried() {
    JsonNode numConsumingSegmentsQueried = _json.get("numConsumingSegmentsQueried");
    return numConsumingSegmentsQueried != null ? numConsumingSegmentsQueried.asLong() : 0;
  }

  @Override
  public long getNumConsumingSegmentsProcessed() {
    JsonNode numConsumingSegmentsProcessed = _json.get("numConsumingSegmentsProcessed");
    return numConsumingSegmentsProcessed != null ? numConsumingSegmentsProcessed.asLong() : 0;
  }

  @Override
  public long getNumConsumingSegmentsMatched() {
    JsonNode numConsumingSegmentsMatched = _json.get("numConsumingSegmentsMatched");
    return numConsumingSegmentsMatched != null ? numConsumingSegmentsMatched.asLong() : 0;
  }

  @Override
  public long getMinConsumingFreshnessTimeMs() {
    JsonNode minConsumingFreshnessTimeMs = _json.get("minConsumingFreshnessTimeMs");
    return minConsumingFreshnessTimeMs != null ? minConsumingFreshnessTimeMs.asLong() : 0;
  }

  @Override
  public long getNumSegmentsPrunedByBroker() {
    JsonNode numSegmentsPrunedByBroker = _json.get("numSegmentsPrunedByBroker");
    return numSegmentsPrunedByBroker != null ? numSegmentsPrunedByBroker.asLong() : 0;
  }

  @Override
  public long getNumSegmentsPrunedByServer() {
    JsonNode numSegmentsPrunedByServer = _json.get("numSegmentsPrunedByServer");
    return numSegmentsPrunedByServer != null ? numSegmentsPrunedByServer.asLong() : 0;
  }

  @Override
  public long getNumSegmentsPrunedInvalid() {
    JsonNode numSegmentsPrunedInvalid = _json.get("numSegmentsPrunedInvalid");
    return numSegmentsPrunedInvalid != null ? numSegmentsPrunedInvalid.asLong() : 0;
  }

  @Override
  public long getNumSegmentsPrunedByLimit() {
    JsonNode numSegmentsPrunedByLimit = _json.get("numSegmentsPrunedByLimit");
    return numSegmentsPrunedByLimit != null ? numSegmentsPrunedByLimit.asLong() : 0;
  }

  @Override
  public long getNumSegmentsPrunedByValue() {
    JsonNode numSegmentsPrunedByValue = _json.get("numSegmentsPrunedByValue");
    return numSegmentsPrunedByValue != null ? numSegmentsPrunedByValue.asLong() : 0;
  }

  @Override
  public long getBrokerReduceTimeMs() {
    JsonNode brokerReduceTimeMs = _json.get("brokerReduceTimeMs");
    return brokerReduceTimeMs != null ? brokerReduceTimeMs.asLong() : 0;
  }

  @Override
  public long getOfflineThreadCpuTimeNs() {
    JsonNode offlineThreadCpuTimeNs = _json.get("offlineThreadCpuTimeNs");
    return offlineThreadCpuTimeNs != null ? offlineThreadCpuTimeNs.asLong() : 0;
  }

  @Override
  public long getRealtimeThreadCpuTimeNs() {
    JsonNode realtimeThreadCpuTimeNs = _json.get("realtimeThreadCpuTimeNs");
    return realtimeThreadCpuTimeNs != null ? realtimeThreadCpuTimeNs.asLong() : 0;
  }

  @Override
  public long getOfflineSystemActivitiesCpuTimeNs() {
    JsonNode offlineSystemActivitiesCpuTimeNs = _json.get("offlineSystemActivitiesCpuTimeNs");
    return offlineSystemActivitiesCpuTimeNs != null ? offlineSystemActivitiesCpuTimeNs.asLong() : 0;
  }

  @Override
  public long getRealtimeSystemActivitiesCpuTimeNs() {
    JsonNode realtimeSystemActivitiesCpuTimeNs = _json.get("realtimeSystemActivitiesCpuTimeNs");
    return realtimeSystemActivitiesCpuTimeNs != null ? realtimeSystemActivitiesCpuTimeNs.asLong() : 0;
  }

  @Override
  public long getOfflineResponseSerializationCpuTimeNs() {
    JsonNode offlineResponseSerializationCpuTimeNs = _json.get("offlineResponseSerializationCpuTimeNs");
    return offlineResponseSerializationCpuTimeNs != null ? offlineResponseSerializationCpuTimeNs.asLong() : 0;
  }

  @Override
  public long getRealtimeResponseSerializationCpuTimeNs() {
    JsonNode realtimeResponseSerializationCpuTimeNs = _json.get("realtimeResponseSerializationCpuTimeNs");
    return realtimeResponseSerializationCpuTimeNs != null ? realtimeResponseSerializationCpuTimeNs.asLong() : 0;
  }

  @Override
  public long getOfflineThreadMemAllocatedBytes() {
    JsonNode offlineThreadMemAllocatedBytes = _json.get("offlineThreadMemAllocatedBytes");
    return offlineThreadMemAllocatedBytes != null ? offlineThreadMemAllocatedBytes.asLong() : 0;
  }

  @Override
  public long getRealtimeThreadMemAllocatedBytes() {
    JsonNode realtimeThreadMemAllocatedBytes = _json.get("realtimeThreadMemAllocatedBytes");
    return realtimeThreadMemAllocatedBytes != null ? realtimeThreadMemAllocatedBytes.asLong() : 0;
  }

  @Override
  public long getOfflineResponseSerMemAllocatedBytes() {
    JsonNode offlineResponseSerMemAllocatedBytes = _json.get("offlineResponseSerMemAllocatedBytes");
    return offlineResponseSerMemAllocatedBytes != null ? offlineResponseSerMemAllocatedBytes.asLong() : 0;
  }

  @Override
  public long getRealtimeResponseSerMemAllocatedBytes() {
    JsonNode realtimeResponseSerMemAllocatedBytes = _json.get("realtimeResponseSerMemAllocatedBytes");
    return realtimeResponseSerMemAllocatedBytes != null ? realtimeResponseSerMemAllocatedBytes.asLong() : 0;
  }

  @Override
  public long getExplainPlanNumEmptyFilterSegments() {
    JsonNode explainPlanNumEmptyFilterSegments = _json.get("explainPlanNumEmptyFilterSegments");
    return explainPlanNumEmptyFilterSegments != null ? explainPlanNumEmptyFilterSegments.asLong() : 0;
  }

  @Override
  public long getExplainPlanNumMatchAllFilterSegments() {
    JsonNode explainPlanNumMatchAllFilterSegments = _json.get("explainPlanNumMatchAllFilterSegments");
    return explainPlanNumMatchAllFilterSegments != null ? explainPlanNumMatchAllFilterSegments.asLong() : 0;
  }

  @Override
  public Map<String, String> getTraceInfo() {
    JsonNode traceInfo = _json.get("traceInfo");
    if (traceInfo != null && traceInfo.isObject()) {
      try {
        return JsonUtils.jsonNodeToObject(traceInfo, Map.class);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return Map.of();
  }

  @Override
  public void setTablesQueried(Set<String> tablesQueried) {
    throw new UnsupportedOperationException("Cannot set tables queried on early broker response");
  }

  @Override
  public Set<String> getTablesQueried() {
    JsonNode tablesQueried = _json.get("tablesQueried");
    if (tablesQueried != null && tablesQueried.isArray()) {
      Set<String> result = new java.util.HashSet<>();
      for (JsonNode tableNode : tablesQueried) {
        result.add(tableNode.asText());
      }
      return result;
    }
    return Set.of();
  }

  @Override
  public void setPools(Set<Integer> pools) {
    throw new UnsupportedOperationException("Cannot set pools on early broker response");
  }

  @Override
  public Set<Integer> getPools() {
    JsonNode pools = _json.get("pools");
    if (pools != null && pools.isArray()) {
      Set<Integer> result = new java.util.HashSet<>();
      for (JsonNode poolNode : pools) {
        result.add(poolNode.asInt());
      }
      return result;
    }
    return Set.of();
  }

  @Override
  public void setRLSFiltersApplied(boolean rlsFiltersApplied) {
    throw new UnsupportedOperationException("Cannot set RLS filters applied on early broker response");
  }

  @Override
  public boolean getRLSFiltersApplied() {
    JsonNode rlsFiltersApplied = _json.get("rlsFiltersApplied");
    return rlsFiltersApplied != null && rlsFiltersApplied.asBoolean();
  }
}
