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

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Interface for broker response.
 */
public interface BrokerResponse {

  /**
   * Set exceptions caught during request handling, into the broker response.
   */
  void setExceptions(List<ProcessingException> exceptions);

  void addToExceptions(QueryProcessingException processingException);

  /**
   * Set the number of servers got queried by the broker.
   *
   * @param numServersQueried number of servers got queried.
   */
  void setNumServersQueried(int numServersQueried);

  /**
   * Set the number of servers responded to the broker.
   *
   * @param numServersResponded number of servers responded.
   */
  void setNumServersResponded(int numServersResponded);

  long getTimeUsedMs();

  /**
   * Set the total time used in request handling, into the broker response.
   */
  void setTimeUsedMs(long timeUsedMs);

  /**
   * Convert the broker response to JSON String.
   */
  default String toJsonString()
      throws IOException {
    return JsonUtils.objectToString(this);
  }

  /**
   * Returns the number of servers queried.
   */
  int getNumServersQueried();

  /**
   * Returns the number of servers responded.
   */
  int getNumServersResponded();

  /**
   * Get number of documents scanned while processing the query.
   */
  long getNumDocsScanned();

  /**
   * Get number of entries scanned in filter phase while processing the query.
   */
  long getNumEntriesScannedInFilter();

  /**
   * Get number of entries scanned post filter phase while processing the query.
   */
  long getNumEntriesScannedPostFilter();

  /**
   * Get the number of segments queried by the broker after broker side pruning
   */
  long getNumSegmentsQueried();

  /**
   * Get the number of segments processed by server after server side pruning
   */
  long getNumSegmentsProcessed();

  /**
   * Get number of segments that had at least one matching document
   */
  long getNumSegmentsMatched();

  /**
   * Get number of consuming segments that were queried.
   */
  long getNumConsumingSegmentsQueried();

  /**
   * Get number of consuming segments processed by server after server side pruning
   */
  long getNumConsumingSegmentsProcessed();

  /**
   * Get number of consuming segments that had at least one matching document
   */
  long getNumConsumingSegmentsMatched();

  /**
   * Get the minimum freshness timestamp across consuming segments that were queried
   */
  long getMinConsumingFreshnessTimeMs();

  /**
   * Get the max number of rows seen by a single operator in the query processing chain.
   * <p>
   * In single-stage this value is equal to {@link #getNumEntriesScannedPostFilter()} because single-stage operators
   * cannot generate more rows than the number of rows received. This is not the case in multi-stage operators, where
   * operators like join or union can emit more rows than the ones received on each of its children operators.
   */
  default long getMaxRowsInOperator() {
    return getNumEntriesScannedPostFilter();
  }

  /**
   * Get total number of documents within the table hit.
   */
  long getTotalDocs();

  /**
   * Returns whether the number of groups limit has been reached.
   */
  boolean isNumGroupsLimitReached();

  /**
   * Returns whether the limit for max rows in join has been reached.
   */
  boolean isMaxRowsInJoinReached();

  /**
   * Get number of exceptions recorded in the response.
   */
  int getExceptionsSize();

  /**
   * set the result table.
   * @param resultTable result table to be set.
   */
  void setResultTable(@Nullable ResultTable resultTable);

  /**
   * Get the result table.
   * @return result table.
   */
  @Nullable
  ResultTable getResultTable();

  /**
   * Get the list of exceptions
   */
  List<QueryProcessingException> getProcessingExceptions();

  /**
   * Get the total number of rows in result set
   */
  int getNumRowsResultSet();

  /**
   * Get the thread cpu time used against offline table in request handling, from the broker response.
   */
  long getOfflineThreadCpuTimeNs();

  /**
   * Get the thread cpu time used against realtime table in request handling, from the broker response.
   */
  long getRealtimeThreadCpuTimeNs();

  /**
   * Get the system activities cpu time used against offline table in request handling, from the broker response.
   */
  long getOfflineSystemActivitiesCpuTimeNs();

  /**
   * Get the system activities cpu time used against realtime table in request handling, from the broker response.
   */
  long getRealtimeSystemActivitiesCpuTimeNs();

  /**
   * Get the response serialization cpu time used against offline table in request handling, from the broker response.
   */
  long getOfflineResponseSerializationCpuTimeNs();

  /**
   * Get the response serialization cpu time used against realtime table in request handling, from the broker response.
   */
  long getRealtimeResponseSerializationCpuTimeNs();

  /**
   * Get the total cpu time(thread cpu time + system activities cpu time + response serialization cpu time) used
   * against offline table in request handling, from the broker response.
   */
  default long getOfflineTotalCpuTimeNs() {
    return getOfflineThreadCpuTimeNs() + getOfflineSystemActivitiesCpuTimeNs()
        + getOfflineResponseSerializationCpuTimeNs();
  }

  /**
   * Get the total cpu time(thread cpu time + system activities cpu time + response serialization cpu time) used
   * against realtime table in request handling, from the broker response.
   */
  default long getRealtimeTotalCpuTimeNs() {
    return getRealtimeThreadCpuTimeNs() + getRealtimeSystemActivitiesCpuTimeNs()
        + getRealtimeResponseSerializationCpuTimeNs();
  }

  /**
   * Get the total number of segments pruned on the Broker side
   */
  long getNumSegmentsPrunedByBroker();

  /**
   * Set the total number of segments pruned on the Broker side
   */
  void setNumSegmentsPrunedByBroker(long numSegmentsPrunedByBroker);

  /**
   * Get the total number of segments pruned on the Server side
   */
  long getNumSegmentsPrunedByServer();

  /**
   * Get the total number of segments pruned due to invalid data or schema.
   *
   * This value is always lower or equal than {@link #getNumSegmentsPrunedByServer()}
   */
  long getNumSegmentsPrunedInvalid();

  /**
   * Get the total number of segments pruned by applying the limit optimization.
   *
   * This value is always lower or equal than {@link #getNumSegmentsPrunedByServer()}
   */
  long getNumSegmentsPrunedByLimit();

  /**
   * Get the total number of segments pruned applying value optimizations, like bloom filters.
   *
   * This value is always lower or equal than {@link #getNumSegmentsPrunedByServer()}
   */
  long getNumSegmentsPrunedByValue();

  /**
   * Get the total number of segments with an EmptyFilterOperator when Explain Plan is called
   */
  long getExplainPlanNumEmptyFilterSegments();

  /**
   * Get the total number of segments with a MatchAllFilterOperator when Explain Plan is called
   */
  long getExplainPlanNumMatchAllFilterSegments();

  /**
   * get request ID for the query
   */
  String getRequestId();

  /**
   * set request ID generated by broker
   */
  void setRequestId(String requestId);

  /**
   * get broker ID of the processing broker
   */
  String getBrokerId();

  /**
   * set broker ID of the processing broker
   */
  void setBrokerId(String requestId);

  long getBrokerReduceTimeMs();

  void setBrokerReduceTimeMs(long brokerReduceTimeMs);

  boolean isPartialResult();
}
