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

import java.util.List;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;


/**
 * Interface for broker response.
 */
public interface BrokerResponse {

  /**
   * Set exceptions caught during request handling, into the broker response.
   */
  void setExceptions(List<ProcessingException> exceptions);

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

  /**
   * Set the total time used in request handling, into the broker response.
   */
  void setTimeUsedMs(long timeUsedMs);

  /**
   * Set the total number of rows in result set
   */
  void setNumRowsResultSet(int numRowsResultSet);

  /**
   * Convert the broker response to JSON String.
   */
  String toJsonString()
      throws Exception;

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
   * Get total number of documents within the table hit.
   */
  long getTotalDocs();

  /**
   * Returns whether the number of groups limit has been reached.
   */
  boolean isNumGroupsLimitReached();

  /**
   * Get number of exceptions recorded in the response.
   */
  int getExceptionsSize();

  /**
   * set the result table.
   * @param resultTable result table to be set.
   */
  void setResultTable(ResultTable resultTable);

  /**
   * Get the result table.
   * @return result table.
   */
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
   * Set the total thread cpu time used against offline table in request handling, into the broker response.
   */
  void setOfflineThreadCpuTimeNs(long offlineThreadCpuTimeNs);

  /**
   * Get the thread cpu time used against offline table in request handling, from the broker response.
   */
  long getOfflineThreadCpuTimeNs();

  /**
   * Get the thread cpu time used against realtime table in request handling, from the broker response.
   */
  long getRealtimeThreadCpuTimeNs();

  /**
   * Set the total thread cpu time used against realtime table in request handling, into the broker response.
   */
  void setRealtimeThreadCpuTimeNs(long realtimeThreadCpuTimeNs);

  /**
   * Get the system activities cpu time used against offline table in request handling, from the broker response.
   */
  long getOfflineSystemActivitiesCpuTimeNs();

  /**
   * Set the system activities cpu time used against offline table in request handling, into the broker response.
   */
  void setOfflineSystemActivitiesCpuTimeNs(long offlineSystemActivitiesCpuTimeNs);

  /**
   * Get the system activities cpu time used against realtime table in request handling, from the broker response.
   */
  long getRealtimeSystemActivitiesCpuTimeNs();

  /**
   * Set the system activities cpu time used against realtime table in request handling, into the broker response.
   */
  void setRealtimeSystemActivitiesCpuTimeNs(long realtimeSystemActivitiesCpuTimeNs);

  /**
   * Get the response serialization cpu time used against offline table in request handling, from the broker response.
   */
  long getOfflineResponseSerializationCpuTimeNs();

  /**
   * Set the response serialization cpu time used against offline table in request handling, into the broker response.
   */
  void setOfflineResponseSerializationCpuTimeNs(long offlineResponseSerializationCpuTimeNs);

  /**
   * Get the response serialization cpu time used against realtime table in request handling, from the broker response.
   */
  long getRealtimeResponseSerializationCpuTimeNs();

  /**
   * Set the response serialization cpu time used against realtime table in request handling, into the broker response.
   */
  void setRealtimeResponseSerializationCpuTimeNs(long realtimeResponseSerializationCpuTimeNs);

  /**
   * Get the total cpu time(thread cpu time + system activities cpu time + response serialization cpu time) used
   * against offline table in request handling, from the broker response.
   */
  long getOfflineTotalCpuTimeNs();

  /**
   * Set the total cpu time(thread cpu time + system activities cpu time + response serialization cpu time) used
   * against offline table in request handling, into the broker response.
   */
  void setOfflineTotalCpuTimeNs(long offlineTotalCpuTimeNs);

  /**
   * Get the total cpu time(thread cpu time + system activities cpu time + response serialization cpu time) used
   * against realtime table in request handling, from the broker response.
   */
  long getRealtimeTotalCpuTimeNs();

  /**
   * Set the total cpu time(thread cpu time + system activities cpu time + response serialization cpu time) used
   * against realtime table in request handling, into the broker response.
   */
  void setRealtimeTotalCpuTimeNs(long realtimeTotalCpuTimeNs);

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
   * Set the total number of segments pruned on the Server side
   */
  void setNumSegmentsPrunedByServer(long numSegmentsPrunedByServer);

  /**
   * Get the total number of segments pruned due to invalid data or schema.
   *
   * This value is always lower or equal than {@link #getNumSegmentsPrunedByServer()}
   */
  long getNumSegmentsPrunedInvalid();

  /**
   * Set the total number of segments pruned due to invalid data or schema.
   *
   * This value is always lower or equal than {@link #getNumSegmentsPrunedByServer()}
   */
  void setNumSegmentsPrunedInvalid(long numSegmentsPrunedInvalid);

  /**
   * Get the total number of segments pruned by applying the limit optimization.
   *
   * This value is always lower or equal than {@link #getNumSegmentsPrunedByServer()}
   */
  long getNumSegmentsPrunedByLimit();

  /**
   * Set the total number of segments pruned by applying the limit optimization.
   *
   * This value is always lower or equal than {@link #getNumSegmentsPrunedByServer()}
   */
  void setNumSegmentsPrunedByLimit(long numSegmentsPrunedByLimit);

  /**
   * Get the total number of segments pruned applying value optimizations, like bloom filters.
   *
   * This value is always lower or equal than {@link #getNumSegmentsPrunedByServer()}
   */
  long getNumSegmentsPrunedByValue();

  /**
   * Set the total number of segments pruned applying value optimizations, like bloom filters.
   *
   * This value is always lower or equal than {@link #getNumSegmentsPrunedByServer()}
   */
  void setNumSegmentsPrunedByValue(long numSegmentsPrunedByValue);

  /**
   * Get the total number of segments with an EmptyFilterOperator when Explain Plan is called
   */
  long getExplainPlanNumEmptyFilterSegments();

  /**
   * Set the total number of segments with an EmptyFilterOperator when Explain Plan is called
   */
  void setExplainPlanNumEmptyFilterSegments(long explainPlanNumEmptyFilterSegments);

  /**
   * Get the total number of segments with a MatchAllFilterOperator when Explain Plan is called
   */
  long getExplainPlanNumMatchAllFilterSegments();

  /**
   * Set the total number of segments with a MatchAllFilterOperator when Explain Plan is called
   */
  void setExplainPlanNumMatchAllFilterSegments(long explainPlanNumMatchAllFilterSegments);

  /**
   * get request ID for the query
   */
  String getRequestId();

  /**
   * set request ID generated by broker
   */
  void setRequestId(String requestId);
}
