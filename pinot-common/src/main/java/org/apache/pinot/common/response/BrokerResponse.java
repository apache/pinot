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
   * Set the total thread cpu time used against realtime table in request handling, into the broker response.
   */
  void setRealtimeThreadCpuTimeNs(long realtimeThreadCpuTimeNs);

  /**
   * Set the total thread cpu time used against offline table in request handling, into the broker response.
   */
  void setOfflineThreadCpuTimeNs(long offlineThreadCpuTimeNs);

  /**
   * Set invalid columns in query.
   */
  void setInvalidColumnsInQuery(String invalidColumnsInQuery);

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
   * Get the list of exceptions
   */
  List<QueryProcessingException> getProcessingExceptions();

  /**
   * Get the total thread cpu time used against realtime table in request handling, into the broker response.
   */
  long getRealtimeThreadCpuTimeNs();

  /**
   * Get the total thread cpu time used against offline table in request handling, into the broker response.
   */
  long getOfflineThreadCpuTimeNs();

  /**
   * Get invalid columns in query.
   */
  String getInvalidColumnsInQuery();
}
