/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.common.query;

import com.google.common.util.concurrent.ListenableFuture;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.context.TimerContext;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Class to encapsulate the query request and query processing
 * context within the server. Goal is to make most of the information
 * available to lower levels of code in the server for logging, tracking
 * etc.
 *
 */
public class ServerQueryRequest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerQueryRequest.class);

  private final InstanceRequest instanceRequest;
  private final FilterQueryTree filterQueryTree;
  // Future to track result of query execution
  private ListenableFuture<DataTable> resultFuture;
  // Timing information for different phases of query execution
  private final TimerContext timerContext;

  private final ServerMetrics serverMetrics;
  private int segmentCountAfterPruning = -1;
  private List<String> segmentsAfterPruning;

  public ServerQueryRequest(InstanceRequest request, ServerMetrics serverMetrics) {
    this.instanceRequest = request;
    this.serverMetrics = serverMetrics;
    BrokerRequest brokerRequest = (request == null) ? null : request.getQuery();
    filterQueryTree = (brokerRequest == null) ? null : RequestUtils.generateFilterQueryTree(brokerRequest);
    timerContext = new TimerContext(brokerRequest, serverMetrics);
  }

  public @Nullable String getBrokerId() {
    return instanceRequest.getBrokerId();
  }

  /**
   * Convenient method to read table name for query
   * from instance request
   * @return
   */
  public String getTableName() {
    return instanceRequest.getQuery().getQuerySource().getTableName();
  }

  /**
   * Convenient method to read broker request object
   * @return
   */
  public BrokerRequest getBrokerRequest() {
    return instanceRequest.getQuery();
  }

  /**
   * Returns the filter query tree for the server request.
   */
  public FilterQueryTree getFilterQueryTree() {
    return filterQueryTree;
  }

  /**
   * Get the instance request received from broker
   * @return
   */
  public InstanceRequest getInstanceRequest() {
    return instanceRequest;
  }

  /**
   * Get server metrics
   * @return
   */
  public ServerMetrics getServerMetrics() {
    return serverMetrics;
  }

  /**
   *
   * @return ListenableFuture that will used to track the result
   * of query execution. This can be null if not set
   */
  public ListenableFuture<DataTable> getResultFuture() {
    return resultFuture;
  }

  public void setResultFuture(ListenableFuture<DataTable> resultFuture) {
    this.resultFuture = resultFuture;
  }

  public TimerContext getTimerContext() {
    return timerContext;
  }

  public void setSegmentsAfterPruning(List<String> segmentsAfterPruning)
  {
    this.segmentsAfterPruning = segmentsAfterPruning;
  }
  public void setSegmentCountAfterPruning(int segmentCountAfterPruning) {
    this.segmentCountAfterPruning = segmentCountAfterPruning;
  }

  public int getSegmentCountAfterPruning() {
    return segmentCountAfterPruning;
  }

  public List<String> getSegmntsAfterPruning()
  {
    return segmentsAfterPruning;
  }
}
