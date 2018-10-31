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

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.context.TimerContext;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Class to encapsulate the query request and query processing context within the server. Goal is to make most of the
 * information available to lower levels of code in the server for logging, tracking etc.
 */
public class ServerQueryRequest {
  private final InstanceRequest _instanceRequest;
  private final ServerMetrics _serverMetrics;
  private final BrokerRequest _brokerRequest;
  private final FilterQueryTree _filterQueryTree;

  // Timing information for different phases of query execution
  private final TimerContext _timerContext;

  private int _segmentCountAfterPruning = -1;

  public ServerQueryRequest(@Nonnull InstanceRequest instanceRequest, @Nonnull ServerMetrics serverMetrics) {
    _instanceRequest = instanceRequest;
    _serverMetrics = serverMetrics;
    _brokerRequest = instanceRequest.getQuery();
    _filterQueryTree = RequestUtils.generateFilterQueryTree(_brokerRequest);
    _timerContext = new TimerContext(_brokerRequest, serverMetrics);
  }

  /**
   * Get the instance request received from broker.
   */
  @Nonnull
  public InstanceRequest getInstanceRequest() {
    return _instanceRequest;
  }

  /**
   * Get the server metrics.
   */
  @Nonnull
  public ServerMetrics getServerMetrics() {
    return _serverMetrics;
  }

  /**
   * Get the broker request.
   */
  @Nonnull
  public BrokerRequest getBrokerRequest() {
    return _brokerRequest;
  }

  /**
   * Get the filter query tree for the server request.
   */
  @Nullable
  public FilterQueryTree getFilterQueryTree() {
    return _filterQueryTree;
  }

  /**
   * Get the timer context.
   */
  @Nonnull
  public TimerContext getTimerContext() {
    return _timerContext;
  }

  /**
   * Get the table name to be queried.
   */
  @Nonnull
  public String getTableName() {
    return _brokerRequest.getQuerySource().getTableName();
  }

  /**
   * Get the segment count after pruning.
   */
  public int getSegmentCountAfterPruning() {
    return _segmentCountAfterPruning;
  }

  /**
   * Set the segment count after pruning.
   */
  public void setSegmentCountAfterPruning(int segmentCountAfterPruning) {
    _segmentCountAfterPruning = segmentCountAfterPruning;
  }
}
