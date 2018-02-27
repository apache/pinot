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
package com.linkedin.pinot.transport.scattergather;

import com.linkedin.pinot.common.request.BrokerRequest;
import java.util.List;
import java.util.Map;


/**
 *
 * All clients of Request-Routing layer must implement and pass the RoutingRequest instance
 * for request-routing
 */
public interface ScatterGatherRequest {

  /**
   * Get the routing table (map from server to list of segments).
   *
   * @return Map from server to list of segments
   */
  Map<String, List<String>> getRoutingTable();

  /**
   * Get the request to be sent to the server.
   *
   * @param segments List of segments to be queried
   * @return Request to be sent
   */
  byte[] getRequestForService(List<String> segments);

  /**
   * Get the request id for tracing purpose.
   *
   * @return Request id
   */
  long getRequestId();

  /**
   * Return timeout in milliseconds for the request. If the timeout gets elapsed, the request will be cancelled.
   * Timeout with negative values are considered infinite timeout.
   */
  long getRequestTimeoutMs();

  /**
   * @return the BrokerRequest object used for this scatterGather
   */
  BrokerRequest getBrokerRequest();
}
