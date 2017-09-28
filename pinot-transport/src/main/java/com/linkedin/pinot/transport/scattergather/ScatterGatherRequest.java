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
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.SegmentIdSet;
import java.util.Map;


/**
 *
 * All clients of Request-Routing layer must implement and pass the RoutingRequest instance
 * for request-routing
 */
public interface ScatterGatherRequest {

  /**
   * Return the candidate set of servers that hosts each segment-set.
   * The List of services are expected to be ordered so that replica-selection strategy can be
   * applied to them to select one Service among the list for each segment.
   *
   * @return SegmentSet to Request map.
   */
  Map<ServerInstance, SegmentIdSet> getSegmentsServicesMap();

  /**
   * Return the requests that will be sent to the service which is hosting a group of interested segments
   * @param service Service to which segments will be sent.
   * @param querySegments Segments to be queried in the service identified by Service.
   * @return byte[] request to be sent
   */
  byte[] getRequestForService(ServerInstance service, SegmentIdSet querySegments);

  /**
   * Request Id for tracing purpose
   * @return
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
