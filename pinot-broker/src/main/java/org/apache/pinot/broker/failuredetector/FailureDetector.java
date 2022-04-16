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
package org.apache.pinot.broker.failuredetector;

import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.core.transport.QueryResponse;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * The {@code FailureDetector} detects unhealthy servers based on the query responses. When it detects an unhealthy
 * server, it will notify the listener via a callback, and schedule a delay to retry the unhealthy server later via
 * another callback.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@ThreadSafe
public interface FailureDetector {

  /**
   * Listener for the failure detector.
   */
  interface Listener {

    /**
     * Notifies the listener of an unhealthy server.
     */
    void notifyUnhealthyServer(String instanceId, FailureDetector failureDetector);

    /**
     * Notifies the listener to retry a previous unhealthy server.
     */
    void retryUnhealthyServer(String instanceId, FailureDetector failureDetector);

    /**
     * Notifies the listener of a previous unhealthy server turning healthy.
     */
    void notifyHealthyServer(String instanceId, FailureDetector failureDetector);
  }

  /**
   * Initializes the failure detector.
   */
  void init(PinotConfiguration config, BrokerMetrics brokerMetrics);

  /**
   * Registers a listener to the failure detector.
   */
  void register(Listener listener);

  /**
   * Starts the failure detector. Listeners should be registered before starting the failure detector.
   */
  void start();

  /**
   * Notifies the failure detector that a query is submitted.
   */
  void notifyQuerySubmitted(QueryResponse queryResponse);

  /**
   * Notifies the failure detector that a query is finished (COMPLETED, FAILED or TIMED_OUT).
   */
  void notifyQueryFinished(QueryResponse queryResponse);

  /**
   * Marks a server as healthy.
   */
  void markServerHealthy(String instanceId);

  /**
   * Marks a server as unhealthy.
   */
  void markServerUnhealthy(String instanceId);

  /**
   * Returns all the unhealthy servers.
   */
  Set<String> getUnhealthyServers();

  /**
   * Stops the failure detector.
   */
  void stop();
}
