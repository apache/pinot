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
package org.apache.pinot.common.failuredetector;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.BrokerMetrics;
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
   * Initializes the failure detector.
   */
  void init(PinotConfiguration config, BrokerMetrics brokerMetrics);

  /**
   * Registers a function that will be periodically called to retry unhealthy servers. The function is called with the
   * instanceId of the unhealthy server and should return true if the server is now healthy, false otherwise.
   */
  void registerUnhealthyServerRetrier(Function<String, Boolean> unhealthyServerRetrier);

  /**
   * Registers a consumer that will be called with the instanceId of a server that is detected as healthy.
   */
  void registerHealthyServerNotifier(Consumer<String> healthyServerNotifier);

  /**
   * Registers a consumer that will be called with the instanceId of a server that is detected as unhealthy.
   */
  void registerUnhealthyServerNotifier(Consumer<String> unhealthyServerNotifier);

  /**
   * Starts the failure detector.
   */
  void start();

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
