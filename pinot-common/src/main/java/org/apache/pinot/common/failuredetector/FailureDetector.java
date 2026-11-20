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
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.env.PinotConfiguration;


/// The `FailureDetector` detects unhealthy servers based on the query responses. When it detects an unhealthy
/// server, it will notify the listener via a callback, and schedule a delay to retry the unhealthy server later via
/// another callback.
@InterfaceAudience.Private
@InterfaceStability.Evolving
@ThreadSafe
public interface FailureDetector {

  /// Initializes the failure detector.
  void init(PinotConfiguration config, BrokerMetrics brokerMetrics);

  /// Registers a function that will be periodically called to retry unhealthy servers. The function is called with the
  /// instanceId of the unhealthy server and should return [ServerState#HEALTHY] if the server is now healthy,
  /// [ServerState#UNHEALTHY] if the server is still unhealthy, and [ServerState#UNKNOWN] if the retrier
  /// does not know about this server.
  void registerUnhealthyServerRetrier(Function<String, ServerState> unhealthyServerRetrier);

  /// Registers a function that checks a server which left a query unanswered (see [#notifyServerNotResponded]). It is
  /// called with the instanceId of the server and should return [ServerState#UNHEALTHY] only if the server is not
  /// answering at all, and [ServerState#HEALTHY] or [ServerState#UNKNOWN] otherwise.
  ///
  /// Kept apart from the retriers on purpose. A retrier answers whether an unhealthy server has recovered, and may err
  /// towards keeping it out -- the multi-stage one reports an idle gRPC channel as unhealthy. A checker decides whether
  /// a server that is in routing comes out, so it must only report a server that verifiably failed to answer.
  default void registerServerNotRespondedChecker(Function<String, ServerState> serverNotRespondedChecker) {
  }

  /// Registers a consumer that will be called with the instanceId of a server that is detected as healthy.
  void registerHealthyServerNotifier(Consumer<String> healthyServerNotifier);

  /// Registers a consumer that will be called with the instanceId of a server that is detected as unhealthy.
  void registerUnhealthyServerNotifier(Consumer<String> unhealthyServerNotifier);

  /// Starts the failure detector.
  void start();

  /// Marks a server as healthy.
  @Deprecated
  default void markServerHealthy(String instanceId) {
    markServerHealthy(instanceId, null);
  };

  /// Marks a server as healthy.
  void markServerHealthy(String instanceId, @Nullable String hostName);

  /// Marks a server as unhealthy.
  @Deprecated
  default void markServerUnhealthy(String instanceId) {
    markServerUnhealthy(instanceId, null);
  };

  /// Marks a server as unhealthy.
  void markServerUnhealthy(String instanceId, @Nullable String hostName);

  /// Notifies the detector that a server left a query unanswered until the query deadline, without its connection
  /// ever breaking.
  ///
  /// A server whose node is gone, whose network path is blackholed, or whose JVM is frozen produces no other signal.
  /// Writing into an open socket succeeds even when the peer is gone, so there is no send exception, the channel is
  /// never torn down, and connection-failure detection cannot see it. The query simply times out.
  ///
  /// A timeout alone cannot tell such a server from one that is merely busy, so implementations must check the server
  /// with the registered checkers (see [#registerServerNotRespondedChecker]) before acting on it, and do nothing when
  /// none is registered.
  default void notifyServerNotResponded(String instanceId, @Nullable String hostName) {
  }

  /// Returns all the unhealthy servers.
  Set<String> getUnhealthyServers();

  /// Stops the failure detector.
  void stop();

  enum ServerState {
    HEALTHY,
    UNHEALTHY,
    UNKNOWN
  }
}
