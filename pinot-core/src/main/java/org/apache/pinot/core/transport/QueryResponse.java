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
package org.apache.pinot.core.transport;

import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;


/**
 * The {@code QueryResponse} interface represents the server responses for a query.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@ThreadSafe
public interface QueryResponse {
  enum Status {
    IN_PROGRESS, COMPLETED, FAILED, TIMED_OUT
  }

  /**
   * Returns the current {@link Status} of the query.
   */
  Status getStatus();

  /**
   * Returns the number of servers responded.
   */
  int getNumServersResponded();

  /**
   * Returns the current server responses without blocking.
   */
  Map<ServerRoutingInstance, ServerResponse> getCurrentResponses();

  /**
   * Waits until the query is done (COMPLETED, FAILED or TIMED_OUT) and returns the final server responses.
   */
  Map<ServerRoutingInstance, ServerResponse> getFinalResponses()
      throws InterruptedException;

  /**
   * Returns the server stats for the query. Should be called after query is done (COMPLETED, FAILED or TIMED_OUT).
   */
  String getServerStats();

  Map<String, Map<String, Integer>> getServerStatsMap();

  /**
   * Returns the time taken for the server to respond to the query.
   * @param serverRoutingInstance
   * @return
   */
  long getServerResponseDelayMs(ServerRoutingInstance serverRoutingInstance);

  /**
   * Returns the failed server if the query fails.
   */
  @Nullable
  ServerRoutingInstance getFailedServer();

  /**
   * Returns the query request Id.
   */
  long getRequestId();

  /**
   * Returns the query timeout in milliseconds.
   */
  long getTimeoutMs();

  /**
   * Returns the exception if the query fails.
   */
  @Nullable
  Exception getException();
}
