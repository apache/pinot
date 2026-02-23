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
package org.apache.pinot.server.starter.helix;

import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelFactory;


/**
 * Manages the custom Helix state transition thread pools for Pinot server.
 * <br/>
 * The implementation of this interface can be passed to {@link SegmentOnlineOfflineStateModelFactory} to provide custom
 * thread pools for different state transition messages, instead of using Helix's managed thread pools.
 * <br/>
 * Helix maintains a cache from message identifier to assigned thread pool. Message identifiers are:
 * <ol>
 *   <li> {@link org.apache.helix.model.Message.MessageInfo} </li>
 *   <li> (resourceName, fromState, toState) combination </li>
 *   <li> resourceName </li>
 * </ol>
 * For each state transition message, Helix gets the message identifiers by the above order, and looks up the cache
 * to find if it's mapped to a thread pool, executes the state transition on it if it's non-null in the cache. If
 * three of the identifiers all map to null, the default Helix-managed thread pool would be used. During the lookup,
 * it calls {@link SegmentOnlineOfflineStateModelFactory#getExecutorService} upon cache misses. The cache is never
 * cleaned up until the server shuts down, so the method would only be called once for each argument combination
 * (even if it returns null).
 */
public interface StateTransitionThreadPoolManager {
  @Nullable
  StateModelFactory.CustomizedExecutorService getExecutorService(Message.MessageInfo messageInfo);

  @Nullable
  ExecutorService getExecutorService(String resourceName, String fromState, String toState);

  @Nullable
  ExecutorService getExecutorService(String resourceName);

  /**
   * Called after the Helix manager has connected successfully.
   * This allows implementations to read configuration from ZooKeeper that requires an active connection.
   */
  default void onHelixManagerConnected() {
    // Default no-op for backward compatibility
  }

  void shutdown();
}
