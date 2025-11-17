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
package org.apache.pinot.spi.accounting;

import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.query.QueryThreadContext;


public interface ThreadAccountant {

  /// Registers a [QueryThreadContext] of a running query to the current thread.
  void setupTask(QueryThreadContext threadContext);

  /// Samples the resource usage of the current thread.
  void sampleUsage();

  /// Clears the previous registered [QueryThreadContext] from the current thread.
  void clear();

  /// Updates the resource usage from an untracked source. This is used for request ser/de threads where thread
  /// execution context cannot be set up beforehand.
  void updateUntrackedResourceUsage(String identifier, long cpuTimeNs, long allocatedBytes,
      TrackingScope trackingScope);

  /// Returns `true` when the query submission should be throttled, `false` otherwise.
  default boolean throttleQuerySubmission() {
    return false;
  }

  /// Starts the watcher periodic task.
  default void startWatcherTask() {
  }

  /// Stops the watcher periodic task.
  default void stopWatcherTask() {
  }

  /// Returns the [PinotClusterConfigChangeListener] if the accountant wants to listen to cluster config changes, `null`
  /// otherwise.
  @Nullable
  default PinotClusterConfigChangeListener getClusterConfigChangeListener() {
    return null;
  }

  /// Returns the [ThreadResourceTracker]s for all threads executing queries.
  Collection<? extends ThreadResourceTracker> getThreadResources();

  /// Returns the [QueryResourceTracker]s for all queries executing in the broker or server.
  Map<String, ? extends QueryResourceTracker> getQueryResources();
}
