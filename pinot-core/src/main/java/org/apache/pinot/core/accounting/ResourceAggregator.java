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
package org.apache.pinot.core.accounting;

/// Interface for aggregating CPU and memory usage of threads.
public interface ResourceAggregator {

  /// Updates the resource usage from an untracked source. This is used for request ser/de threads where thread
  /// execution context cannot be set up beforehand.
  void updateUntrackedResourceUsage(String identifier, long cpuTimeNs, long allocatedBytes);

  /// Sleep time between aggregations.
  int getAggregationSleepTimeMs();

  /// Pre-aggregation step to be called before the aggregation of all thread entries.
  void preAggregate(Iterable<ThreadResourceTrackerImpl> threadTrackers);

  /// Aggregates on a thread entry.
  void aggregate(ThreadResourceTrackerImpl threadTracker);

  /// Post-aggregation step to be called after the aggregation of all thread entries.
  void postAggregate();

  /// Cleans up state after periodic aggregation is complete.
  void cleanUpPostAggregation();
}
