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

import java.util.List;


/// Interface for aggregating CPU and memory usage of threads.
public interface ResourceAggregator {

  /// Updates CPU usage for one-off cases where identifier is known beforehand. For example: broker inbound netty thread
  /// where queryId and workloadName are already known.
  ///
  /// @param name identifier name - queryId, workload name, etc.
  /// @param cpuTimeNs CPU time in nanoseconds
  void updateConcurrentCpuUsage(String name, long cpuTimeNs);

  /// Updates memory usage for one-off cases where identifier is known beforehand. For example: broker inbound netty
  /// thread where queryId and workloadName are already known.
  ///
  /// @param name identifier name - queryId, workload name, etc.
  /// @param memBytes memory usage in bytes
  void updateConcurrentMemUsage(String name, long memBytes);

  /// Cleans up state after periodic aggregation is complete.
  void cleanUpPostAggregation();

  /// Sleep time between aggregations.
  int getAggregationSleepTimeMs();

  /// Pre-aggregation step to be called before the aggregation of all thread entries.
  void preAggregate(List<CPUMemThreadLevelAccountingObjects.ThreadEntry> anchorThreadEntries);

  /// Aggregates on a thread entry.
  void aggregate(Thread thread, CPUMemThreadLevelAccountingObjects.ThreadEntry threadEntry);

  /// Post-aggregation step to be called after the aggregation of all thread entries.
  void postAggregate();
}
