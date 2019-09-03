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
package org.apache.pinot.core.query.scheduler;

/**
 * Accounting interface to track resource utilization by a scheduler group.
 * Scheduler group represents a subqueue if a typical multi-level scheduler.
 * Sub-queues can selected using SchedulerGroupMapper.
 *
 * We mainly account for wall clock time of each thread for a query. This captures
 * CPU and IO cost for each query but also penalizes for GC activity. We do not account
 * for memory utilization yet. Nevertheless, wall clock time is a good approximation for
 * resource utilization in an online system.
 */
public interface SchedulerGroupAccountant extends Comparable<SchedulerGroupAccountant> {
  /**
   * Mark that a new thread is being used for this group
   */
  void incrementThreads();

  /**
   * Decrement threads usage for this group
   */
  void decrementThreads();

  /**
   * Get total number of threads in active use
   * @return number of threads in use
   */
  int getThreadsInUse();

  /**
   * Reserve additional threads for this scheduling group
   * @param threads number of threads
   */
  void addReservedThreads(int threads);

  /**
   * Releases thread capacity reserved for this scheduler group
   * @param threads
   */
  void releasedReservedThreads(int threads);

  /**
   * Total number of threads reserved for this group
   * @return
   */
  int totalReservedThreads();

  /**
   * Mark start of a query if the implementor wants to perform additional accounting
   */
  void startQuery();

  /**
   * Mark end of query execution.
   */
  void endQuery();
}
