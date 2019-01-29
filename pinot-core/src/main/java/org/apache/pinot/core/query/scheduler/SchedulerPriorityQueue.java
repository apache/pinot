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

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Interface for scheduler priority queue for priority based schedulers
 */
public interface SchedulerPriorityQueue {
  /**
   * Adds a query to the scheduler priority queue. This call always succeeds or
   * throws exception if the queues are out of capacity. This will never block for
   * resources to become available.
   * @param query query to add to the list of waiters
   * @throws OutOfCapacityException if the internal query queues are full
   */
  // TODO: throw OutOfCapacity or drop from the front of the queue ?
  // It may be more beneficial to drop oldest queries to move forward
  void put(@Nonnull SchedulerQueryContext query)
      throws OutOfCapacityException;

  /**
   * Blocking call to select the query with highest priority to schedule for execution next.
   * The returned query will be removed from internal queues.
   * @return query to schedule. Returns null only when interrupted
   */
  @Nullable
  SchedulerQueryContext take();

  /**
   * Get the list of all the pending queries in the queue
   * @return Non-null list of all the pending queries in the queue
   *         List is empty if there are no pending queries
   */
  @Nonnull
  List<SchedulerQueryContext> drain();
}
