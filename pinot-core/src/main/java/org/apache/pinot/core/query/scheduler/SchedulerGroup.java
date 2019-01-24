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
 * Scheduler group is a sub-queue in multi-level scheduling queues.
 * This class maintains context information for each of the scheduling
 * queues. Each SchedulerGroup is a queue of requests and related accounting.
 * A query request is mapped to a scheduler group. This mapping can be based on
 * tableName, tenantName or it can be completely configuration driven which allows
 * mapping a group of tables to a specific group. For example, a set of tables
 * with low QPS can be mapped to a single scheduler group.
 */
public interface SchedulerGroup extends SchedulerGroupAccountant {
  /**
   * Provides scheduler group name
   */
  String name();

  /**
   * Appends the query to the list of pending queries
   */
  void addLast(SchedulerQueryContext query);

  /**
   * Get the first pending query without removing it from the queue
   * @return query at the head of the queue or null if the queue is empty
   */
  SchedulerQueryContext peekFirst();

  /**
   * Removes first query from the pending queue and returns it
   * @return first query at the head of the pending queue or null if the queue is empty
   */
  SchedulerQueryContext removeFirst();

  /**
   * Remove all the pending queries with arrival time earlier than the deadline
   */
  void trimExpired(long deadlineMillis);

  /**
   * @return true if there are no pending queries for this group
   */
  boolean isEmpty();

  /**
   * Number of pending queries
   * @return
   */
  int numPending();

  /**
   * Number of running queries
   */
  int numRunning();
}
