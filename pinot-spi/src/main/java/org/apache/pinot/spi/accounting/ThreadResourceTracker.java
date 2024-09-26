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

import com.fasterxml.jackson.databind.annotation.JsonSerialize;


/**
 * Tracks allocated bytes and CPU time by a thread when executing a task of a query.
 */
@JsonSerialize
public interface ThreadResourceTracker {
  /**
   * Total execution CPU Time(nanoseconds) of a thread when executing a query task in a server or broker.
   * @return A long containing the nanoseconds.
   */
  long getCPUTimeMS();

  /**
   * Allocated bytes for a query task in a server or broker
   * @return A long containing the number of bytes allocated to execute the query task.
   */
  long getAllocatedBytes();

  /**
   * QueryId of the task the thread is executing.
   * @return a string containing the query id.
   */
  String getQueryId();

  /**
   * TaskId of the task the thread is executing.
   * @return an int containing the task id.
   */
  int getTaskId();

  ThreadExecutionContext.TaskType getTaskType();
}
