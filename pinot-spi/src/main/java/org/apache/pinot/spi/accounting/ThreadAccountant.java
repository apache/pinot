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

import javax.annotation.Nullable;


public interface ThreadAccountant {

  /**
   * clear thread accounting info
   */
  void clear();

  /**
   * check if the corresponding runner thread of current thread is interrupted
   */
  boolean isRootThreadInterrupted();

  /**
   * Task tracking info
   * @param queryId query id string
   * @param taskId a unique task id
   * @param parentContext the parent execution context, null for root(runner) thread
   */
  void createExecutionContext(String queryId, int taskId, @Nullable ExecutionContext parentContext);


  ExecutionContext getExecutionContext();

  /**
   * set resource usage provider
   */
  void setThreadResourceUsageProvider(ThreadResourceUsageProvider threadResourceUsageProvider);

  /**
   * operator call to update thread cpu time
   */
  void sampleThreadCPUTime();

  /**
   * operator call to update thread bytes allocated
   */
  void sampleThreadBytesAllocated();

  /**
   * start the periodical task
   */
  void startWatcherTask();

  /**
   * get error message if the query is killed
   * @return empty string if N/A
   */
  String getErrorMsg();
}
