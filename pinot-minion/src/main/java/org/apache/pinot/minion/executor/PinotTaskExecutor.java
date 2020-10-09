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
package org.apache.pinot.minion.executor;

import org.apache.pinot.core.minion.PinotTaskConfig;


/**
 * The interface <code>PinotTaskExecutor</code> defines the APIs for task executors.
 */
public interface PinotTaskExecutor {

  /**
   * Pre processing operations to be done at the beginning of task execution
   */
  default void preProcess(PinotTaskConfig pinotTaskConfig) {
  }

  /**
   * Executes the task based on the given task config and returns the execution result.
   */
  Object executeTask(PinotTaskConfig pinotTaskConfig)
      throws Exception;

  /**
   * Tries to cancel the task.
   */
  void cancel();

  /**
   * Post processing operations to be done before exiting a successful task execution
   */
  default void postProcess(PinotTaskConfig pinotTaskConfig) {
  }
}
