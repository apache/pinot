/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.minion.executor;

import com.linkedin.pinot.common.config.PinotTaskConfig;
import javax.annotation.Nonnull;


/**
 * The interface <code>PinotTaskExecutor</code> defines the APIs for task executors.
 */
public interface PinotTaskExecutor {

  /**
   * Execute the task based on the given {@link PinotTaskConfig}.
   */
  void executeTask(@Nonnull PinotTaskConfig pinotTaskConfig) throws Exception;

  /**
   * Try to cancel the task.
   */
  void cancel();
}
