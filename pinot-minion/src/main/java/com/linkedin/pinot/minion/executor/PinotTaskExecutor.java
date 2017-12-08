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
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.minion.MinionContext;
import java.io.InputStream;
import javax.annotation.Nonnull;


/**
 * The interface <code>PinotTaskExecutor</code> defines the APIs for task executors.
 */
public interface PinotTaskExecutor {

  /**
   * Set the minion context.
   */
  void setMinionContext(@Nonnull MinionContext minionContext);

  /**
   * Execute the task based on the given {@link PinotTaskConfig}.
   */
  void executeTask(@Nonnull PinotTaskConfig pinotTaskConfig);

  /**
   * Try to cancel the task.
   */
  void cancel();

  /**
   * Try to upload segment with crc
   */
  int uploadSegment(String uri, final String fileName, final InputStream inputStream, final long lengthInBytes,
      FileUploadUtils.SendFileMethod httpMethod, String crc);
}
