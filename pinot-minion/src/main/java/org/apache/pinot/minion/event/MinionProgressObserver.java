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
package org.apache.pinot.minion.event;

import java.io.PrintWriter;
import java.io.StringWriter;
import javax.annotation.Nullable;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A minion event observer that can track task progress status in memory.
 */
public class MinionProgressObserver extends DefaultMinionEventObserver {
  private static final Logger LOGGER = LoggerFactory.getLogger(MinionProgressObserver.class);

  private static volatile long _startTs;
  private static volatile Object _lastStatus;

  @Override
  public void notifyTaskStart(PinotTaskConfig pinotTaskConfig) {
    _startTs = System.currentTimeMillis();
    super.notifyTaskStart(pinotTaskConfig);
  }

  public void notifyProgress(PinotTaskConfig pinotTaskConfig, @Nullable Object progress) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Update progress: {} for task: {}", progress, pinotTaskConfig.getTaskId());
    }
    _lastStatus = progress;
    super.notifyProgress(pinotTaskConfig, progress);
  }

  @Nullable
  public Object getProgress() {
    return _lastStatus;
  }

  @Override
  public void notifyTaskSuccess(PinotTaskConfig pinotTaskConfig, @Nullable Object executionResult) {
    long endTs = System.currentTimeMillis();
    _lastStatus = "Task succeeded in " + (endTs - _startTs) + "ms";
    super.notifyTaskSuccess(pinotTaskConfig, executionResult);
  }

  @Override
  public void notifyTaskCancelled(PinotTaskConfig pinotTaskConfig) {
    long endTs = System.currentTimeMillis();
    _lastStatus = "Task got cancelled after " + (endTs - _startTs) + "ms";
    super.notifyTaskCancelled(pinotTaskConfig);
  }

  @Override
  public void notifyTaskError(PinotTaskConfig pinotTaskConfig, Exception e) {
    long endTs = System.currentTimeMillis();
    _lastStatus = "Task failed in " + (endTs - _startTs) + "ms, with error:\n" + makeStringFromException(e);
    super.notifyTaskError(pinotTaskConfig, e);
  }

  private static String makeStringFromException(Exception exp) {
    StringWriter expStr = new StringWriter();
    try (PrintWriter pw = new PrintWriter(expStr)) {
      exp.printStackTrace(pw);
    }
    return expStr.toString();
  }
}
