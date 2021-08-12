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
package org.apache.pinot.integration.tests.plugin.minion.tasks;

import javax.annotation.Nullable;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.integration.tests.SimpleMinionClusterIntegrationTest;
import org.apache.pinot.minion.event.MinionEventObserver;
import org.apache.pinot.minion.event.MinionEventObserverFactory;
import org.apache.pinot.minion.executor.MinionTaskZkMetadataManager;
import org.apache.pinot.spi.annotations.minion.EventObserverFactory;

import static org.testng.Assert.assertTrue;


/**
 * Event observer factory for {@link SimpleMinionClusterIntegrationTest}.
 */
@EventObserverFactory
public class TestEventObserverFactory implements MinionEventObserverFactory {

  @Override
  public void init(MinionTaskZkMetadataManager zkMetadataManager) {
  }

  @Override
  public String getTaskType() {
    return SimpleMinionClusterIntegrationTest.TASK_TYPE;
  }

  @Override
  public MinionEventObserver create() {
    return new MinionEventObserver() {
      @Override
      public void notifyTaskStart(PinotTaskConfig pinotTaskConfig) {
        SimpleMinionClusterIntegrationTest.TASK_START_NOTIFIED.set(true);
      }

      @Override
      public void notifyTaskSuccess(PinotTaskConfig pinotTaskConfig, @Nullable Object executionResult) {
        assertTrue(executionResult instanceof Boolean);
        assertTrue((Boolean) executionResult);
        SimpleMinionClusterIntegrationTest.TASK_SUCCESS_NOTIFIED.set(true);
      }

      @Override
      public void notifyTaskCancelled(PinotTaskConfig pinotTaskConfig) {
        SimpleMinionClusterIntegrationTest.TASK_CANCELLED_NOTIFIED.set(true);
      }

      @Override
      public void notifyTaskError(PinotTaskConfig pinotTaskConfig, Exception exception) {
        SimpleMinionClusterIntegrationTest.TASK_ERROR_NOTIFIED.set(true);
      }
    };
  }
}
