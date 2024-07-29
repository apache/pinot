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

import java.util.Map;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.integration.tests.SimpleMinionClusterIntegrationTest;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.minion.exception.TaskCancelledException;
import org.apache.pinot.minion.executor.MinionTaskZkMetadataManager;
import org.apache.pinot.minion.executor.PinotTaskExecutor;
import org.apache.pinot.minion.executor.PinotTaskExecutorFactory;
import org.apache.pinot.plugin.minion.tasks.BaseTaskExecutor;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.spi.annotations.minion.TaskExecutorFactory;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Task executor factory for {@link SimpleMinionClusterIntegrationTest}.
 */
@TaskExecutorFactory
public class TestTaskExecutorFactory implements PinotTaskExecutorFactory {

  @Override
  public void init(MinionTaskZkMetadataManager zkMetadataManager) {
  }

  @Override
  public void init(MinionTaskZkMetadataManager zkMetadataManager, MinionConf minionConf) {
  }

  @Override
  public String getTaskType() {
    return SimpleMinionClusterIntegrationTest.TASK_TYPE;
  }

  @Override
  public PinotTaskExecutor create() {
    return new BaseTaskExecutor() {
      @Override
      public Boolean executeTask(PinotTaskConfig pinotTaskConfig) {
        assertTrue(MINION_CONTEXT.getDataDir().exists());
        assertNotNull(MINION_CONTEXT.getHelixPropertyStore());
        assertNotNull(MinionMetrics.get());

        assertEquals(pinotTaskConfig.getTaskType(), SimpleMinionClusterIntegrationTest.TASK_TYPE);
        Map<String, String> configs = pinotTaskConfig.getConfigs();
        assertEquals(configs.size(), SimpleMinionClusterIntegrationTest.NUM_CONFIGS);
        String offlineTableName = configs.get("tableName");
        assertEquals(TableNameBuilder.getTableTypeFromTableName(offlineTableName), TableType.OFFLINE);
        String rawTableName = TableNameBuilder.extractRawTableName(offlineTableName);
        assertTrue(rawTableName.equals(SimpleMinionClusterIntegrationTest.TABLE_NAME_1) || rawTableName
            .equals(SimpleMinionClusterIntegrationTest.TABLE_NAME_2));
        assertEquals(configs.get("tableType"), TableType.OFFLINE.toString());

        do {
          if (_cancelled) {
            throw new TaskCancelledException("Task has been cancelled");
          }
        } while (SimpleMinionClusterIntegrationTest.HOLD.get());
        return true;
      }

      @Override
      protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(
          PinotTaskConfig pinotTaskConfig, SegmentConversionResult segmentConversionResult) {
        throw new UnsupportedOperationException();
      }
    };
  }
}
