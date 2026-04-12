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
package org.apache.pinot.integration.tests;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.plugin.minion.tasks.MinionTaskUtils;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.testng.annotations.Test;


/**
 * Integration test that runs the Purge minion task with METADATA {@link
 * org.apache.pinot.spi.ingestion.batch.BatchConfigProperties.SegmentPushType} to verify the full flow.
 * Only {@link #testFirstRunPurge()} is enabled; other tests from the base class are disabled.
 */
public class PurgeMetadataPushMinionClusterIntegrationTest extends PurgeMinionClusterIntegrationTest {

  @Override
  protected TableTaskConfig getPurgeTaskConfig() {
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put(MinionConstants.PurgeTask.LAST_PURGE_TIME_THREESOLD_PERIOD, "1d");
    tableTaskConfigs.put(BatchConfigProperties.PUSH_MODE, BatchConfigProperties.SegmentPushType.METADATA.name());
    tableTaskConfigs.put(MinionTaskUtils.ALLOW_METADATA_PUSH_WITH_LOCAL_FS, "true");
    return new TableTaskConfig(Collections.singletonMap(MinionConstants.PurgeTask.TASK_TYPE, tableTaskConfigs));
  }

  @Override
  @Test(enabled = false)
  public void testPassedDelayTimePurge() {
    // Disabled: only testFirstRunPurge runs for METADATA push flow.
  }

  @Override
  @Test(enabled = false)
  public void testNotPassedDelayTimePurge() {
    // Disabled: only testFirstRunPurge runs for METADATA push flow.
  }

  @Override
  @Test(enabled = false)
  public void testPurgeOnOldSegmentsWithIndicesOnNewColumns() {
    // Disabled: only testFirstRunPurge runs for METADATA push flow.
  }

  @Override
  @Test(enabled = false)
  public void testSegmentDeletionWhenAllRecordsPurged() {
    // Disabled: only testFirstRunPurge runs for METADATA push flow.
  }

  @Override
  @Test(enabled = false)
  public void testRealtimeLastSegmentPreservation() {
    // Disabled: only testFirstRunPurge runs for METADATA push flow.
  }
}
