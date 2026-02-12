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

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.plugin.minion.tasks.MinionTaskUtils;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that runs the Refresh Segment minion task with METADATA {@link
 * org.apache.pinot.spi.ingestion.batch.BatchConfigProperties.SegmentPushType} to verify the full flow.
 * Only {@link #testFirstSegmentRefresh()} is enabled; other tests from the base class are disabled.
 */
public class RefreshSegmentMetadataPushMinionClusterIntegrationTest
    extends RefreshSegmentMinionClusterIntegrationTest {

  @Override
  @BeforeClass
  public void setUp()
      throws Exception {
    super.setUp();
    File outputSegmentDir = new File(_tempDir, "metadataPushOutput");
    TestUtils.ensureDirectoriesExistAndEmpty(outputSegmentDir);
  }

  @Override
  protected TableTaskConfig getRefreshSegmentTaskConfig() {
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put(BatchConfigProperties.PUSH_MODE,
        BatchConfigProperties.SegmentPushType.METADATA.name());
    tableTaskConfigs.put(MinionTaskUtils.ALLOW_METADATA_PUSH_WITH_LOCAL_FS, "true");
    return new TableTaskConfig(
        Collections.singletonMap(MinionConstants.RefreshSegmentTask.TASK_TYPE, tableTaskConfigs));
  }

  @Override
  @Test(priority = 2, enabled = false)
  public void testValidDatatypeChange() {
    // Disabled: only testFirstSegmentRefresh runs for METADATA push flow.
  }

  @Override
  @Test(priority = 3, enabled = false)
  public void testIndexChanges() {
    // Disabled: only testFirstSegmentRefresh runs for METADATA push flow.
  }

  @Override
  @Test(priority = 4, enabled = false)
  public void checkColumnAddition() {
    // Disabled: only testFirstSegmentRefresh runs for METADATA push flow.
  }

  @Override
  @Test(priority = 5, enabled = false)
  public void checkRefreshNotNecessary() {
    // Disabled: only testFirstSegmentRefresh runs for METADATA push flow.
  }
}
