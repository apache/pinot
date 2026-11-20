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

import com.google.common.base.Joiner;
import java.io.File;
import org.apache.pinot.client.ExecutionStats;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/// Runs the [UpsertTableSegmentUploadIntegrationTest] flow on one server with upsert snapshots and preload enabled:
/// before the restart, it commits the consuming segments and waits for the uploaded segments' validDocIds snapshots,
/// so the restart preloads the segments from their snapshots.
public class UpsertTableSegmentPreloadIntegrationTest extends UpsertTableSegmentUploadIntegrationTest {

  @Override
  protected int getNumServers() {
    return 1;
  }

  @Override
  protected boolean pushBeforeUpload() {
    return false;
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX + ".max.segment.preload.threads",
        "1");
    serverConf.setProperty(Joiner.on(".")
        .join(Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX, Server.Upsert.CONFIG_PREFIX,
            Server.Upsert.DEFAULT_ENABLE_SNAPSHOT), "true");
    serverConf.setProperty(Joiner.on(".")
        .join(Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX, Server.Upsert.CONFIG_PREFIX,
            Server.Upsert.DEFAULT_ENABLE_PRELOAD), "true");
  }

  @Override
  protected void beforeRestart()
      throws Exception {
    waitForSnapshotCreation();
  }

  /// The 5 segments plus the 2 consuming segments created when consumption resumes
  @Override
  protected int getNumSegmentsAfterRestart() {
    return NUM_SEGMENTS + 2;
  }

  private void waitForSnapshotCreation()
      throws Exception {
    // Pause consumption and wait until all consuming segments are committed and loaded as immutable segment
    String rawTableName = getTableName();
    getOrCreateAdminClient().getTableClient().pauseConsumption(rawTableName);
    TestUtils.waitForCondition(aVoid -> {
      ExecutionStats executionStats =
          getPinotConnection().execute("SELECT COUNT(*) FROM " + rawTableName).getExecutionStats();
      return executionStats.getNumSegmentsQueried() == NUM_SEGMENTS
          && executionStats.getNumConsumingSegmentsQueried() == 0;
    }, 600_000L, "Failed to pause consumption");
    // Resume consumption to trigger snapshot
    getOrCreateAdminClient().getTableClient().resumeConsumption(rawTableName, null);

    // All the uploaded segments should have snapshots generated. Snapshots for the just committed segments are best
    // effort: the snapshot round triggered by the new consuming segment skips a segment without retry when its
    // segmentLock is still held, e.g. by the committing thread or the CONSUMING -> ONLINE state transition. Only the
    // latest committed segment of each partition is best effort, but this test has a single commit cycle, so every
    // LLC segment is a just committed one, and all of them are excluded from the check.
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
    TestUtils.waitForCondition(aVoid -> {
      for (BaseServerStarter serverStarter : _serverStarters) {
        String segmentDir = serverStarter.getConfig().getProperty(Server.CONFIG_OF_INSTANCE_DATA_DIR);
        File[] files = new File(segmentDir, realtimeTableName).listFiles(
            (dir, name) -> name.startsWith(rawTableName) && LLCSegmentName.of(name) == null);
        assertNotNull(files);
        assertEquals(files.length, 3);
        for (File file : files) {
          if (!new File(new File(file, "v3"), V1Constants.VALID_DOC_IDS_ON_DISK_BITMAP_FILE_NAME).exists()) {
            return false;
          }
        }
      }
      return true;
    }, 600_000L, "Failed to verify snapshots");
  }
}
