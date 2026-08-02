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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/// REALTIME end-to-end ingestion + commit test for an OPEN_STRUCT column. Consumes the data from
/// Kafka, triggers forceCommit so the consuming segment seals to a committed ONLINE segment, then
/// reuses the inherited index_map + dense/sparse validation against the committed REALTIME segment.
@Test(suiteName = "CustomClusterIntegrationTest")
public class OpenStructIngestionCommitRealtimeTest extends OpenStructIngestionCommitTestBase {

  private static final String DEFAULT_TABLE_NAME = "OpenStructIngestionCommitRealtimeTest";
  private static final long FORCE_COMMIT_TIMEOUT_MS = 120_000L;

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    // <= NUM_DOCS so a segment seals during consumption; forceCommit then seals any remainder.
    return 500;
  }

  @Override
  protected TableType getSegmentTableType() {
    return TableType.REALTIME;
  }

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    super.setUp();
    forceCommitAndWait();
  }

  private void forceCommitAndWait()
      throws Exception {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    String response = getOrCreateAdminClient().getTableClient().forceCommit(realtimeTableName);
    String jobId = JsonUtils.stringToJsonNode(response).get("forceCommitJobId").asText();
    TestUtils.waitForCondition(aVoid -> {
      try {
        return isForceCommitJobCompleted(jobId);
      } catch (Exception e) {
        return false;
      }
    }, 1000L, FORCE_COMMIT_TIMEOUT_MS, "Force commit did not complete for " + realtimeTableName);
  }

  private boolean isForceCommitJobCompleted(String jobId)
      throws Exception {
    String status = getOrCreateAdminClient().getTableClient().getForceCommitJobStatus(jobId);
    JsonNode node = JsonUtils.stringToJsonNode(status);
    JsonNode pending = node.get(CommonConstants.ControllerJob.CONSUMING_SEGMENTS_YET_TO_BE_COMMITTED_LIST);
    return pending != null && pending.size() == 0;
  }
}
