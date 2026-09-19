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
import java.util.List;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReaderV7;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Realtime commit/load coverage for the V7 codec-pipeline format.
@Test(suiteName = "CustomClusterIntegrationTest")
public class CodecPipelineRealtimeIntegrationTest extends CodecPipelineIntegrationTest {
  private static final String TABLE_NAME = "CodecPipelineRealtimeIntegrationTest";

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }

  @Override
  protected int getNumKafkaPartitions() {
    return 1;
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    // Keep all 600 test rows consuming until setUp() force-commits them.
    return 1_000;
  }

  @Override
  @BeforeClass
  public void setUp()
      throws Exception {
    super.setUp();
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    String response = getOrCreateAdminClient().getTableClient().forceCommit(realtimeTableName);
    String jobId = JsonUtils.stringToJsonNode(response).get("forceCommitJobId").asText();
    TestUtils.waitForCondition(aVoid -> isForceCommitComplete(jobId), 120_000L,
        "Timed out waiting for forceCommit job: " + jobId);
    TestUtils.waitForCondition(aVoid -> inspectImmutableSegments(realtimeTableName) > 0, 120_000L,
        "Timed out waiting for a committed V7 realtime segment");
  }

  @Override
  @Test
  public void testGeneratedSegmentsUseConfiguredForwardIndexFormats() {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    assertTrue(inspectImmutableSegments(realtimeTableName) > 0, "No committed V7 realtime segment was loaded");
  }

  private int inspectImmutableSegments(String realtimeTableName) {
    int inspected = 0;
    for (BaseServerStarter serverStarter : getSharedServerStarters()) {
      TableDataManager tableDataManager = serverStarter.getServerInstance().getInstanceDataManager()
          .getTableDataManager(realtimeTableName);
      if (tableDataManager == null) {
        continue;
      }
      List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
      try {
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          IndexSegment segment = segmentDataManager.getSegment();
          if (segment instanceof ImmutableSegmentImpl) {
            assertV7((ImmutableSegmentImpl) segment, "intLz4", "LZ4");
            assertV7((ImmutableSegmentImpl) segment, "longDeltaLz4", "DELTA,LZ4");
            inspected++;
          }
        }
      } finally {
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          tableDataManager.releaseSegment(segmentDataManager);
        }
      }
    }
    return inspected;
  }

  private static void assertV7(ImmutableSegmentImpl segment, String column, String codecSpec) {
    ForwardIndexReader<?> reader = segment.getDataSource(column).getForwardIndex();
    assertTrue(reader instanceof FixedByteChunkSVForwardIndexReaderV7,
        column + " was routed to " + reader.getClass().getSimpleName());
    try (SegmentDirectory directory = new SegmentLocalFSDirectory(segment.getSegmentMetadata().getIndexDir(),
        ReadMode.mmap); SegmentDirectory.Reader segmentReader = directory.createReader()) {
      PinotDataBuffer forwardIndexBuffer = segmentReader.getIndexFor(column, StandardIndexes.forward());
      assertEquals(FixedByteChunkSVForwardIndexReaderV7.readCodecSpec(forwardIndexBuffer), codecSpec);
    } catch (Exception e) {
      throw new AssertionError("Failed to inspect V7 codec header for " + column, e);
    }
    assertNull(reader.getCompressionType());
  }

  private boolean isForceCommitComplete(String jobId) {
    try {
      String response = getOrCreateAdminClient().getTableClient().getForceCommitJobStatus(jobId);
      JsonNode status = JsonUtils.stringToJsonNode(response);
      return status.get(CommonConstants.ControllerJob.NUM_CONSUMING_SEGMENTS_YET_TO_BE_COMMITTED).asInt(-1) == 0;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
