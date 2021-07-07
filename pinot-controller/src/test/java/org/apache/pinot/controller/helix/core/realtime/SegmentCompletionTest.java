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
package org.apache.pinot.controller.helix.core.realtime;

import com.google.common.base.Preconditions;
import java.lang.reflect.Field;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.PinotMetricUtils;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.realtime.segment.CommittingSegmentDescriptor;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.LongMsgOffsetFactory;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.protocols.SegmentCompletionProtocol.ControllerResponseStatus;
import static org.apache.pinot.common.protocols.SegmentCompletionProtocol.Request;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SegmentCompletionTest {
  private static final String FAKEFS_FINAL_SEGMENT_URI = "fakefs:///final_segment_uri";
  private MockPinotLLCRealtimeSegmentManager segmentManager;
  private MockSegmentCompletionManager segmentCompletionMgr;
  private Map<String, Object> fsmMap;
  private Map<String, Long> commitTimeMap;
  private final String tableName = "someTable";
  private String segmentNameStr;
  private final String s1 = "S1";
  private final String s2 = "S2";
  private final String s3 = "S3";

  private final LongMsgOffset s1Offset = new LongMsgOffset(20L);
  private final LongMsgOffset s2Offset = new LongMsgOffset(40L);
  private final LongMsgOffset s3Offset = new LongMsgOffset(30L);

  @BeforeMethod
  public void testCaseSetup() throws Exception {
    testCaseSetup(true, true);
  }

  private StreamPartitionMsgOffset getModifiedLongOffset(LongMsgOffset original, long increment) {
    long newOffset = original.getOffset() + increment;
    return new LongMsgOffset(newOffset);
  }

  private void verifyOffset(SegmentCompletionProtocol.Response response, StreamPartitionMsgOffset offset) {
    StreamPartitionMsgOffset respOffset = new LongMsgOffset(response.getStreamPartitionMsgOffset());
    Assert.assertEquals(respOffset.compareTo(offset), 0);
    // Compatibility test:
    // The controller must always respond with BOTH fields -- 'offset' as well as 'streamPartitionMsgOffset', and they
    // should be the same value.
    Assert.assertEquals(respOffset.compareTo(offset), 0);
  }

  public void testCaseSetup(boolean isLeader, boolean isConnected) throws Exception {
    PinotHelixResourceManager mockPinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    HelixManager mockHelixManager = createMockHelixManager(isLeader, isConnected);
    when(mockPinotHelixResourceManager.getHelixZkManager()).thenReturn(mockHelixManager);
    segmentManager = new MockPinotLLCRealtimeSegmentManager(mockPinotHelixResourceManager);
    final int partitionId = 23;
    final int seqId = 12;
    final long now = System.currentTimeMillis();
    final LLCSegmentName segmentName = new LLCSegmentName(tableName, partitionId, seqId, now);
    segmentNameStr = segmentName.getSegmentName();
    final LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
    metadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    metadata.setNumReplicas(3);
    segmentManager._segmentMetadata = metadata;

    segmentCompletionMgr = new MockSegmentCompletionManager(segmentManager, isLeader, isConnected);
    segmentManager._segmentCompletionMgr = segmentCompletionMgr;

    Field fsmMapField = SegmentCompletionManager.class.getDeclaredField("_fsmMap");
    fsmMapField.setAccessible(true);
    fsmMap = (Map<String, Object>) fsmMapField.get(segmentCompletionMgr);

    Field ctMapField = SegmentCompletionManager.class.getDeclaredField("_commitTimeMap");
    ctMapField.setAccessible(true);
    commitTimeMap = (Map<String, Long>) ctMapField.get(segmentCompletionMgr);
  }

  // Simulate a new controller taking over with an empty completion manager object,
  // but segment metadata is fine in zk
  private void replaceSegmentCompletionManager() throws Exception {
    long oldSecs = segmentCompletionMgr._seconds;
    segmentCompletionMgr = new MockSegmentCompletionManager(segmentManager, true, true);
    segmentCompletionMgr._seconds = oldSecs;
    Field fsmMapField = SegmentCompletionManager.class.getDeclaredField("_fsmMap");
    fsmMapField.setAccessible(true);
    fsmMap = (Map<String, Object>) fsmMapField.get(segmentCompletionMgr);
  }

  @Test
  public void testStoppedConsumeDuringCompletion() throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    final String reason = "IAmLazy";

    // s1 sends offset of 20, gets HOLD at t = 5s;
    segmentCompletionMgr._seconds = 5;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3).withStreamPartitionMsgOffset(s3Offset.toString()).
        withSegmentName(segmentNameStr).withReason(reason);
    response = segmentCompletionMgr.segmentStoppedConsuming(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.PROCESSED);
    Assert.assertEquals(new LLCSegmentName(segmentNameStr), segmentManager._stoppedSegmentName);
    Assert.assertEquals(s3, segmentManager._stoppedInstance);
    segmentManager._stoppedSegmentName = null;
    segmentManager._stoppedInstance = null;

    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    // s2 is asked to commit.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
    // s3 comes back with new caught up offset, it should get a HOLD, since commit is not done yet.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 executes a successful commit
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);

    segmentCompletionMgr._seconds += 5;
    params = new Request.Params().withInstanceId(s2).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitEnd(params, true, false,
        CommittingSegmentDescriptor.fromSegmentCompletionReqParams(params));
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);

    // Now the FSM should have disappeared from the map
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // Now if s3 or s1 come back, they are asked to keep the segment they have.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s1).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.KEEP);

    // And the FSM should be removed.
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));
  }

  @Test
  public void testStoppedConsumeBeforeHold() throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    final String reason = "IAmLazy";
    // s1 stops consuming at t = 5;
    segmentCompletionMgr._seconds = 5;
    params = new Request.Params().withInstanceId(s1).withStreamPartitionMsgOffset(s1Offset.toString()).
        withSegmentName(segmentNameStr).withReason(reason);
    response = segmentCompletionMgr.segmentStoppedConsuming(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.PROCESSED);
    Assert.assertEquals(new LLCSegmentName(segmentNameStr), segmentManager._stoppedSegmentName);
    Assert.assertEquals(s1, segmentManager._stoppedInstance);
    segmentManager._stoppedSegmentName = null;
    segmentManager._stoppedInstance = null;

    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // s3 sends offset of 30, gets catchup to 40, s2 should have been decided as the winner now
    // since we are never expecting to hear back from s1
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3).withStreamPartitionMsgOffset(s3Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    verifyOffset(response, s2Offset);

    // s3 happens to come back (after catchup to s2offset) before s2 should get a hold since s2 has been decided as
    // the winner.
    // TODO Can optimize here since s2 is not notified yet.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    verifyOffset(response, s2Offset);
    // s2 is asked to commit.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    // s2 executes a successful commit
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);

    segmentCompletionMgr._seconds += 5;
    params = new Request.Params().withInstanceId(s2).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitEnd(params, true, false,
        CommittingSegmentDescriptor.fromSegmentCompletionReqParams(params));
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);

    // Now the FSM should have disappeared from the map
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // Now if s3 or s1 come back, they are asked to keep the segment they have.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s1).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.KEEP);

    // And the FSM should be removed.
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));
  }

  // s2 sends stoppedConsuming message, but then may have gotten restarted, so eventually we complete the segment.
  @Test
  public void testHappyPathAfterStoppedConsuming() throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    segmentCompletionMgr._seconds = 5;

    params = new Request.Params().withInstanceId(s2).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr).withReason("some reason");
    response = segmentCompletionMgr.segmentStoppedConsuming(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.PROCESSED);
    Assert.assertEquals(new LLCSegmentName(segmentNameStr), segmentManager._stoppedSegmentName);
    Assert.assertEquals(s2, segmentManager._stoppedInstance);
    segmentManager._stoppedSegmentName = null;
    segmentManager._stoppedInstance = null;

    testHappyPath(6L);
  }

  @Test
  public void testHappyPath() throws Exception {
    testHappyPath(5L);
  }

  // Tests happy path with split commit protocol
  @Test
  public void testHappyPathSplitCommitWithLocalFS() throws Exception {
    testHappyPathSplitCommit(5L, "/local/file", "http://null:null/segments/" + tableName + "/" + segmentNameStr);
  }

  @Test
  public void testHappyPathSplitCommitWithDeepstore() throws Exception {
    testHappyPathSplitCommit(5L, "fakefs:///segment1", "fakefs:///segment1");
  }

  @Test
  public void testHappyPathSplitCommitWithPeerDownloadScheme() throws Exception {
    testHappyPathSplitCommit(5L, CommonConstants.Segment.PEER_SEGMENT_DOWNLOAD_SCHEME + "/segment1",
        CommonConstants.Segment.PEER_SEGMENT_DOWNLOAD_SCHEME + "/segment1");
  }

  @Test
  public void testExceptionInConsumedMessage() {
    // Erase segment metadata in zookeeper so that FSM creation in lookupOrCreateFsm() will fail.
    segmentManager._segmentMetadata = null;

    SegmentCompletionProtocol.Response response;
    Request.Params params;
    segmentCompletionMgr._seconds = 10;
    params = new Request.Params().withInstanceId(s1).withStreamPartitionMsgOffset(s1Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.FAILED);
  }

  // When commit segment file fails, makes sure the fsm aborts and that a segment can successfully commit afterwards.
  @Test
  public void testCommitSegmentFileFail() throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    segmentCompletionMgr._seconds = 5L;
    params = new Request.Params().withInstanceId(s1).withStreamPartitionMsgOffset(s1Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3).withStreamPartitionMsgOffset(s3Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    verifyOffset(response, s2Offset);
    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s1).withStreamPartitionMsgOffset(s1Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    verifyOffset(response, s2Offset);
    // s2 is asked to commit.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    // TODO: Verify controller asked to do a split commit
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
    // s3 comes back with new caught up offset, it should get a HOLD, since commit is not done yet.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 executes a successful commit start
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);
    // s2's file does not successfully commit because MockPinotLLCRealtimeSegmentManager.commitSegmentFile() returns
    // false when detecting SegmentLocation == "doNotCommitMe";
    segmentCompletionMgr._seconds += 5;
    params = new Request.Params().withInstanceId(s2).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr).withSegmentLocation("doNotCommitMe");
    response = segmentCompletionMgr.segmentCommitEnd(params, true, true,
        CommittingSegmentDescriptor.fromSegmentCompletionReqParams(params));
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.FAILED);

    // Now the FSM should have aborted
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // Now s1 comes back; it is asked to hold.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s1).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // Now s3 comes back; it is asked to commit
    segmentCompletionMgr._seconds += 5;
    params = new Request.Params().withInstanceId(s3).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.COMMIT);

    // Now s2 comes back; it is asked to hold
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.HOLD);

    // s3 executes a successful commit start
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);
    // s3's file successfully commits
    segmentCompletionMgr._seconds += 5;
    params = new Request.Params().withInstanceId(s3).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr).withSegmentLocation("location");
    response = segmentCompletionMgr.segmentCommitEnd(params, true, true,
        CommittingSegmentDescriptor.fromSegmentCompletionReqParams(params));
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.COMMIT_SUCCESS);
    // And the FSM should be removed.
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));
  }

  private void testHappyPathSplitCommit(long startTime, String segmentLocation, String downloadURL) throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    segmentCompletionMgr._seconds = startTime;
    params = new Request.Params().withInstanceId(s1).withStreamPartitionMsgOffset(s1Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3).withStreamPartitionMsgOffset(s3Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    verifyOffset(response, s2Offset);
    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s1).withStreamPartitionMsgOffset(s1Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    verifyOffset(response, s2Offset);
    // s2 is asked to commit.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    // TODO: Verify controller asked to do a split commit
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
    // s3 comes back with new caught up offset, it should get a HOLD, since commit is not done yet.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 executes a successful commit
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);

    segmentCompletionMgr._seconds += 5;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr)
        .withSegmentLocation(segmentLocation);
    response = segmentCompletionMgr.segmentCommitEnd(params, true, true,
        CommittingSegmentDescriptor.fromSegmentCompletionReqParams(params));
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
    Assert.assertEquals(segmentManager.getSegmentZKMetadata(null, null, null).getDownloadUrl(), downloadURL);

    // Now the FSM should have disappeared from the map
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // Now if s3 or s1 come back, they are asked to keep the segment they have.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s1).withStreamPartitionMsgOffset(s2Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.KEEP);

    // And the FSM should be removed.
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));
  }

  // Tests that we abort when the server instance comes back with a different offset than it is told to commit with
  @Test
  public void testCommitDifferentOffsetSplitCommit() throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    segmentCompletionMgr._seconds = 5L;
    params = new Request.Params().withInstanceId(s1).withStreamPartitionMsgOffset(s1Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 20, gets HOLD
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2).withStreamPartitionMsgOffset(s1Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 20, gets commit
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3).withStreamPartitionMsgOffset(s1Offset.toString()).
        withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.COMMIT);
    verifyOffset(response, s1Offset);

    // s3 sends a commit start with 20
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);

    // s3 comes back to try to commit with a different offset
    segmentCompletionMgr._seconds += 5;
    params = new Request.Params().withInstanceId(s3)
        .withStreamPartitionMsgOffset(s3Offset.toString())
        .withSegmentName(segmentNameStr)
        .withSegmentLocation("location");
    response = segmentCompletionMgr.segmentCommitEnd(params, true, true,
        CommittingSegmentDescriptor.fromSegmentCompletionReqParams(params));
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.FAILED);

    // Now the FSM should have disappeared from the map
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // Now if s2 or s1 come back, they are asked to hold.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // FSM should still be there for that segment
    Assert.assertTrue(fsmMap.containsKey(segmentNameStr));
  }

  public void testHappyPath(long startTime) throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    segmentCompletionMgr._seconds = startTime;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3)
        .withStreamPartitionMsgOffset(s3Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    verifyOffset(response, s2Offset);
    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    // s2 is asked to commit.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
    // s3 comes back with new caught up offset, it should get a HOLD, since commit is not done yet.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 executes a successful commit
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);

    segmentCompletionMgr._seconds += 5;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitEnd(params, true, false,
        CommittingSegmentDescriptor.fromSegmentCompletionReqParams(params));
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);

    // Now the FSM should have disappeared from the map
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // Now if s3 or s1 come back, they are asked to keep the segment they have.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.KEEP);

    // And the FSM should be removed.
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));
  }

  @Test
  public void testControllerNotConnected() throws Exception {
    testCaseSetup(true, false); // Leader but not connected
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    segmentCompletionMgr._seconds = 5L;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr)
        .withReason("rowLimit");
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.NOT_LEADER);
  }

  @Test
  public void testWinnerOnTimeLimit() throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    segmentCompletionMgr._seconds = 10L;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr)
        .withReason(SegmentCompletionProtocol.REASON_TIME_LIMIT);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
  }

  @Test
  public void testWinnerOnRowLimit() throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    segmentCompletionMgr._seconds = 10L;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr)
        .withReason(SegmentCompletionProtocol.REASON_ROW_LIMIT);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.COMMIT);
    // S2 comes with the same offset as S1
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr)
        .withReason(SegmentCompletionProtocol.REASON_ROW_LIMIT);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.HOLD);
    segmentCompletionMgr._seconds += 1;
    // S3 comes with a different offset and without row limit. we ask it to hold even though it is higher.
    params = new Request.Params().withInstanceId(s3)
        .withStreamPartitionMsgOffset(s3Offset.toString())
        .withSegmentName(segmentNameStr)
        .withReason(SegmentCompletionProtocol.REASON_TIME_LIMIT);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.HOLD);
    // S1 comes back to commit the segment
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);

    segmentCompletionMgr._seconds += 5;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitEnd(params, true, false,
        CommittingSegmentDescriptor.fromSegmentCompletionReqParams(params));
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
    // We ask S2 to keep the segment
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr)
        .withReason(SegmentCompletionProtocol.REASON_ROW_LIMIT);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.KEEP);
    // And we ask S3 to discard because it was ahead.
    params = new Request.Params().withInstanceId(s3)
        .withStreamPartitionMsgOffset(s3Offset.toString())
        .withSegmentName(segmentNameStr)
        .withReason(SegmentCompletionProtocol.REASON_TIME_LIMIT);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.DISCARD);
  }

  // Tests that when server is delayed(Stalls for a hour), when server comes back, we commit successfully.
  @Test
  public void testDelayedServerSplitCommit() throws Exception {
    testDelayedServer(true);
  }

  @Test
  public void testDelayedServer() throws Exception {
    testDelayedServer(false);
  }

  public void testDelayedServer(boolean isSplitCommit) throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    final int startTimeSecs = 5;
    segmentCompletionMgr._seconds = startTimeSecs;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // Now s1 comes back again, and is asked to hold
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 is asked to commit.
    segmentCompletionMgr._seconds += SegmentCompletionProtocol.MAX_HOLD_TIME_MS / 1000;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    // Now s3 comes up with a better offset, but we ask it to hold, since the committer has not committed yet.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3).withSegmentName(segmentNameStr);
    params.withStreamPartitionMsgOffset(getModifiedLongOffset(s2Offset, 10).toString());
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 commits.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);
    segmentCompletionMgr._seconds += 5;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr)
        .withSegmentLocation("location");
    response = segmentCompletionMgr.segmentCommitEnd(params, true, isSplitCommit,
        CommittingSegmentDescriptor.fromSegmentCompletionReqParams(params));
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
    // Now the FSM should have disappeared from the map
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // Now s3 comes back to get a discard.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3).withSegmentName(segmentNameStr);
    params.withStreamPartitionMsgOffset(getModifiedLongOffset(s2Offset, 10).toString());
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.DISCARD);
    // Now the FSM should have disappeared from the map
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));
  }

  // We test the case where all servers go silent after controller asks one of them commit
  @Test
  public void testDeadServers() throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    final int startTimeSecs = 5;
    segmentCompletionMgr._seconds = startTimeSecs;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 is asked to catch up.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3)
        .withStreamPartitionMsgOffset(s3Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    verifyOffset(response, s2Offset);

    // Now s2 is asked to commit.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    // All servers are dead
    segmentCompletionMgr._seconds += 3600;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.HOLD);
    Assert.assertTrue(fsmMap.containsKey(segmentNameStr));

    // Now s2 is asked to commit because the max time to pick committer has passed.
    segmentCompletionMgr._seconds += 4;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.COMMIT);
  }

  // We test the case when the committer is asked to commit, but they never come back.
  @Test
  public void testCommitterFailure() throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    final int startTimeSecs = 5;
    segmentCompletionMgr._seconds = startTimeSecs;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 is asked to hold.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3)
        .withStreamPartitionMsgOffset(s3Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    verifyOffset(response, s2Offset);
    // Now s2 is asked to commit.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    // Time passes, s2 never comes back.
    segmentCompletionMgr._seconds += SegmentCompletionProtocol.MAX_HOLD_TIME_MS / 1000;

    // But since s1 and s3 are in HOLDING state, they should come back again. s1 is asked to catchup
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);

    // Max time to commit passes
    segmentCompletionMgr._seconds +=
        SegmentCompletionProtocol.getMaxSegmentCommitTimeMs() * SegmentCompletionProtocol.MAX_HOLD_TIME_MS / 1000;

    // s1 comes back with the updated offset, since it was asked to catch up.
    // The FSM will be aborted, and destroyed ...
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // s1 comes back again, a new FSM created
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // s3 comes back
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // And winner chosen when the last one does not come back at all
    segmentCompletionMgr._seconds += 5;
    params = new Request.Params().withInstanceId(s3)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    // The FSM is still present to complete the happy path as before.
    Assert.assertTrue(fsmMap.containsKey(segmentNameStr));
  }

  @Test
  public void testHappyPathSlowCommit() throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    // s1 sends offset of 20, gets HOLD at t = 1509242466s;
    final long startTime = 1509242466;
    final String tableName = new LLCSegmentName(segmentNameStr).getTableName();
    Assert.assertNull(commitTimeMap.get(tableName));
    segmentCompletionMgr._seconds = startTime;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3)
        .withStreamPartitionMsgOffset(s3Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    verifyOffset(response, s2Offset);
    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    // s2 is asked to commit.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
    long commitTimeSec = response.getBuildTimeSeconds();
    Assert.assertTrue(commitTimeSec > 0);

    // Fast forward to one second before commit time, and send a lease renewal request for 20s
    segmentCompletionMgr._seconds = startTime + commitTimeSec - 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr)
        .withExtraTimeSec(20);
    response = segmentCompletionMgr.extendBuildTime(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.PROCESSED);
    Assert.assertTrue((fsmMap.containsKey(segmentNameStr)));

    // Another lease extension in 19s.
    segmentCompletionMgr._seconds += 19;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr)
        .withExtraTimeSec(20);
    response = segmentCompletionMgr.extendBuildTime(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.PROCESSED);
    Assert.assertTrue((fsmMap.containsKey(segmentNameStr)));

    // Commit in 15s
    segmentCompletionMgr._seconds += 15;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);
    long commitTimeMs = (segmentCompletionMgr._seconds - startTime) * 1000;
    Assert.assertEquals(commitTimeMap.get(tableName).longValue(), commitTimeMs);
    segmentCompletionMgr._seconds += 55;
    response = segmentCompletionMgr.segmentCommitEnd(params, true, false,
        CommittingSegmentDescriptor.fromSegmentCompletionReqParams(params));
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
    // now FSM should be out of the map.
    Assert.assertFalse((fsmMap.containsKey(segmentNameStr)));
  }

  @Test
  public void testFailedSlowCommit() throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    final String tableName = new LLCSegmentName(segmentNameStr).getTableName();
    // s1 sends offset of 20, gets HOLD at t = 5s;
    final long startTime = 5;
    segmentCompletionMgr._seconds = startTime;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3)
        .withStreamPartitionMsgOffset(s3Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    verifyOffset(response, s2Offset);
    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    // s2 is asked to commit.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
    long commitTimeSec = response.getBuildTimeSeconds();
    Assert.assertTrue(commitTimeSec > 0);

    // Fast forward to one second before commit time, and send a lease renewal request for 20s
    segmentCompletionMgr._seconds = startTime + commitTimeSec - 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr)
        .withExtraTimeSec(20);
    response = segmentCompletionMgr.extendBuildTime(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.PROCESSED);
    Assert.assertTrue((fsmMap.containsKey(segmentNameStr)));

    // Come back too late.
    segmentCompletionMgr._seconds += 25;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.HOLD);
    // now FSM should be out of the map.
    Assert.assertFalse((fsmMap.containsKey(segmentNameStr)));
    Assert.assertFalse(commitTimeMap.containsKey(tableName));
  }

  @Test
  public void testLeaseTooLong() throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    final long startTime = 5;
    segmentCompletionMgr._seconds = startTime;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3)
        .withStreamPartitionMsgOffset(s3Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    verifyOffset(response, s2Offset);
    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    // s2 is asked to commit.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
    long commitTimeSec = response.getBuildTimeSeconds();
    Assert.assertTrue(commitTimeSec > 0);

    // Fast forward to one second before commit time, and send a lease renewal request for 20s
    segmentCompletionMgr._seconds = startTime + commitTimeSec - 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr)
        .withExtraTimeSec(20);
    response = segmentCompletionMgr.extendBuildTime(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.PROCESSED);
    Assert.assertTrue((fsmMap.containsKey(segmentNameStr)));

    final int leaseTimeSec = 20;
    // Lease will not be granted if the time taken so far plus lease time exceeds the max allowabale.
    while (segmentCompletionMgr._seconds + leaseTimeSec
        <= startTime + SegmentCompletionManager.getMaxCommitTimeForAllSegmentsSeconds()) {
      params = new Request.Params().withInstanceId(s2)
          .withStreamPartitionMsgOffset(s2Offset.toString())
          .withSegmentName(segmentNameStr)
          .withExtraTimeSec(leaseTimeSec);
      response = segmentCompletionMgr.extendBuildTime(params);
      Assert.assertEquals(response.getStatus(), ControllerResponseStatus.PROCESSED);
      Assert.assertTrue((fsmMap.containsKey(segmentNameStr)));
      segmentCompletionMgr._seconds += leaseTimeSec;
    }

    // Now the lease request should fail.
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr)
        .withExtraTimeSec(leaseTimeSec);
    response = segmentCompletionMgr.extendBuildTime(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.FAILED);
    Assert.assertFalse((fsmMap.containsKey(segmentNameStr)));
  }

  @Test
  public void testControllerFailureDuringCommit() throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    segmentCompletionMgr._seconds = 5;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3)
        .withStreamPartitionMsgOffset(s3Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    verifyOffset(response, s2Offset);
    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    // s2 is asked to commit.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    // Now the controller fails, and a new one takes over, with no knowledge of what was done before.
    replaceSegmentCompletionManager();

    // s3 comes back with the correct offset but is asked to hold.
    params = new Request.Params().withInstanceId(s3)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // s1 comes back, and still asked to hold.
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // s2 has no idea the controller failed, so it comes back with a commit,but the controller asks it to hold,
    // (essentially a commit failure)
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertTrue(response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.HOLD));

    // So s2 goes back into HOLDING state. s1 and s3 are already holding, so now it will get COMMIT back.
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
  }

  // Tests that when controller fails, after controller failure, commit doesn't continue. Then because all server instances
  // are in holding state, we will ask one to commit.
  @Test
  public void testControllerFailureDuringSplitCommit() throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    segmentCompletionMgr._seconds = 5;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s3)
        .withStreamPartitionMsgOffset(s3Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    verifyOffset(response, s2Offset);
    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    // s2 is asked to commit.
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    // Now the controller fails, and a new one takes over, with no knowledge of what was done before.
    replaceSegmentCompletionManager();

    // s3 comes back with the correct offset but is asked to hold.
    params = new Request.Params().withInstanceId(s3)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // s1 comes back, and still asked to hold.
    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // s2 has no idea the controller failed, so it comes back with a commit,but the controller asks it to hold,
    // (essentially a commit failure)
    segmentCompletionMgr._seconds += 1;
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertTrue(response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.HOLD));

    // So s2 goes back into HOLDING state. s1 and s3 are already holding, so now it will get COMMIT back.
    params = new Request.Params().withInstanceId(s2)
        .withStreamPartitionMsgOffset(s2Offset.toString())
        .withSegmentName(segmentNameStr)
        .withSegmentLocation("location");
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
  }

  @Test
  public void testNotLeader() throws Exception {
    testCaseSetup(false, true);
    SegmentCompletionProtocol.Response response;
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();

    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER);

    params = new Request.Params().withInstanceId(s1)
        .withStreamPartitionMsgOffset(s1Offset.toString())
        .withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER);
  }

  private static HelixManager createMockHelixManager(boolean isLeader, boolean isConnected) {
    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.isLeader()).thenReturn(isLeader);
    when(helixManager.isConnected()).thenReturn(isConnected);
    return helixManager;
  }

  public static class MockPinotLLCRealtimeSegmentManager extends PinotLLCRealtimeSegmentManager {
    public LLCRealtimeSegmentZKMetadata _segmentMetadata;
    public MockSegmentCompletionManager _segmentCompletionMgr;
    private static final ControllerConf CONTROLLER_CONF = new ControllerConf();
    public LLCSegmentName _stoppedSegmentName;
    public String _stoppedInstance;
    public HelixManager _helixManager = mock(HelixManager.class);

    protected MockPinotLLCRealtimeSegmentManager(PinotHelixResourceManager pinotHelixResourceManager) {
      this(pinotHelixResourceManager, new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
    }

    protected MockPinotLLCRealtimeSegmentManager(PinotHelixResourceManager pinotHelixResourceManager,
        ControllerMetrics controllerMetrics) {
      super(pinotHelixResourceManager, CONTROLLER_CONF, controllerMetrics);
    }

    @Override
    public LLCRealtimeSegmentZKMetadata getSegmentZKMetadata(String realtimeTableName, String segmentName, Stat stat) {
      return _segmentMetadata;
    }

    @Override
    public void commitSegmentMetadata(String rawTableName, CommittingSegmentDescriptor committingSegmentDescriptor) {
      _segmentMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
      _segmentMetadata.setEndOffset(committingSegmentDescriptor.getNextOffset());
      _segmentMetadata.setDownloadUrl(committingSegmentDescriptor.getSegmentLocation());
      _segmentMetadata.setEndTime(_segmentCompletionMgr.getCurrentTimeMs());
    }

    @Override
    public void commitSegmentFile(String rawTableName, CommittingSegmentDescriptor committingSegmentDescriptor) {
      Preconditions.checkState(!committingSegmentDescriptor.getSegmentLocation().equals("doNotCommitMe"));
    }

    @Override
    public void segmentStoppedConsuming(final LLCSegmentName llcSegmentName, final String instanceName) {
      _stoppedSegmentName = llcSegmentName;
      _stoppedInstance = instanceName;
    }
  }

  public static class MockSegmentCompletionManager extends SegmentCompletionManager {
    public long _seconds;
    private boolean _isLeader;

    protected MockSegmentCompletionManager(PinotLLCRealtimeSegmentManager segmentManager, boolean isLeader,
        boolean isConnected) {
      this(createMockHelixManager(isLeader, isConnected), segmentManager, isLeader);
    }

    protected MockSegmentCompletionManager(HelixManager helixManager, PinotLLCRealtimeSegmentManager segmentManager,
        boolean isLeader) {
      this(helixManager, segmentManager, isLeader, new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
    }

    protected MockSegmentCompletionManager(HelixManager helixManager, PinotLLCRealtimeSegmentManager segmentManager,
        boolean isLeader, ControllerMetrics controllerMetrics) {
      super(helixManager, segmentManager, controllerMetrics,
          new LeadControllerManager("localhost_1234", helixManager, controllerMetrics),
          SegmentCompletionProtocol.getDefaultMaxSegmentCommitTimeSeconds());
      _isLeader = isLeader;
    }

    @Override
    protected StreamPartitionMsgOffsetFactory getStreamPartitionMsgOffsetFactory(LLCSegmentName llcSegmentName) {
      return new LongMsgOffsetFactory();
    }

    public StreamPartitionMsgOffsetFactory getStreamPartitionMsgOffsetFactory(String segmentName) {
      return new LongMsgOffsetFactory();
    }

    @Override
    protected long getCurrentTimeMs() {
      return _seconds * 1000L;
    }

    @Override
    protected boolean isLeader(String tableName) {
      return _isLeader;
    }
  }
}
