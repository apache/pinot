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

package com.linkedin.pinot.controller.helix.core.realtime;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.controller.ControllerConf;
import com.yammer.metrics.core.MetricsRegistry;
import static com.linkedin.pinot.common.protocols.SegmentCompletionProtocol.ControllerResponseStatus;
import static com.linkedin.pinot.common.protocols.SegmentCompletionProtocol.Request;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SegmentCompletionTest {
  private MockPinotLLCRealtimeSegmentManager segmentManager;
  private MockSegmentCompletionManager segmentCompletionMgr;
  private Map<String, Object> fsmMap;
  private Map<String, Long> commitTimeMap;
  private String segmentNameStr;
  private final String s1 = "S1";
  private final String s2 = "S2";
  private final String s3 = "S3";

  private final long s1Offset = 20L;
  private final long s2Offset = 40L;
  private final long s3Offset = 30L;

  @BeforeMethod
  public void testCaseSetup() throws Exception {
    testCaseSetup(true, true);
  }

  public void testCaseSetup(boolean isLeader, boolean isConnected) throws Exception {
    segmentManager = new MockPinotLLCRealtimeSegmentManager();
    final int partitionId = 23;
    final int seqId = 12;
    final long now = System.currentTimeMillis();
    final String tableName = "someTable";
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
    fsmMap = (Map<String, Object>)fsmMapField.get(segmentCompletionMgr);

    Field ctMapField = SegmentCompletionManager.class.getDeclaredField("_commitTimeMap");
    ctMapField.setAccessible(true);
    commitTimeMap = (Map<String, Long>)ctMapField.get(segmentCompletionMgr);
  }

  // Simulate a new controller taking over with an empty completion manager object,
  // but segment metadata is fine in zk
  private void replaceSegmentCompletionManager() throws Exception {
    long oldSecs = segmentCompletionMgr._secconds;
    segmentCompletionMgr = new MockSegmentCompletionManager(segmentManager, true, true);
    segmentCompletionMgr._secconds = oldSecs;
    Field fsmMapField = SegmentCompletionManager.class.getDeclaredField("_fsmMap");
    fsmMapField.setAccessible(true);
    fsmMap = (Map<String, Object>)fsmMapField.get(segmentCompletionMgr);
  }

  @Test
  public void testStoppedConsumeDuringCompletion() throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    final String reason = "IAmLazy";

    // s1 sends offset of 20, gets HOLD at t = 5s;
    segmentCompletionMgr._secconds = 5;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s3Offset).withSegmentName(segmentNameStr).withReason(reason);
    response = segmentCompletionMgr.segmentStoppedConsuming(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.PROCESSED);
    Assert.assertEquals(new LLCSegmentName(segmentNameStr), segmentManager._stoppedSegmentName);
    Assert.assertEquals(s3, segmentManager._stoppedInstance);
    segmentManager._stoppedSegmentName = null;
    segmentManager._stoppedInstance = null;

    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    // s2 is asked to commit.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
    // s3 comes back with new caught up offset, it should get a HOLD, since commit is not done yet.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 executes a succesful commit
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);

    segmentCompletionMgr._secconds += 5;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitEnd(params, true, false);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);

    // Now the FSM should have disappeared from the map
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // Now if s3 or s1 come back, they are asked to keep the segment they have.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
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
    segmentCompletionMgr._secconds = 5;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr).withReason(
        reason);
    response = segmentCompletionMgr.segmentStoppedConsuming(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.PROCESSED);
    Assert.assertEquals(new LLCSegmentName(segmentNameStr), segmentManager._stoppedSegmentName);
    Assert.assertEquals(s1, segmentManager._stoppedInstance);
    segmentManager._stoppedSegmentName = null;
    segmentManager._stoppedInstance = null;

    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // s3 sends offset of 30, gets catchup to 40, s2 should have been decided as the winner now
    // since we are never expecting to hear back from s1
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s3Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    Assert.assertEquals(response.getOffset(), s2Offset);

    // s3 happens to come back (after catchup to s2offset) before s2 should get a hold since s2 has been decided as
    // the winner.
    // TODO Can optimize here since s2 is not notified yet.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    Assert.assertEquals(response.getOffset(), s2Offset);
    // s2 is asked to commit.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    // s2 executes a succesful commit
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);

    segmentCompletionMgr._secconds += 5;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitEnd(params, true, false);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);

    // Now the FSM should have disappeared from the map
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // Now if s3 or s1 come back, they are asked to keep the segment they have.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
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
    segmentCompletionMgr._secconds = 5;

    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr).withReason("some reason");
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
  public void testHappyPathSplitCommit() throws Exception {
    testHappyPathSplitCommit(5L);
  }

  @Test
  public void testExceptionInConsumedMessage() throws Exception {
    segmentManager._segmentMetadata = null;

    SegmentCompletionProtocol.Response response;
    Request.Params params;
    segmentCompletionMgr._secconds = 10;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.FAILED);
  }

  // When commit segment file fails, makes sure the fsm aborts and that a segment can successfully commit afterwards.
  @Test
  public void testCommitSegmentFileFail() throws  Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    segmentCompletionMgr._secconds = 5L;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s3Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    Assert.assertEquals(response.getOffset(), s2Offset);
    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    Assert.assertEquals(response.getOffset(), s2Offset);
    // s2 is asked to commit.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    // TODO: Verify controller asked to do a split commit
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
    // s3 comes back with new caught up offset, it should get a HOLD, since commit is not done yet.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 executes a succesful commit start
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);
    // s2's file does not successfully commit
    segmentCompletionMgr._secconds += 5;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr).withSegmentLocation("doNotCommitMe");
    response = segmentCompletionMgr.segmentCommitEnd(params, true, true);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.FAILED);

    // Now the FSM should have aborted
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // Now s1 comes back; it is asked to hold.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s1).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // Now s3 comes back; it is asked to commit
    segmentCompletionMgr._secconds += 5;
    params = new Request.Params().withInstanceId(s3).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.COMMIT);

    // Now s2 comes back; it is asked to hold
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.HOLD);

    // s3 executes a successful commit start
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);
    // s2's file successfully commits
    segmentCompletionMgr._secconds += 5;
    params = new Request.Params().withInstanceId(s3).withOffset(s2Offset).withSegmentName(segmentNameStr).withSegmentLocation("location");
    response = segmentCompletionMgr.segmentCommitEnd(params, true, true);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.COMMIT_SUCCESS);
    // And the FSM should be removed.
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));
  }

  public void testHappyPathSplitCommit(long startTime) throws  Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    segmentCompletionMgr._secconds = startTime;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s3Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    Assert.assertEquals(response.getOffset(), s2Offset);
    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    Assert.assertEquals(response.getOffset(), s2Offset);
    // s2 is asked to commit.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    // TODO: Verify controller asked to do a split commit
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
    // s3 comes back with new caught up offset, it should get a HOLD, since commit is not done yet.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 executes a succesful commit
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);

    segmentCompletionMgr._secconds += 5;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr).withSegmentLocation("location");
    response = segmentCompletionMgr.segmentCommitEnd(params, true, true);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);

    // Now the FSM should have disappeared from the map
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // Now if s3 or s1 come back, they are asked to keep the segment they have.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
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
    segmentCompletionMgr._secconds = 5L;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 20, gets HOLD
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 20, gets commit
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.COMMIT);
    Assert.assertEquals(response.getOffset(), s1Offset);

    // s3 sends a commit start with 20
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);

    // s3 comes back to try to commit with a different offset
    segmentCompletionMgr._secconds += 5;
    params = new Request.Params().withInstanceId(s3).withOffset(s3Offset).withSegmentName(segmentNameStr).withSegmentLocation("location");
    response = segmentCompletionMgr.segmentCommitEnd(params, true, true);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.FAILED);

    // Now the FSM should have disappeared from the map
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // Now if s3 or s1 come back, they are asked to hold.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // FSM should still be there for that segment
    Assert.assertTrue(fsmMap.containsKey(segmentNameStr));
  }

  public void testHappyPath(long startTime) throws  Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    segmentCompletionMgr._secconds = startTime;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s3Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    Assert.assertEquals(response.getOffset(), s2Offset);
    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    // s2 is asked to commit.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
    // s3 comes back with new caught up offset, it should get a HOLD, since commit is not done yet.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 executes a succesful commit
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);

    segmentCompletionMgr._secconds += 5;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitEnd(params, true, false);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);

    // Now the FSM should have disappeared from the map
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // Now if s3 or s1 come back, they are asked to keep the segment they have.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
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
    segmentCompletionMgr._secconds = 5L;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr)
    .withReason("rowLimit");
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.NOT_LEADER);

  }

  @Test
  public void testWinnerOnTimeLimit() throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    segmentCompletionMgr._secconds = 10L;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr)
        .withReason(SegmentCompletionProtocol.REASON_TIME_LIMIT);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
  }

  @Test
  public void testWinnerOnRowLimit() throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    segmentCompletionMgr._secconds = 10L;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr)
        .withReason(SegmentCompletionProtocol.REASON_ROW_LIMIT);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.COMMIT);
    // S2 comes with the same offset as S1
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s1Offset).withSegmentName(segmentNameStr)
        .withReason(SegmentCompletionProtocol.REASON_ROW_LIMIT);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.HOLD);
    segmentCompletionMgr._secconds += 1;
    // S3 comes with a different offset and without row limit. we ask it to hold even though it is higher.
    params = new Request.Params().withInstanceId(s3).withOffset(s3Offset).withSegmentName(segmentNameStr)
        .withReason(SegmentCompletionProtocol.REASON_TIME_LIMIT);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.HOLD);
    // S1 comes back to commit the segment
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);

    segmentCompletionMgr._secconds += 5;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitEnd(params, true, false);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
    // We ask S2 to keep the segment
    params = new Request.Params().withInstanceId(s2).withOffset(s1Offset).withSegmentName(segmentNameStr)
        .withReason(SegmentCompletionProtocol.REASON_ROW_LIMIT);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.KEEP);
    // And we ask S3 to discard because it was ahead.
    params = new Request.Params().withInstanceId(s3).withOffset(s3Offset).withSegmentName(segmentNameStr)
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
    segmentCompletionMgr._secconds = startTimeSecs;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // Now s1 comes back again, and is asked to hold
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 is asked to commit.
    segmentCompletionMgr._secconds += SegmentCompletionProtocol.MAX_HOLD_TIME_MS/1000;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    // Now s3 comes up with a better offset, but we ask it to hold, since the committer has not committed yet.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s2Offset + 10).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 commits.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);
    segmentCompletionMgr._secconds += 5;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr).withSegmentLocation("location");
    response = segmentCompletionMgr.segmentCommitEnd(params, true, isSplitCommit);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
    // Now the FSM should have disappeared from the map
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // Now s3 comes back to get a discard.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s2Offset + 10).withSegmentName(segmentNameStr);
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
    segmentCompletionMgr._secconds = startTimeSecs;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 is asked to hole
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s3Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    Assert.assertEquals(response.getOffset(), s2Offset);

    // Now s2 is asked to commit.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    // All servers are dead
    segmentCompletionMgr._secconds += 3600;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.HOLD);
    Assert.assertTrue(fsmMap.containsKey(segmentNameStr));

    segmentCompletionMgr._secconds += 4;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
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
    segmentCompletionMgr._secconds = startTimeSecs;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 is asked to hole
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s3Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    Assert.assertEquals(response.getOffset(), s2Offset);
    // Now s2 is asked to commit.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    // Time passes, s2 never comes back.
    segmentCompletionMgr._secconds += SegmentCompletionProtocol.MAX_HOLD_TIME_MS/1000;

    // But since s1 and s3 are in HOLDING state, they should come back again. s1 is asked to catchup
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);

    // Max time to commit passes
    segmentCompletionMgr._secconds += SegmentCompletionProtocol.getMaxSegmentCommitTimeMs() * SegmentCompletionProtocol.MAX_HOLD_TIME_MS/1000;

    // s1 comes back with the updated offset, since it was asked to catch up.
    // The FSM will be aborted, and destroyed ...
    params = new Request.Params().withInstanceId(s1).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // s1 comes back again, a new FSM created
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s1).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // s3 comes back
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // And winner chosen when the last one does not come back at all
    segmentCompletionMgr._secconds += 5;
    params = new Request.Params().withInstanceId(s3).withOffset(s2Offset).withSegmentName(segmentNameStr);
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
    segmentCompletionMgr._secconds = startTime;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s3Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    Assert.assertEquals(response.getOffset(), s2Offset);
    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    // s2 is asked to commit.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
    long commitTimeSec = response.getBuildTimeSeconds();
    Assert.assertTrue(commitTimeSec > 0);

    // Fast forward to one second before commit time, and send a lease renewal request for 20s
    segmentCompletionMgr._secconds = startTime + commitTimeSec - 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr).withExtraTimeSec(20);
    response = segmentCompletionMgr.extendBuildTime(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.PROCESSED);
    Assert.assertTrue((fsmMap.containsKey(segmentNameStr)));

    // Another lease extension in 19s.
    segmentCompletionMgr._secconds += 19;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr).withExtraTimeSec(
        20);
    response = segmentCompletionMgr.extendBuildTime(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.PROCESSED);
    Assert.assertTrue((fsmMap.containsKey(segmentNameStr)));

    // Commit in 15s
    segmentCompletionMgr._secconds += 15;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);
    long commitTimeMs = (segmentCompletionMgr._secconds - startTime) * 1000;
    Assert.assertEquals(commitTimeMap.get(tableName).longValue(), commitTimeMs);
    segmentCompletionMgr._secconds += 55;
    response = segmentCompletionMgr.segmentCommitEnd(params, true, false);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
    // now FSM should be out of the map.
    Assert.assertFalse((fsmMap.containsKey(segmentNameStr)));
  }

  @Test
  public void testFailedSlowCommit() throws Exception
  {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    final String tableName = new LLCSegmentName(segmentNameStr).getTableName();
    // s1 sends offset of 20, gets HOLD at t = 5s;
    final long startTime = 5;
    segmentCompletionMgr._secconds = startTime;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s3Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    Assert.assertEquals(response.getOffset(), s2Offset);
    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    // s2 is asked to commit.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
    long commitTimeSec = response.getBuildTimeSeconds();
    Assert.assertTrue(commitTimeSec > 0);

    // Fast forward to one second before commit time, and send a lease renewal request for 20s
    segmentCompletionMgr._secconds = startTime + commitTimeSec - 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr).withExtraTimeSec(
        20);
    response = segmentCompletionMgr.extendBuildTime(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.PROCESSED);
    Assert.assertTrue((fsmMap.containsKey(segmentNameStr)));

    // Come back too late.
    segmentCompletionMgr._secconds += 25;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
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
    segmentCompletionMgr._secconds = startTime;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s3Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    Assert.assertEquals(response.getOffset(), s2Offset);
    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    // s2 is asked to commit.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
    long commitTimeSec = response.getBuildTimeSeconds();
    Assert.assertTrue(commitTimeSec > 0);

    // Fast forward to one second before commit time, and send a lease renewal request for 20s
    segmentCompletionMgr._secconds = startTime + commitTimeSec - 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr).withExtraTimeSec(
        20);
    response = segmentCompletionMgr.extendBuildTime(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.PROCESSED);
    Assert.assertTrue((fsmMap.containsKey(segmentNameStr)));

    final int leaseTimeSec = 20;
    // Lease will not be granted if the time taken so far plus lease time exceeds the max allowabale.
    while (segmentCompletionMgr._secconds + leaseTimeSec <= startTime + SegmentCompletionManager.getMaxCommitTimeForAllSegmentsSeconds()) {
      params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr).withExtraTimeSec(
          leaseTimeSec);
      response = segmentCompletionMgr.extendBuildTime(params);
      Assert.assertEquals(response.getStatus(), ControllerResponseStatus.PROCESSED);
      Assert.assertTrue((fsmMap.containsKey(segmentNameStr)));
      segmentCompletionMgr._secconds += leaseTimeSec;
    }

    // Now the lease request should fail.
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr)
        .withExtraTimeSec(
        leaseTimeSec);
    response = segmentCompletionMgr.extendBuildTime(params);
    Assert.assertEquals(response.getStatus(), ControllerResponseStatus.FAILED);
    Assert.assertFalse((fsmMap.containsKey(segmentNameStr)));
  }

  @Test
  public void testControllerFailureDuringCommit() throws Exception {
    SegmentCompletionProtocol.Response response;
    Request.Params params;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    segmentCompletionMgr._secconds = 5;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s3Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    Assert.assertEquals(response.getOffset(), s2Offset);
    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    // s2 is asked to commit.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    // Now the controller fails, and a new one takes over, with no knowledge of what was done before.
    replaceSegmentCompletionManager();

    // s3 comes back with the correct offset but is asked to hold.
    params = new Request.Params().withInstanceId(s3).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // s1 comes back, and still asked to hold.
    params = new Request.Params().withInstanceId(s1).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // s2 has no idea the controller failed, so it comes back with a commit,but the controller asks it to hold,
    // (essentially a commit failure)
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertTrue(response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.HOLD));

    // So s2 goes back into HOLDING state. s1 and s3 are already holding, so now it will get COMMIT back.
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
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
    segmentCompletionMgr._secconds = 5;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s3).withOffset(s3Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    Assert.assertEquals(response.getOffset(), s2Offset);
    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    // s2 is asked to commit.
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    // Now the controller fails, and a new one takes over, with no knowledge of what was done before.
    replaceSegmentCompletionManager();

    // s3 comes back with the correct offset but is asked to hold.
    params = new Request.Params().withInstanceId(s3).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // s1 comes back, and still asked to hold.
    params = new Request.Params().withInstanceId(s1).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // s2 has no idea the controller failed, so it comes back with a commit,but the controller asks it to hold,
    // (essentially a commit failure)
    segmentCompletionMgr._secconds += 1;
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentCommitStart(params);
    Assert.assertTrue(response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.HOLD));

    // So s2 goes back into HOLDING state. s1 and s3 are already holding, so now it will get COMMIT back.
    params = new Request.Params().withInstanceId(s2).withOffset(s2Offset).withSegmentName(segmentNameStr).withSegmentLocation("location");
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
  }

  @Test
  public void testNotLeader() throws Exception {
    testCaseSetup(false, true);
    SegmentCompletionProtocol.Response response;
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();

    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
    response = segmentCompletionMgr.segmentConsumed(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER);

    params = new Request.Params().withInstanceId(s1).withOffset(s1Offset).withSegmentName(segmentNameStr);
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
    public static final String clusterName = "someCluster";
    public LLCRealtimeSegmentZKMetadata _segmentMetadata;
    public MockSegmentCompletionManager _segmentCompletionMgr;
    private static final ControllerConf CONTROLLER_CONF = new ControllerConf();
    public LLCSegmentName _stoppedSegmentName;
    public String _stoppedInstance;

    protected MockPinotLLCRealtimeSegmentManager() {
      super(null, clusterName, null, null, null, CONTROLLER_CONF, new ControllerMetrics(new MetricsRegistry()));
    }

    @Override
    public LLCRealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(String realtimeTableName, String segmentName, Stat stat) {
      return _segmentMetadata;
    }

    @Override
         public boolean commitSegmentMetadata(String rawTableName, String committingSegmentName, long nextOffset,
        long memoryUsedBytes) {
      _segmentMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
      _segmentMetadata.setEndOffset(nextOffset);
      _segmentMetadata.setDownloadUrl(
          ControllerConf.constructDownloadUrl(rawTableName, committingSegmentName, CONTROLLER_CONF.generateVipUrl()));
      _segmentMetadata.setEndTime(_segmentCompletionMgr.getCurrentTimeMs());
      return true;
    }

    @Override
    public boolean commitSegmentFile(String rawTableName, String segmentLocation, String segmentName) {
      if (segmentLocation.equals("doNotCommitMe")) {
        return false;
      } else {
        return true;
      }
    }

    @Override
    protected void writeSegmentsToPropertyStore(List<String> paths, List<ZNRecord> records, String realtimeTableName) {
      _segmentMetadata = new LLCRealtimeSegmentZKMetadata(records.get(0));  // Updated record that we are writing to ZK
    }

    @Override
    public void segmentStoppedConsuming(final LLCSegmentName segmentName, final String instance) {
      _stoppedSegmentName = segmentName;
      _stoppedInstance = instance;
    }
  }

  public static class MockSegmentCompletionManager extends SegmentCompletionManager {
    public long _secconds;
    protected MockSegmentCompletionManager(PinotLLCRealtimeSegmentManager segmentManager, boolean isLeader,
        boolean isConnected) {
      super(createMockHelixManager(isLeader, isConnected), segmentManager, new ControllerMetrics(new MetricsRegistry()));
    }
    @Override
    protected long getCurrentTimeMs() {
      return _secconds * 1000L;
    }
  }
}
