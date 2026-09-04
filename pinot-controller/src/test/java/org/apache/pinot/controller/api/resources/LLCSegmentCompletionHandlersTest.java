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
package org.apache.pinot.controller.api.resources;

import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionManager;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class LLCSegmentCompletionHandlersTest {
  private static final String INSTANCE_ID = "Server_localhost_8099";
  private static final String SEGMENT_NAME = "foo__0__0__12345Z";
  private static final String OFFSET = "7000";

  @Test
  public void testSegmentCommitStartBindsReasonAndReasonCode() {
    SegmentCompletionManager segmentCompletionManager = mock(SegmentCompletionManager.class);
    when(segmentCompletionManager.segmentCommitStart(any())).thenReturn(SegmentCompletionProtocol.RESP_COMMIT_CONTINUE);
    LLCSegmentCompletionHandlers handler = new LLCSegmentCompletionHandlers();
    handler._segmentCompletionManager = segmentCompletionManager;

    handler.segmentCommitStart(INSTANCE_ID, SEGMENT_NAME, OFFSET, 4000, 1000, 2000, 6000, 5000,
        "legacyReason", "100");

    SegmentCompletionProtocol.Request.Params params = captureSegmentCommitStartParams(segmentCompletionManager);
    Assert.assertEquals(params.getReason(), SegmentCompletionProtocol.REASON_ROW_LIMIT);
    Assert.assertEquals(params.getReasonCode(), SegmentCompletionProtocol.ReasonCode.ROW_LIMIT);
  }

  @Test
  public void testSegmentCommitStartFallsBackToLegacyReasonForUnknownCode() {
    SegmentCompletionManager segmentCompletionManager = mock(SegmentCompletionManager.class);
    when(segmentCompletionManager.segmentCommitStart(any())).thenReturn(SegmentCompletionProtocol.RESP_COMMIT_CONTINUE);
    LLCSegmentCompletionHandlers handler = new LLCSegmentCompletionHandlers();
    handler._segmentCompletionManager = segmentCompletionManager;

    handler.segmentCommitStart(INSTANCE_ID, SEGMENT_NAME, OFFSET, 4000, 1000, 2000, 6000, 5000,
        SegmentCompletionProtocol.REASON_TIME_LIMIT, "999");

    SegmentCompletionProtocol.Request.Params params = captureSegmentCommitStartParams(segmentCompletionManager);
    Assert.assertEquals(params.getReason(), SegmentCompletionProtocol.REASON_TIME_LIMIT);
    Assert.assertEquals(params.getReasonCode(), SegmentCompletionProtocol.ReasonCode.TIME_LIMIT);
  }

  @Test
  public void testSegmentConsumedPrefersKnownReasonCodeOverLegacyReason() {
    SegmentCompletionManager segmentCompletionManager = mock(SegmentCompletionManager.class);
    when(segmentCompletionManager.segmentConsumed(any())).thenReturn(SegmentCompletionProtocol.RESP_FAILED);
    LLCSegmentCompletionHandlers handler = new LLCSegmentCompletionHandlers();
    handler._segmentCompletionManager = segmentCompletionManager;

    handler.segmentConsumed(INSTANCE_ID, SEGMENT_NAME, OFFSET, "legacyReason", "100", 4000, 6000);

    ArgumentCaptor<SegmentCompletionProtocol.Request.Params> paramsCaptor =
        ArgumentCaptor.forClass(SegmentCompletionProtocol.Request.Params.class);
    verify(segmentCompletionManager).segmentConsumed(paramsCaptor.capture());
    SegmentCompletionProtocol.Request.Params params = paramsCaptor.getValue();
    Assert.assertEquals(params.getReason(), SegmentCompletionProtocol.REASON_ROW_LIMIT);
    Assert.assertEquals(params.getReasonCode(), SegmentCompletionProtocol.ReasonCode.ROW_LIMIT);
  }

  private static SegmentCompletionProtocol.Request.Params captureSegmentCommitStartParams(
      SegmentCompletionManager segmentCompletionManager) {
    ArgumentCaptor<SegmentCompletionProtocol.Request.Params> paramsCaptor =
        ArgumentCaptor.forClass(SegmentCompletionProtocol.Request.Params.class);
    verify(segmentCompletionManager).segmentCommitStart(paramsCaptor.capture());
    return paramsCaptor.getValue();
  }
}
