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

import java.util.Collections;
import java.util.Map;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.LongMsgOffsetFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.helix.core.realtime.SegmentCompletionConfig.DEFAULT_FSM_SCHEME_KEY;
import static org.apache.pinot.controller.helix.core.realtime.SegmentCompletionConfig.DEFAULT_PAUSELESS_FSM_SCHEME_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SegmentCompletionFSMFactoryTest {
  private static final String DEFAULT_FSM_CLASS = BlockingSegmentCompletionFSM.class.getName();
  private static final String LLC_SEGMENT_NAME = "tableName__0__0__20250228T1903Z";

  @AfterMethod
  public void tearDown() {
    // Clear the factory's static state between tests
    SegmentCompletionFSMFactory.shutdown();

    // Re-register the default mappings
    SegmentCompletionFSMFactory.register(SegmentCompletionConfig.DEFAULT_FSM_SCHEME,
        BlockingSegmentCompletionFSM.class);
    SegmentCompletionFSMFactory.register(SegmentCompletionConfig.DEFAULT_PAUSELESS_FSM_SCHEME,
        PauselessSegmentCompletionFSM.class);
  }

  @Test
  public void testCreateFSMWithDefaultFsm() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration(Collections.emptyMap());
    SegmentCompletionConfig segmentCompletionConfig = new SegmentCompletionConfig(pinotConfiguration);
    SegmentCompletionFSMFactory.init(segmentCompletionConfig);

    // mock different objects that are required for creating FSM
    SegmentCompletionManager segmentCompletionManager = mock(SegmentCompletionManager.class);
    LongMsgOffsetFactory longMsgOffsetFactory = mock(LongMsgOffsetFactory.class);
    when(segmentCompletionManager.getCurrentTimeMs()).thenReturn(System.currentTimeMillis());
    when(segmentCompletionManager.getStreamPartitionMsgOffsetFactory(any())).thenReturn(longMsgOffsetFactory);
    when(longMsgOffsetFactory.create(anyString())).thenReturn(new LongMsgOffset(100));

    SegmentZKMetadata segmentZKMetadata = mock(SegmentZKMetadata.class);
    when(segmentZKMetadata.getNumReplicas()).thenReturn(3);
    when(segmentZKMetadata.getEndOffset()).thenReturn("100");

    PinotLLCRealtimeSegmentManager pinotLLCRealtimeSegmentManager = mock(PinotLLCRealtimeSegmentManager.class);
    when(pinotLLCRealtimeSegmentManager.getCommitTimeoutMS(anyString())).thenReturn(System.currentTimeMillis());

    // Asset that the default fsm (BlockingSegmentCompletionFSM) is picked when the "default" scheme is passed
    Assert.assertTrue(
        SegmentCompletionFSMFactory.createFSM(segmentCompletionConfig.getDefaultFsmScheme(), segmentCompletionManager,
            pinotLLCRealtimeSegmentManager, new LLCSegmentName(LLC_SEGMENT_NAME),
            segmentZKMetadata) instanceof BlockingSegmentCompletionFSM);

    // Asset that the default pauseless fsm (PauselessSegmentCompletionFSM) is picked when the "pauseless" scheme is
    // passed
    Assert.assertTrue(SegmentCompletionFSMFactory.createFSM(segmentCompletionConfig.getDefaultPauselessFsmScheme(),
        segmentCompletionManager, pinotLLCRealtimeSegmentManager, new LLCSegmentName(LLC_SEGMENT_NAME),
        segmentZKMetadata) instanceof PauselessSegmentCompletionFSM);
  }

  @Test
  public void testCreateFSMWithCustomProvidedFsm() {

    String fakeCustomeFsmClass =
        "org.apache.pinot.controller.helix.core.realtime.SegmentCompletionFSMFactoryTest$FakeCustomFSM";

    PinotConfiguration pinotConfiguration = new PinotConfiguration(
        Map.of(DEFAULT_PAUSELESS_FSM_SCHEME_KEY, fakeCustomeFsmClass, DEFAULT_FSM_SCHEME_KEY, DEFAULT_FSM_CLASS));

    // mock different objects that are required for creating FSM
    SegmentCompletionConfig segmentCompletionConfig = new SegmentCompletionConfig(pinotConfiguration);
    SegmentCompletionFSMFactory.init(segmentCompletionConfig);

    SegmentCompletionManager segmentCompletionManager = mock(SegmentCompletionManager.class);
    LongMsgOffsetFactory longMsgOffsetFactory = mock(LongMsgOffsetFactory.class);
    when(segmentCompletionManager.getCurrentTimeMs()).thenReturn(System.currentTimeMillis());
    when(segmentCompletionManager.getStreamPartitionMsgOffsetFactory(any())).thenReturn(longMsgOffsetFactory);
    when(longMsgOffsetFactory.create(anyString())).thenReturn(new LongMsgOffset(100));

    SegmentZKMetadata segmentZKMetadata = mock(SegmentZKMetadata.class);
    when(segmentZKMetadata.getNumReplicas()).thenReturn(3);
    when(segmentZKMetadata.getEndOffset()).thenReturn("100");

    PinotLLCRealtimeSegmentManager pinotLLCRealtimeSegmentManager = mock(PinotLLCRealtimeSegmentManager.class);
    when(pinotLLCRealtimeSegmentManager.getCommitTimeoutMS(anyString())).thenReturn(System.currentTimeMillis());

    // Assert that registering for the same scheme i.e. "default", works as expected and BlockingSegmentCompletionFSM
    // object is created
    Assert.assertTrue(
        SegmentCompletionFSMFactory.createFSM(segmentCompletionConfig.getDefaultFsmScheme(), segmentCompletionManager,
            pinotLLCRealtimeSegmentManager, new LLCSegmentName(LLC_SEGMENT_NAME),
            segmentZKMetadata) instanceof BlockingSegmentCompletionFSM);

    // Assert that changing the default for pauseless returns the new custom fsm.
    Assert.assertTrue(SegmentCompletionFSMFactory.createFSM(segmentCompletionConfig.getDefaultPauselessFsmScheme(),
        segmentCompletionManager, pinotLLCRealtimeSegmentManager, new LLCSegmentName(LLC_SEGMENT_NAME),
        segmentZKMetadata) instanceof FakeCustomFSM);
  }

  public static class FakeCustomFSM extends BlockingSegmentCompletionFSM {

    public FakeCustomFSM(PinotLLCRealtimeSegmentManager segmentManager,
        SegmentCompletionManager segmentCompletionManager, LLCSegmentName segmentName,
        SegmentZKMetadata segmentMetadata) {
      super(segmentManager, segmentCompletionManager, segmentName, segmentMetadata);
    }
  }
}
