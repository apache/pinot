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
package org.apache.pinot.server.realtime;

import java.io.File;
import java.io.IOException;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.core.data.manager.realtime.LLRealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.SegmentCommitter;
import org.apache.pinot.core.data.manager.realtime.SegmentCommitterFactory;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.helix.examples.Quickstart.*;
import static org.mockito.Mockito.*;


public class SegmentCommitterTest {
  private static Logger LOGGER = LoggerFactory.getLogger(SegmentCommitterTest.class);
  private File _tempFile;

  @BeforeClass
  public void setUp() throws IOException {
    _tempFile = File.createTempFile("segmentCommitterTest", null);
    startZookeeper();
    startController();
  }

  @Test
  public void testSplitCommit() {
    IndexLoadingConfig indexLoadingConfig = mock(IndexLoadingConfig.class);
    ServerSegmentCompletionProtocolHandler protocolHandler = new ServerSegmentCompletionProtocolHandler(null, "table");
    SegmentCompletionProtocol.Request.Params params = mock(SegmentCompletionProtocol.Request.Params.class);
    SegmentCompletionProtocol.Response prevResponse = mock(SegmentCompletionProtocol.Response.class);
    LLRealtimeSegmentDataManager.SegmentBuildDescriptor segmentBuildDescriptor = mock(
        LLRealtimeSegmentDataManager.SegmentBuildDescriptor.class);
    when(segmentBuildDescriptor.getSegmentTarFilePath()).thenReturn(_tempFile.getAbsolutePath());

    when(protocolHandler.segmentCommitStart(any())).thenReturn(prevResponse);
    when(prevResponse.getStatus()).thenReturn(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);
    SegmentCompletionProtocol.Response continuedResponse = mock(SegmentCompletionProtocol.Response.class);
    when(protocolHandler.segmentCommitUpload(any(), any(), any())).thenReturn(continuedResponse);
    when(continuedResponse.getStatus()).thenReturn(SegmentCompletionProtocol.ControllerResponseStatus.UPLOAD_SUCCESS);
    when(indexLoadingConfig.isEnableSplitCommitEndWithMetadata()).thenReturn(false);
    SegmentCompletionProtocol.Response endResponse = mock(SegmentCompletionProtocol.Response.class);
    when(endResponse.getStatus()).thenReturn(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);

    SegmentCommitterFactory segmentCommitterFactory = new SegmentCommitterFactory(LOGGER, indexLoadingConfig, protocolHandler);
    SegmentCommitter segmentCommitter = segmentCommitterFactory.createSplitSegmentCommitter(params, prevResponse);
    segmentCommitter.commit(1, 3, segmentBuildDescriptor);
  }

  @Test
  public void testDefaultCommit() {
    IndexLoadingConfig indexLoadingConfig = mock(IndexLoadingConfig.class);
    ServerSegmentCompletionProtocolHandler protocolHandler = mock(ServerSegmentCompletionProtocolHandler.class);
    SegmentCompletionProtocol.Request.Params params = mock(SegmentCompletionProtocol.Request.Params.class);
    SegmentCompletionProtocol.Response prevResponse = mock(SegmentCompletionProtocol.Response.class);
    LLRealtimeSegmentDataManager.SegmentBuildDescriptor segmentBuildDescriptor = mock(
        LLRealtimeSegmentDataManager.SegmentBuildDescriptor.class);
    when(segmentBuildDescriptor.getSegmentTarFilePath()).thenReturn(_tempFile.getAbsolutePath());

    when(protocolHandler.segmentCommit(any(), any())).thenReturn(prevResponse);
    when(prevResponse.getStatus()).thenReturn(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);

    SegmentCommitterFactory segmentCommitterFactory = new SegmentCommitterFactory(LOGGER, indexLoadingConfig, protocolHandler);
    SegmentCommitter segmentCommitter = segmentCommitterFactory.createDefaultSegmentCommitter(params, prevResponse);
    segmentCommitter.commit(1, 3, segmentBuildDescriptor);
  }
}
