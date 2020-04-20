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
package org.apache.pinot.core.data.manager.realtime;

import java.io.File;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ControllerVipBasedSegmentUploaderTest {
  private static final String GOOD_CONTROLLER_VIP = "good_controller_vip";
  private static final String BAD_CONTROLLER_VIP = "bad_controller_vip";
  public static final String SEGMENT_LOCATION = "segment_location";
  private static Logger _logger = LoggerFactory.getLogger(ControllerVipBasedSegmentUploaderTest.class);
  private ServerSegmentCompletionProtocolHandler _handler;
  private File _file;

  @BeforeClass
  public void setUp() {
    SegmentCompletionProtocol.Response successResp = new SegmentCompletionProtocol.Response();
    successResp.setStatus(SegmentCompletionProtocol.ControllerResponseStatus.UPLOAD_SUCCESS);
    successResp.setSegmentLocation(SEGMENT_LOCATION);
    SegmentCompletionProtocol.Response failureResp = new SegmentCompletionProtocol.Response();
    failureResp.setStatus(SegmentCompletionProtocol.ControllerResponseStatus.FAILED);

    _handler = mock(ServerSegmentCompletionProtocolHandler.class);
    when(_handler.segmentCommitUpload(any(), any(), eq(GOOD_CONTROLLER_VIP))).thenReturn(successResp);
    when(_handler.segmentCommitUpload(any(), any(), eq(BAD_CONTROLLER_VIP))).thenReturn(failureResp);
    _file = FileUtils.getFile(FileUtils.getTempDirectory(), UUID.randomUUID().toString());
    _file.deleteOnExit();
  }

  @AfterClass
  public void tearDown() {
    _file.delete();
  }

  @Test
  public void testUploadSuccess() {
    ControllerVipBasedSegmentUploader uploader =
        new ControllerVipBasedSegmentUploader(_logger, _handler, new SegmentCompletionProtocol.Request.Params(),
            GOOD_CONTROLLER_VIP);
    SegmentUploadStatus status = uploader.segmentUpload(_file);
    Assert.assertTrue(status.isUploadSuccessful());
    Assert.assertEquals(status.getSegmentLocation(), SEGMENT_LOCATION);
  }

  @Test
  public void testUploadFailure() {
    ControllerVipBasedSegmentUploader uploader =
        new ControllerVipBasedSegmentUploader(_logger, _handler, new SegmentCompletionProtocol.Request.Params(),
            BAD_CONTROLLER_VIP);
    SegmentUploadStatus status = uploader.segmentUpload(_file);
    Assert.assertFalse(status.isUploadSuccessful());
  }
}
