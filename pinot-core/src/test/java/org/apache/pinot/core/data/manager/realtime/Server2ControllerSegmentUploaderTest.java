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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class Server2ControllerSegmentUploaderTest {
  private static final String GOOD_CONTROLLER_VIP = "good_controller_vip";
  private static final String BAD_CONTROLLER_VIP = "bad_controller_vip";
  public static final String SEGMENT_LOCATION = "http://server/segment_location";
  private static Logger _logger = LoggerFactory.getLogger(Server2ControllerSegmentUploaderTest.class);
  private FileUploadDownloadClient _fileUploadDownloadClient;
  private File _file;
  private LLCSegmentName _llcSegmentName;

  @BeforeClass
  public void setUp()
      throws URISyntaxException, IOException, HttpErrorStatusException {
    SegmentCompletionProtocol.Response successResp = new SegmentCompletionProtocol.Response();
    successResp.setStatus(SegmentCompletionProtocol.ControllerResponseStatus.UPLOAD_SUCCESS);
    successResp.setSegmentLocation(SEGMENT_LOCATION);
    SimpleHttpResponse successHttpResponse = new SimpleHttpResponse(200, successResp.toJsonString());

    SegmentCompletionProtocol.Response failureResp = new SegmentCompletionProtocol.Response();
    failureResp.setStatus(SegmentCompletionProtocol.ControllerResponseStatus.FAILED);
    SimpleHttpResponse failHttpResponse = new SimpleHttpResponse(404, failureResp.toJsonString());

    _fileUploadDownloadClient = mock(FileUploadDownloadClient.class);

    when(_fileUploadDownloadClient
        .uploadSegment(eq(new URI(GOOD_CONTROLLER_VIP)), anyString(), any(File.class), any(), any(), anyInt()))
        .thenReturn(successHttpResponse);
    when(_fileUploadDownloadClient
        .uploadSegment(eq(new URI(BAD_CONTROLLER_VIP)), anyString(), any(File.class), any(), any(), anyInt()))
        .thenReturn(failHttpResponse);

    _file = FileUtils.getFile(FileUtils.getTempDirectory(), UUID.randomUUID().toString());
    _file.deleteOnExit();

    _llcSegmentName = new LLCSegmentName("test_REALTIME", 1, 0, System.currentTimeMillis());
  }

  @AfterClass
  public void tearDown() {
    _file.delete();
  }

  @Test
  public void testUploadSuccess()
      throws URISyntaxException {
    Server2ControllerSegmentUploader uploader =
        new Server2ControllerSegmentUploader(_logger, _fileUploadDownloadClient, GOOD_CONTROLLER_VIP, "segmentName",
            10000, mock(ServerMetrics.class), null, _llcSegmentName.getTableName());
    URI segmentURI = uploader.uploadSegment(_file, _llcSegmentName);
    Assert.assertEquals(segmentURI.toString(), SEGMENT_LOCATION);
  }

  @Test
  public void testUploadFailure()
      throws URISyntaxException {
    Server2ControllerSegmentUploader uploader =
        new Server2ControllerSegmentUploader(_logger, _fileUploadDownloadClient, BAD_CONTROLLER_VIP, "segmentName",
            10000, mock(ServerMetrics.class), null, _llcSegmentName.getTableName());
    URI segmentURI = uploader.uploadSegment(_file, _llcSegmentName);
    Assert.assertNull(segmentURI);
  }
}
