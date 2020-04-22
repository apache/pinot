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
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.core.util.SegmentCompletionProtocolUtils;
import org.slf4j.Logger;

import static org.apache.pinot.common.protocols.SegmentCompletionProtocol.ControllerResponseStatus.UPLOAD_SUCCESS;


// A segment uploader which uploads segments to the controller via the controller's segmentCommitUpload end point
// point.
public class ControllerVipBasedSegmentUploader implements SegmentUploader {
  private final Logger _segmentLogger;
  private final String _controllerVipUrl;
  private final FileUploadDownloadClient _fileUploadDownloadClient;
  private final String _segmentName;
  private final int _segmentUploadRequestTimeoutMs;
  private final ServerMetrics _serverMetrics;

  public ControllerVipBasedSegmentUploader(Logger segmentLogger, FileUploadDownloadClient fileUploadDownloadClient,
      String controllerVipUrl, String segmentName, int segmentUploadRequestTimeoutMs, ServerMetrics serverMetrics) {
    _segmentLogger = segmentLogger;
    _fileUploadDownloadClient = fileUploadDownloadClient;
    _controllerVipUrl = controllerVipUrl;
    _segmentName = segmentName;
    _segmentUploadRequestTimeoutMs = segmentUploadRequestTimeoutMs;
    _serverMetrics = serverMetrics;
  }

  @Override
  public URI uploadSegment(File segmentFile)
      throws URISyntaxException {
    SegmentCompletionProtocol.Response response = SegmentCompletionProtocolUtils
        .uploadSegmentWithFileUploadDownloadClient(_fileUploadDownloadClient, segmentFile, _controllerVipUrl,
            _segmentName, _segmentUploadRequestTimeoutMs, _segmentLogger);
    SegmentCompletionProtocolUtils.raiseSegmentCompletionProtocolResponseMetric(_serverMetrics, response);
    if (response.getStatus() == UPLOAD_SUCCESS) {
      return new URI(response.getSegmentLocation());
    }
    return null;
  }
}
