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
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.util.SegmentCompletionProtocolUtils;
import org.apache.pinot.server.realtime.ControllerLeaderLocator;
import org.slf4j.Logger;


// A segment uploader which uploads segments to the controller via the controller's segmentCommitUpload end point.
public class Server2ControllerSegmentUploader implements SegmentUploader {
  private final Logger _segmentLogger;
  // The controller segment upload url.
  private final URI _controllerSegmentUploadCommitUrl;
  private final FileUploadDownloadClient _fileUploadDownloadClient;
  private final String _segmentName;
  private final int _segmentUploadRequestTimeoutMs;
  private final ServerMetrics _serverMetrics;
  private final String _authToken;

  public Server2ControllerSegmentUploader(Logger segmentLogger, FileUploadDownloadClient fileUploadDownloadClient,
      String controllerSegmentUploadCommitUrl, String segmentName, int segmentUploadRequestTimeoutMs,
      ServerMetrics serverMetrics, String authToken) throws URISyntaxException {
    _segmentLogger = segmentLogger;
    _fileUploadDownloadClient = fileUploadDownloadClient;
    _controllerSegmentUploadCommitUrl = new URI(controllerSegmentUploadCommitUrl);
    _segmentName = segmentName;
    _segmentUploadRequestTimeoutMs = segmentUploadRequestTimeoutMs;
    _serverMetrics = serverMetrics;
    _authToken = authToken;
  }

  @Override
  public URI uploadSegment(File segmentFile, LLCSegmentName segmentName) {
    SegmentCompletionProtocol.Response response = uploadSegmentToController(segmentFile);
    if (response.getStatus() == SegmentCompletionProtocol.ControllerResponseStatus.UPLOAD_SUCCESS) {
      try {
        return new URI(response.getSegmentLocation());
      } catch (URISyntaxException e) {
        _segmentLogger.error("Error in segment location format: ", e);
      }
    }
    return null;
  }

  public SegmentCompletionProtocol.Response uploadSegmentToController(File segmentFile) {
    SegmentCompletionProtocol.Response response;
    try {
      String responseStr =
          _fileUploadDownloadClient
              .uploadSegment(_controllerSegmentUploadCommitUrl, _segmentName, segmentFile,
                  FileUploadDownloadClient.makeAuthHeader(_authToken), null, _segmentUploadRequestTimeoutMs)
              .getResponse();
      response = SegmentCompletionProtocol.Response.fromJsonString(responseStr);
      _segmentLogger.info("Controller response {} for {}", response.toJsonString(), _controllerSegmentUploadCommitUrl);
      if (response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER)) {
        ControllerLeaderLocator.getInstance().invalidateCachedControllerLeader();
      }
    } catch (Exception e) {
      // Catch all exceptions, we want the protocol to handle the case assuming the request was never sent.
      response = SegmentCompletionProtocol.RESP_NOT_SENT;
      _segmentLogger.error("Could not send request {}", _controllerSegmentUploadCommitUrl, e);
      // Invalidate controller leader cache, as exception could be because of leader being down (deployment/failure) and
      // hence unable to send {@link SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER}
      // If cache is not invalidated, we will not recover from exceptions until the controller comes back up
      ControllerLeaderLocator.getInstance().invalidateCachedControllerLeader();
    }
    SegmentCompletionProtocolUtils.raiseSegmentCompletionProtocolResponseMetric(_serverMetrics, response);
    return response;
  }
}
