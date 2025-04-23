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
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.util.SegmentCompletionProtocolUtils;
import org.apache.pinot.server.realtime.ControllerLeaderLocator;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
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
  private final AuthProvider _authProvider;
  private final String _rawTableName;

  public Server2ControllerSegmentUploader(Logger segmentLogger, FileUploadDownloadClient fileUploadDownloadClient,
      String controllerSegmentUploadCommitUrl, String segmentName, int segmentUploadRequestTimeoutMs,
      ServerMetrics serverMetrics, AuthProvider authProvider, String tableName)
      throws URISyntaxException {
    _segmentLogger = segmentLogger;
    _fileUploadDownloadClient = fileUploadDownloadClient;
    _controllerSegmentUploadCommitUrl = new URI(controllerSegmentUploadCommitUrl);
    _segmentName = segmentName;
    _segmentUploadRequestTimeoutMs = segmentUploadRequestTimeoutMs;
    _serverMetrics = serverMetrics;
    _authProvider = authProvider;
    _rawTableName = TableNameBuilder.extractRawTableName(tableName);
  }

  @Override
  public URI uploadSegment(File segmentFile, LLCSegmentName segmentName) {
    return uploadSegment(segmentFile, segmentName, _segmentUploadRequestTimeoutMs);
  }

  @Override
  public URI uploadSegment(File segmentFile, LLCSegmentName segmentName, int timeoutInMillis) {
    SegmentCompletionProtocol.Response response = uploadSegmentToController(segmentFile, timeoutInMillis);
    if (response.getStatus() == SegmentCompletionProtocol.ControllerResponseStatus.UPLOAD_SUCCESS) {
      try {
        URI uri = new URI(response.getSegmentLocation());
        _serverMetrics.addMeteredTableValue(_rawTableName, ServerMeter.SEGMENT_UPLOAD_SUCCESS, 1);
        return uri;
      } catch (URISyntaxException e) {
        _segmentLogger.error("Error in segment location format: ", e);
      }
    }
    _serverMetrics.addMeteredTableValue(_rawTableName, ServerMeter.SEGMENT_UPLOAD_FAILURE, 1);
    return null;
  }

  public SegmentCompletionProtocol.Response uploadSegmentToController(File segmentFile) {
    return uploadSegmentToController(segmentFile, _segmentUploadRequestTimeoutMs);
  }

  private SegmentCompletionProtocol.Response uploadSegmentToController(File segmentFile, int timeoutInMillis) {
    SegmentCompletionProtocol.Response response;
    long startTime = System.currentTimeMillis();
    try {
      String responseStr = _fileUploadDownloadClient
          .uploadSegment(_controllerSegmentUploadCommitUrl, _segmentName, segmentFile,
              AuthProviderUtils.toRequestHeaders(_authProvider), null, timeoutInMillis).getResponse();
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
    } finally {
      long duration = System.currentTimeMillis() - startTime;
      _serverMetrics.addTimedTableValue(_rawTableName, ServerTimer.SEGMENT_UPLOAD_TIME_MS, duration,
          TimeUnit.MILLISECONDS);
    }
    SegmentCompletionProtocolUtils.raiseSegmentCompletionProtocolResponseMetric(_serverMetrics, response);
    return response;
  }
}
