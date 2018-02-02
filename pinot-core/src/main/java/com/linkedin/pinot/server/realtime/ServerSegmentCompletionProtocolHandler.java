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


package com.linkedin.pinot.server.realtime;

import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import java.io.File;
import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class that handles sending segment completion protocol requests to the controller and getting
 * back responses
 */
public class ServerSegmentCompletionProtocolHandler {
  private static Logger LOGGER = LoggerFactory.getLogger(ServerSegmentCompletionProtocolHandler.class);
  private static final int SEGMENT_UPLOAD_REQUEST_TIMEOUT_MS = 30_000;
  private static final int OTHER_REQUESTS_TIMEOUT = 5_000;

  private final String _instanceId;
  private final FileUploadDownloadClient _fileUploadDownloadClient;

  public ServerSegmentCompletionProtocolHandler(String instanceId) {
    _instanceId = instanceId;
    _fileUploadDownloadClient = new FileUploadDownloadClient();
  }

  public SegmentCompletionProtocol.Response segmentCommitStart(long offset, final String segmentName) {
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withInstanceId(_instanceId).withOffset(offset).withSegmentName(segmentName);
    SegmentCompletionProtocol.SegmentCommitStartRequest request = new SegmentCompletionProtocol.SegmentCommitStartRequest(params);
    String url = createSegmentCompletionUrl(request);
    if (url == null) {
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }
    return sendRequest(url);
  }

  public SegmentCompletionProtocol.Response segmentCommitUpload(long offset, final String segmentName, final File segmentTarFile,
      final String controllerVipUrl) {
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withInstanceId(_instanceId).withOffset(offset).withSegmentName(segmentName);
    SegmentCompletionProtocol.SegmentCommitUploadRequest request = new SegmentCompletionProtocol.SegmentCommitUploadRequest(params);

    String hostPort;
    String protocol;
    try {
      URI uri = URI.create(controllerVipUrl);
      protocol = uri.getScheme();
      hostPort = uri.getAuthority();
    } catch (Exception e) {
      throw new RuntimeException("Could not make URI", e);
    }
    String url = request.getUrl(hostPort, protocol);
    return uploadSegment(url, segmentName, segmentTarFile);
  }

  public SegmentCompletionProtocol.Response segmentCommitEnd(long offset, final String segmentName, String segmentLocation) {
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withInstanceId(_instanceId).withOffset(offset).withSegmentName(segmentName).withSegmentLocation(segmentLocation);
    SegmentCompletionProtocol.SegmentCommitEndRequest request = new SegmentCompletionProtocol.SegmentCommitEndRequest(params);
    String url = createSegmentCompletionUrl(request);
    if (url == null) {
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }
    return sendRequest(url);
  }

  public SegmentCompletionProtocol.Response segmentCommit(long offset, final String segmentName, final File segmentTarFile) {
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withInstanceId(_instanceId).withOffset(offset).withSegmentName(segmentName);
    SegmentCompletionProtocol.SegmentCommitRequest request = new SegmentCompletionProtocol.SegmentCommitRequest(params);
    String url = createSegmentCompletionUrl(request);
    if (url == null) {
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }

    return uploadSegment(url, segmentName, segmentTarFile);
  }

  public SegmentCompletionProtocol.Response extendBuildTime(SegmentCompletionProtocol.Request.Params params) {
    params.withInstanceId(_instanceId);
    SegmentCompletionProtocol.ExtendBuildTimeRequest request = new SegmentCompletionProtocol.ExtendBuildTimeRequest(params);
    String url = createSegmentCompletionUrl(request);
    return sendRequest(url);
  }

  public SegmentCompletionProtocol.Response segmentConsumed(SegmentCompletionProtocol.Request.Params params) {
    params.withInstanceId(_instanceId);
    SegmentCompletionProtocol.SegmentConsumedRequest request = new SegmentCompletionProtocol.SegmentConsumedRequest(params);
    String url = createSegmentCompletionUrl(request);
    if (url == null) {
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }
    return sendRequest(url);
  }

  public SegmentCompletionProtocol.Response segmentStoppedConsuming(SegmentCompletionProtocol.Request.Params params) {
    params.withInstanceId(_instanceId);
    SegmentCompletionProtocol.SegmentStoppedConsuming request = new SegmentCompletionProtocol.SegmentStoppedConsuming(params);
    String url = createSegmentCompletionUrl(request);
    if (url == null) {
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }
    return sendRequest(url);
  }


  private String createSegmentCompletionUrl(SegmentCompletionProtocol.Request request) {
    ControllerLeaderLocator leaderLocator = ControllerLeaderLocator.getInstance();
    final String leaderHostPort = leaderLocator.getControllerLeader();
    if (leaderHostPort == null) {
      LOGGER.warn("No leader found while trying to send {}", request.toString());
      return null;
    }

    return request.getUrl(leaderHostPort, "http");
  }

  private SegmentCompletionProtocol.Response sendRequest(String url) {
    try {
      String responseStr = _fileUploadDownloadClient.sendSegmentCompletionProtocolRequest(url, OTHER_REQUESTS_TIMEOUT);
      return new SegmentCompletionProtocol.Response(responseStr);
    } catch (Exception e) {
      // Catch all exceptions, we want the protocol to handle the case assuming the request was never sent.
      LOGGER.error("Could not send request {}", url, e);
    }
    return SegmentCompletionProtocol.RESP_NOT_SENT;
  }

  private SegmentCompletionProtocol.Response uploadSegment(String url, final String segmentName, final File segmentTarFile) {
    try {
      String responseStr = _fileUploadDownloadClient.uploadSegment(url, segmentName, segmentTarFile, SEGMENT_UPLOAD_REQUEST_TIMEOUT_MS);
      return new SegmentCompletionProtocol.Response(responseStr);
    } catch (Exception e) {
      // Catch all exceptions, we want the protocol to handle the case assuming the request was never sent.
      LOGGER.error("Could not send request {}", url, e);
    }
    return SegmentCompletionProtocol.RESP_NOT_SENT;
  }
}
