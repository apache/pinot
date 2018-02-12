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
import com.linkedin.pinot.common.utils.ClientSSLContextGenerator;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import com.linkedin.pinot.core.query.utils.Pair;
import java.io.File;
import java.net.URI;
import javax.net.ssl.SSLContext;
import org.apache.commons.configuration.Configuration;
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
  private static final String HTTPS_PROTOCOL = "https";
  private static final String HTTP_PROTOCOL = "http";

  private static final String CONFIG_OF_CONTROLLER_HTTPS_ENABLED = "enabled";
  private static final String CONFIG_OF_CONTROLLER_HTTPS_PORT = "controller.port";

  private static Integer _controllerHttpsPort = null;

  private final String _instanceId;
  private final FileUploadDownloadClient _fileUploadDownloadClient;
  private static SSLContext _sslContext;

  public static void init(Configuration uploaderConfig) {
    Configuration httpsConfig = uploaderConfig.subset(HTTPS_PROTOCOL);
    if (httpsConfig.getBoolean(CONFIG_OF_CONTROLLER_HTTPS_ENABLED, false)) {
      _sslContext = new ClientSSLContextGenerator(httpsConfig.subset(CommonConstants.PREFIX_OF_SSL_SUBSET)).generate();
      _controllerHttpsPort = httpsConfig.getInt(CONFIG_OF_CONTROLLER_HTTPS_PORT);
    }
  }

  public ServerSegmentCompletionProtocolHandler(String instanceId) {
    _instanceId = instanceId;
    if (_sslContext != null) {
      _fileUploadDownloadClient = new FileUploadDownloadClient(_sslContext);
    } else {
      _fileUploadDownloadClient = new FileUploadDownloadClient();
    }
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

  // TODO We need to make this work with trusted certificates if the VIP is using https.
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

  public SegmentCompletionProtocol.Response segmentCommitEnd(long offset, final String segmentName, String segmentLocation,
      long memoryUsedBytes) {
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withInstanceId(_instanceId).withOffset(offset).withSegmentName(segmentName)
        .withSegmentLocation(segmentLocation).withMemoryUsedBytes(memoryUsedBytes);
    SegmentCompletionProtocol.SegmentCommitEndRequest request = new SegmentCompletionProtocol.SegmentCommitEndRequest(params);
    String url = createSegmentCompletionUrl(request);
    if (url == null) {
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }
    return sendRequest(url);
  }

  public SegmentCompletionProtocol.Response segmentCommit(long offset, final String segmentName, long memoryUsedBytes,
      final File segmentTarFile) {
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withInstanceId(_instanceId).withOffset(offset).withSegmentName(segmentName).withMemoryUsedBytes(memoryUsedBytes);
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
    final Pair<String, Integer> leaderHostPort = leaderLocator.getControllerLeader();
    if (leaderHostPort == null) {
      LOGGER.warn("No leader found while trying to send {}", request.toString());
      return null;
    }
    String protocol = HTTP_PROTOCOL;
    if (_controllerHttpsPort != null) {
      leaderHostPort.setSecond(_controllerHttpsPort);
      protocol = HTTPS_PROTOCOL;
    }

    return request.getUrl(leaderHostPort.getFirst() + ":" + leaderHostPort.getSecond(), protocol);
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
