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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.ClientSSLContextGenerator;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.http.HttpClientConfig;
import org.apache.pinot.core.data.manager.realtime.Server2ControllerSegmentUploader;
import org.apache.pinot.core.util.SegmentCompletionProtocolUtils;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.Server.SegmentCompletionProtocol.*;


/**
 * A class that handles sending segment completion protocol requests to the controller and getting
 * back responses
 */
// TODO: Use exception based code to handle different types of exceptions.
public class ServerSegmentCompletionProtocolHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerSegmentCompletionProtocolHandler.class);
  private static final String HTTPS_PROTOCOL = CommonConstants.HTTPS_PROTOCOL;
  private static final String HTTP_PROTOCOL = CommonConstants.HTTP_PROTOCOL;

  private static SSLContext _sslContext;
  private static HttpClientConfig _httpClientConfig = HttpClientConfig.DEFAULT_HTTP_CLIENT_CONFIG;
  private static Integer _controllerHttpsPort;
  private static int _segmentUploadRequestTimeoutMs;
  private static AuthProvider _authProvider;
  private static String _protocol = HTTP_PROTOCOL;

  private final FileUploadDownloadClient _fileUploadDownloadClient;
  private final ServerMetrics _serverMetrics;
  private final String _rawTableName;

  public static void init(PinotConfiguration uploaderConfig) {
    PinotConfiguration httpsConfig = uploaderConfig.subset(HTTPS_PROTOCOL);

    // NOTE: legacy https config for segment upload is deprecated. If you're relying on these settings, please consider
    // moving to server-wide TLS configs instead. Legacy support will be removed eventually.
    if (httpsConfig.getProperty(CONFIG_OF_CONTROLLER_HTTPS_ENABLED, false)) {
      _sslContext = new ClientSSLContextGenerator(httpsConfig.subset(CommonConstants.PREFIX_OF_SSL_SUBSET)).generate();
      _controllerHttpsPort = httpsConfig.getProperty(CONFIG_OF_CONTROLLER_HTTPS_PORT, Integer.class);
    }

    _protocol = uploaderConfig.getProperty(CONFIG_OF_PROTOCOL, HTTP_PROTOCOL);
    _segmentUploadRequestTimeoutMs = uploaderConfig
        .getProperty(CONFIG_OF_SEGMENT_UPLOAD_REQUEST_TIMEOUT_MS, DEFAULT_SEGMENT_UPLOAD_REQUEST_TIMEOUT_MS);

    _authProvider = AuthProviderUtils.extractAuthProvider(uploaderConfig, CONFIG_OF_SEGMENT_UPLOADER_AUTH);

    _httpClientConfig = HttpClientConfig.newBuilder(uploaderConfig).build();
  }

  public ServerSegmentCompletionProtocolHandler(ServerMetrics serverMetrics, String tableNameWithType) {
    _fileUploadDownloadClient = new FileUploadDownloadClient(_httpClientConfig, _sslContext);
    _serverMetrics = serverMetrics;
    _rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
  }

  public static int getSegmentUploadRequestTimeoutMs() {
    return _segmentUploadRequestTimeoutMs;
  }

  public FileUploadDownloadClient getFileUploadDownloadClient() {
    return _fileUploadDownloadClient;
  }

  public AuthProvider getAuthProvider() {
    return _authProvider;
  }

  public SegmentCompletionProtocol.Response segmentCommitStart(SegmentCompletionProtocol.Request.Params params) {
    SegmentCompletionProtocol.SegmentCommitStartRequest request =
        new SegmentCompletionProtocol.SegmentCommitStartRequest(params);
    String url = createSegmentCompletionUrl(request);
    if (url == null) {
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }
    return sendRequest(url);
  }

  // TODO We need to make this work with trusted certificates if the VIP is using https.
  public String getSegmentCommitUploadURL(SegmentCompletionProtocol.Request.Params params,
      final String controllerVipUrl) {
    SegmentCompletionProtocol.SegmentCommitUploadRequest request =
        new SegmentCompletionProtocol.SegmentCommitUploadRequest(params);

    String hostPort;
    String protocol;
    try {
      URI uri = URI.create(controllerVipUrl);
      protocol = uri.getScheme();
      hostPort = uri.getAuthority();
    } catch (Exception e) {
      throw new RuntimeException("Could not make URI", e);
    }
    return request.getUrl(hostPort, protocol);
  }

  // Replaced by segmentCommitEndWithMetadata().
  @Deprecated
  public SegmentCompletionProtocol.Response segmentCommitEnd(SegmentCompletionProtocol.Request.Params params) {
    SegmentCompletionProtocol.SegmentCommitEndRequest request =
        new SegmentCompletionProtocol.SegmentCommitEndRequest(params);
    String url = createSegmentCompletionUrl(request);
    if (url == null) {
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }
    return sendRequest(url);
  }

  public SegmentCompletionProtocol.Response segmentCommitEndWithMetadata(
      SegmentCompletionProtocol.Request.Params params, final Map<String, File> metadataFiles) {
    SegmentCompletionProtocol.SegmentCommitEndWithMetadataRequest request =
        new SegmentCompletionProtocol.SegmentCommitEndWithMetadataRequest(params);
    String url = createSegmentCompletionUrl(request);
    if (url == null) {
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }
    return sendCommitEndWithMetadataFiles(url, metadataFiles);
  }

  public SegmentCompletionProtocol.Response segmentCommit(SegmentCompletionProtocol.Request.Params params,
      final File segmentTarFile) {
    SegmentCompletionProtocol.SegmentCommitRequest request = new SegmentCompletionProtocol.SegmentCommitRequest(params);
    String url = createSegmentCompletionUrl(request);
    if (url == null) {
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }

    Server2ControllerSegmentUploader segmentUploader = null;
    try {
      segmentUploader =
          new Server2ControllerSegmentUploader(LOGGER, _fileUploadDownloadClient, url, params.getSegmentName(),
              _segmentUploadRequestTimeoutMs, _serverMetrics, _authProvider, _rawTableName);
    } catch (URISyntaxException e) {
      LOGGER.error("Segment commit upload url error: ", e);
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }
    return segmentUploader.uploadSegmentToController(segmentTarFile);
  }

  public SegmentCompletionProtocol.Response extendBuildTime(SegmentCompletionProtocol.Request.Params params) {
    SegmentCompletionProtocol.ExtendBuildTimeRequest request =
        new SegmentCompletionProtocol.ExtendBuildTimeRequest(params);
    String url = createSegmentCompletionUrl(request);
    return sendRequest(url);
  }

  public SegmentCompletionProtocol.Response segmentConsumed(SegmentCompletionProtocol.Request.Params params) {
    SegmentCompletionProtocol.SegmentConsumedRequest request =
        new SegmentCompletionProtocol.SegmentConsumedRequest(params);
    String url = createSegmentCompletionUrl(request);
    if (url == null) {
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }
    return sendRequest(url);
  }

  public SegmentCompletionProtocol.Response segmentStoppedConsuming(SegmentCompletionProtocol.Request.Params params) {
    SegmentCompletionProtocol.SegmentStoppedConsuming request =
        new SegmentCompletionProtocol.SegmentStoppedConsuming(params);
    String url = createSegmentCompletionUrl(request);
    if (url == null) {
      return SegmentCompletionProtocol.RESP_NOT_SENT;
    }
    return sendRequest(url);
  }

  private String createSegmentCompletionUrl(SegmentCompletionProtocol.Request request) {
    ControllerLeaderLocator leaderLocator = ControllerLeaderLocator.getInstance();
    final Pair<String, Integer> leaderHostPort = leaderLocator.getControllerLeader(_rawTableName);
    if (leaderHostPort == null) {
      LOGGER.warn("No leader found while trying to send {}", request.toString());
      return null;
    }
    Integer port = leaderHostPort.getRight();
    String protocol = _protocol;
    if (_controllerHttpsPort != null) {
      port = _controllerHttpsPort;
      protocol = HTTPS_PROTOCOL;
    }
    return request.getUrl(leaderHostPort.getLeft() + ":" + port, protocol);
  }

  private SegmentCompletionProtocol.Response sendRequest(String url) {
    SegmentCompletionProtocol.Response response;
    try {
      String responseStr = _fileUploadDownloadClient
          .sendSegmentCompletionProtocolRequest(new URI(url), AuthProviderUtils.toRequestHeaders(_authProvider), null,
              DEFAULT_OTHER_REQUESTS_TIMEOUT).getResponse();
      response = SegmentCompletionProtocol.Response.fromJsonString(responseStr);
      LOGGER.info("Controller response {} for {}", response.toJsonString(), url);
      if (response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER)) {
        ControllerLeaderLocator.getInstance().invalidateCachedControllerLeader();
      }
    } catch (Exception e) {
      // Catch all exceptions, we want the protocol to handle the case assuming the request was never sent.
      response = SegmentCompletionProtocol.RESP_NOT_SENT;
      LOGGER.error("Could not send request {}", url, e);
      // Invalidate controller leader cache, as exception could be because of leader being down (deployment/failure)
      // and hence unable to send {@link SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER}
      // If cache is not invalidated, we will not recover from exceptions until the controller comes back up
      ControllerLeaderLocator.getInstance().invalidateCachedControllerLeader();
    }
    SegmentCompletionProtocolUtils.raiseSegmentCompletionProtocolResponseMetric(_serverMetrics, response);
    return response;
  }

  private SegmentCompletionProtocol.Response sendCommitEndWithMetadataFiles(String url,
      Map<String, File> metadataFiles) {
    SegmentCompletionProtocol.Response response;
    try {
      String responseStr = _fileUploadDownloadClient
          .uploadSegmentMetadataFiles(new URI(url), metadataFiles, AuthProviderUtils.toRequestHeaders(_authProvider),
              null, _segmentUploadRequestTimeoutMs).getResponse();
      response = SegmentCompletionProtocol.Response.fromJsonString(responseStr);
      LOGGER.info("Controller response {} for {}", response.toJsonString(), url);
      if (response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER)) {
        ControllerLeaderLocator.getInstance().invalidateCachedControllerLeader();
      }
    } catch (Exception e) {
      // Catch all exceptions, we want the protocol to handle the case assuming the request was never sent.
      response = SegmentCompletionProtocol.RESP_NOT_SENT;
      LOGGER.error("Could not send request {}", url, e);
      // Invalidate controller leader cache, as exception could be because of leader being down (deployment/failure)
      // and hence unable to send {@link SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER}
      // If cache is not invalidated, we will not recover from exceptions until the controller comes back up
      ControllerLeaderLocator.getInstance().invalidateCachedControllerLeader();
    }
    SegmentCompletionProtocolUtils.raiseSegmentCompletionProtocolResponseMetric(_serverMetrics, response);
    return response;
  }
}
