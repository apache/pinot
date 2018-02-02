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
import com.linkedin.pinot.common.utils.CommonConstants;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.PartSource;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class that handles sending segment completion protocol requests to the controller and getting
 * back responses
 */
public class ServerSegmentCompletionProtocolHandler {
  private static Logger LOGGER = LoggerFactory.getLogger(ServerSegmentCompletionProtocolHandler.class);

  private final String _instanceId;

  public ServerSegmentCompletionProtocolHandler(String instanceId) {
    _instanceId = instanceId;
  }

  private Part[] createFileParts(final String segmentName, final File segmentTarFile) {
    InputStream fileInputStream;
    try {
      fileInputStream = new FileInputStream(segmentTarFile);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }

    final InputStream inputStream = fileInputStream;
    Part[] parts = {
        new FilePart(segmentName, new PartSource() {
          @Override
          public long getLength() {
            return segmentTarFile.length();
          }

          @Override
          public String getFileName() {
            return "fileName";
          }

          @Override
          public InputStream createInputStream() throws IOException {
            return new BufferedInputStream(inputStream);
          }
        })
    };
    return parts;
  }
  public SegmentCompletionProtocol.Response segmentCommitStart(long offset, final String segmentName) {
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withInstanceId(_instanceId).withOffset(offset).withSegmentName(segmentName);
    SegmentCompletionProtocol.SegmentCommitStartRequest request = new SegmentCompletionProtocol.SegmentCommitStartRequest(params);

    return doHttp(request, null);
  }

  public SegmentCompletionProtocol.Response segmentCommitUpload(long offset, final String segmentName, final File segmentTarFile,
      final String controllerVipUrl) {
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withInstanceId(_instanceId).withOffset(offset).withSegmentName(segmentName);
    SegmentCompletionProtocol.SegmentCommitUploadRequest request = new SegmentCompletionProtocol.SegmentCommitUploadRequest(params);

    return postSegmentToVip(request, createFileParts(segmentName, segmentTarFile), controllerVipUrl);
  }

  public SegmentCompletionProtocol.Response segmentCommitEnd(long offset, final String segmentName, String segmentLocation,
      long memoryUsedBytes) {
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withInstanceId(_instanceId).withOffset(offset).withSegmentName(segmentName)
        .withSegmentLocation(segmentLocation).withMemoryUsedBytes(memoryUsedBytes);
    SegmentCompletionProtocol.SegmentCommitEndRequest request = new SegmentCompletionProtocol.SegmentCommitEndRequest(params);

    return doHttp(request, null);
  }

  public SegmentCompletionProtocol.Response segmentCommit(long offset, final String segmentName, long memoryUsedBytes,
      final File segmentTarFile) {
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withInstanceId(_instanceId).withOffset(offset).withSegmentName(segmentName).withMemoryUsedBytes(memoryUsedBytes);
    SegmentCompletionProtocol.SegmentCommitRequest request = new SegmentCompletionProtocol.SegmentCommitRequest(params);

    return doHttp(request, createFileParts(segmentName, segmentTarFile));
  }

  public SegmentCompletionProtocol.Response extendBuildTime(SegmentCompletionProtocol.Request.Params params) {
    params.withInstanceId(_instanceId);
    SegmentCompletionProtocol.ExtendBuildTimeRequest request = new SegmentCompletionProtocol.ExtendBuildTimeRequest(params);
    return doHttp(request, null);
  }

  public SegmentCompletionProtocol.Response segmentConsumed(SegmentCompletionProtocol.Request.Params params) {
    params.withInstanceId(_instanceId);
    SegmentCompletionProtocol.SegmentConsumedRequest request = new SegmentCompletionProtocol.SegmentConsumedRequest(params);
    return doHttp(request, null);
  }

  public SegmentCompletionProtocol.Response segmentStoppedConsuming(SegmentCompletionProtocol.Request.Params params) {
    params.withInstanceId(_instanceId);
    SegmentCompletionProtocol.SegmentStoppedConsuming request = new SegmentCompletionProtocol.SegmentStoppedConsuming(params);
    return doHttp(request, null);
  }

  // Much as it is tempting to merge postSegmentToVip() and doHttp() into one method, it is probably better to
  // keep them separate. One posts to the vip, whereas the other posts (or gets) from the current leader controller.

  // Post a segment to the VIP (used during split commit).
  // Note that the VipURL may either be http or https (depending on controller configuration).
  private SegmentCompletionProtocol.Response postSegmentToVip(SegmentCompletionProtocol.Request request, Part[] parts,
      String vipUrl) {
    SegmentCompletionProtocol.Response response = SegmentCompletionProtocol.RESP_NOT_SENT;
    HttpClient httpClient = new HttpClient();

    String hostPort;
    String protocol;
    try {
      URI uri = new URI(vipUrl, /* boolean escaped */ false);
      protocol = uri.getScheme();
      hostPort = uri.getAuthority();
    } catch (Exception e) {
      throw new RuntimeException("Could not make URI", e);
    }

    final String url;
    url = request.getUrl(hostPort, protocol);
    PostMethod postMethod = new PostMethod(url);
    postMethod.setRequestEntity(new MultipartRequestEntity(parts, new HttpMethodParams()));
    LOGGER.info("Sending request {} for {}", url, this.toString());
    try {
      int responseCode = httpClient.executeMethod(postMethod);
      // TODO Pick these up from constants defined in a common place.
      Header controllerHostNameHdr =  postMethod.getResponseHeader(CommonConstants.Controller.HOST_HTTP_HEADER);
      Header controllerVersionHdr = postMethod.getResponseHeader(CommonConstants.Controller.VERSION_HTTP_HEADER);

      if (responseCode >= 300) {
        LOGGER.error("Bad controller response code {} for {} from {}, version {}", responseCode, request,
            controllerHostNameHdr.toExternalForm(), controllerVersionHdr.toExternalForm());
        return response;
      } else {
        response = new SegmentCompletionProtocol.Response(postMethod.getResponseBodyAsString());
        LOGGER.info("Controller response {} for {} from {}", response.toJsonString(), request,
            controllerHostNameHdr.toExternalForm());
        return response;
      }
    } catch (IOException e) {
      LOGGER.error("IOException {}", this.toString(), e);
      return response;
    }
  }

  // Do GET or POST operations on the leader controller. Since we are communicating directly with the leader controller,
  // we must handle NOT_LEADER errors.
  private SegmentCompletionProtocol.Response doHttp(SegmentCompletionProtocol.Request request, Part[] parts) {
    ControllerLeaderLocator leaderLocator = ControllerLeaderLocator.getInstance();
    final String leaderHostPort = leaderLocator.getControllerLeader();
    if (leaderHostPort == null) {
      LOGGER.warn("No leader found {}", this.toString());
      return SegmentCompletionProtocol.RESP_NOT_LEADER;
    }
    SegmentCompletionProtocol.Response response = SegmentCompletionProtocol.RESP_NOT_SENT;
    HttpClient httpClient = new HttpClient();

    final String url;
    url = request.getUrl(leaderHostPort, "http");
    HttpMethodBase method;
    if (parts != null) {
      PostMethod postMethod = new PostMethod(url);
      postMethod.setRequestEntity(new MultipartRequestEntity(parts, new HttpMethodParams()));
      method = postMethod;
    } else {
      method = new GetMethod(url);
    }
    LOGGER.info("Sending request {} for {}", url, this.toString());
    try {
      int responseCode = httpClient.executeMethod(method);
      if (responseCode >= 300) {
        LOGGER.error("Bad controller response code {} for {}", responseCode, request);
        return response;
      } else {
        response = new SegmentCompletionProtocol.Response(method.getResponseBodyAsString());
        LOGGER.info("Controller response {} for {}", response.toJsonString(), request);
        if (response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER)) {
          leaderLocator.refreshControllerLeader();
        }
        return response;
      }
    } catch (IOException e) {
      LOGGER.error("IOException {}", this.toString(), e);
      leaderLocator.refreshControllerLeader();
      return response;
    }
  }
}
