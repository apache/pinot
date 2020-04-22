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
package org.apache.pinot.core.util;

import java.io.File;
import java.net.URI;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.server.realtime.ControllerLeaderLocator;
import org.slf4j.Logger;


/**
 * Util methods related to low level consumers' segment completion protocols. 
 */
public class SegmentCompletionProtocolUtils {
  public static SegmentCompletionProtocol.Response uploadSegmentWithFileUploadDownloadClient(
      FileUploadDownloadClient fileUploadDownloadClient, File segmentFile, String controllerVipUrl, String segmentName,
      int segmentUploadRequestTimeoutMs, Logger logger) {
    SegmentCompletionProtocol.Response response;
    try {
      String responseStr = fileUploadDownloadClient
          .uploadSegment(new URI(controllerVipUrl), segmentName, segmentFile, null, null, segmentUploadRequestTimeoutMs)
          .getResponse();
      response = SegmentCompletionProtocol.Response.fromJsonString(responseStr);
      logger.info("Controller response {} for {}", response.toJsonString(), controllerVipUrl);
      if (response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER)) {
        ControllerLeaderLocator.getInstance().invalidateCachedControllerLeader();
      }
    } catch (Exception e) {
      // Catch all exceptions, we want the protocol to handle the case assuming the request was never sent.
      response = SegmentCompletionProtocol.RESP_NOT_SENT;
      logger.error("Could not send request {}", controllerVipUrl, e);
      // Invalidate controller leader cache, as exception could be because of leader being down (deployment/failure) and hence unable to send {@link SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER}
      // If cache is not invalidated, we will not recover from exceptions until the controller comes back up
      ControllerLeaderLocator.getInstance().invalidateCachedControllerLeader();
    }
    return response;
  }

  /**
   * raise a metric indicating the response we received from the controller
   */
  public static void raiseSegmentCompletionProtocolResponseMetric(ServerMetrics serverMetrics, 
      SegmentCompletionProtocol.Response response) {
    switch (response.getStatus()) {
      case NOT_SENT:
        serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_NOT_SENT, 1);
        break;
      case COMMIT:
        serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_COMMIT, 1);
        break;
      case HOLD:
        serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_HOLD, 1);
        break;
      case CATCH_UP:
        serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_CATCH_UP, 1);
        break;
      case DISCARD:
        serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_DISCARD, 1);
        break;
      case KEEP:
        serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_KEEP, 1);
        break;
      case NOT_LEADER:
        serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_NOT_LEADER, 1);
        break;
      case FAILED:
        serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_FAILED, 1);
        break;
      case COMMIT_SUCCESS:
        serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_COMMIT_SUCCESS, 1);
        break;
      case COMMIT_CONTINUE:
        serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_COMMIT_CONTINUE, 1);
        break;
      case PROCESSED:
        serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_PROCESSED, 1);
        break;
      case UPLOAD_SUCCESS:
        serverMetrics.addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_UPLOAD_SUCCESS, 1);
        break;
    }
  }
}
