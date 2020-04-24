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

import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;


/**
 * Util methods related to low level consumers' segment completion protocols. 
 */
public class SegmentCompletionProtocolUtils {
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
