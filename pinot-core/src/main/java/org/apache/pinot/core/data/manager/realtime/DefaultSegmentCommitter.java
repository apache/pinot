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
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.slf4j.Logger;


/**
 * Sends segmentCommit() to the controller.
 * If that succeeds, swap in-memory segment with the one built.
 */
public class DefaultSegmentCommitter implements SegmentCommitter {
  private final SegmentCompletionProtocol.Request.Params _params;
  private final ServerSegmentCompletionProtocolHandler _protocolHandler;

  private final Logger _segmentLogger;

  public DefaultSegmentCommitter(Logger segmentLogger, ServerSegmentCompletionProtocolHandler protocolHandler,
      SegmentCompletionProtocol.Request.Params params) {
    _segmentLogger = segmentLogger;
    _protocolHandler = protocolHandler;
    _params = params;
  }

  @Override
  public SegmentCompletionProtocol.Response commit(
      LLRealtimeSegmentDataManager.SegmentBuildDescriptor segmentBuildDescriptor) {
    File segmentTarFile = segmentBuildDescriptor.getSegmentTarFile();

    SegmentCompletionProtocol.Response response = _protocolHandler.segmentCommit(_params, segmentTarFile);
    if (!response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS)) {
      _segmentLogger.warn("Commit failed  with response {}", response.toJsonString());
    }
    return response;
  }
}
