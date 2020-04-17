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


// A segment uploader which uploads segments to the controller via the segment completion protocol's controller end
// point.
public class ControllerVipBasedSegmentUploader implements SegmentUploader {
  private final SegmentCompletionProtocol.Request.Params _params;
  private final ServerSegmentCompletionProtocolHandler _protocolHandler;
  private final Logger _segmentLogger;
  private final String _controllerVipUrl;

  public ControllerVipBasedSegmentUploader(Logger segmentLogger, ServerSegmentCompletionProtocolHandler protocolHandler,
      SegmentCompletionProtocol.Request.Params params, String controllerVipUrl) {
    _segmentLogger = segmentLogger;
    _protocolHandler = protocolHandler;
    _params = params;
    _controllerVipUrl = controllerVipUrl;
  }

  @Override
  public SegmentUploadStatus segmentUpload(File segmentFile) {
    SegmentCompletionProtocol.Response segmentCommitUploadResponse =
        _protocolHandler.segmentCommitUpload(_params, segmentFile, _controllerVipUrl);
    if (!segmentCommitUploadResponse.getStatus()
        .equals(SegmentCompletionProtocol.ControllerResponseStatus.UPLOAD_SUCCESS)) {
      _segmentLogger.warn("Segment upload failed with response {}", segmentCommitUploadResponse.toJsonString());
      return new SegmentUploadStatus(false, null);
    }
    return new SegmentUploadStatus(true, segmentCommitUploadResponse.getSegmentLocation());
  }
}
