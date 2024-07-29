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

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.net.URI;
import javax.annotation.Nullable;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.StringUtil;
import org.slf4j.Logger;


/**
 * Sends segmentStart, segmentUpload, & segmentCommitEnd to the controller
 * If that succeeds, swap in-memory segment with the one built.
 */
public class SplitSegmentCommitter implements SegmentCommitter {
  private final SegmentCompletionProtocol.Request.Params _params;
  private final ServerSegmentCompletionProtocolHandler _protocolHandler;
  private final SegmentUploader _segmentUploader;
  private final String _peerDownloadScheme;
  private final Logger _segmentLogger;

  public SplitSegmentCommitter(Logger segmentLogger, ServerSegmentCompletionProtocolHandler protocolHandler,
      SegmentCompletionProtocol.Request.Params params, SegmentUploader segmentUploader,
      @Nullable String peerDownloadScheme) {
    _segmentLogger = segmentLogger;
    _protocolHandler = protocolHandler;
    _params = new SegmentCompletionProtocol.Request.Params(params);
    _segmentUploader = segmentUploader;
    _peerDownloadScheme = peerDownloadScheme;
  }

  @VisibleForTesting
  SegmentUploader getSegmentUploader() {
    return _segmentUploader;
  }

  public SplitSegmentCommitter(Logger segmentLogger, ServerSegmentCompletionProtocolHandler protocolHandler,
      SegmentCompletionProtocol.Request.Params params, SegmentUploader segmentUploader) {
    this(segmentLogger, protocolHandler, params, segmentUploader, null);
  }

  @Override
  public SegmentCompletionProtocol.Response commit(
      RealtimeSegmentDataManager.SegmentBuildDescriptor segmentBuildDescriptor) {
    File segmentTarFile = segmentBuildDescriptor.getSegmentTarFile();

    SegmentCompletionProtocol.Response segmentCommitStartResponse = _protocolHandler.segmentCommitStart(_params);
    if (!segmentCommitStartResponse.getStatus()
        .equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE)) {
      _segmentLogger.warn("CommitStart failed  with response {}", segmentCommitStartResponse.toJsonString());
      return SegmentCompletionProtocol.RESP_FAILED;
    }

    String segmentLocation = uploadSegment(segmentTarFile, _segmentUploader, _params);
    if (segmentLocation == null) {
      return SegmentCompletionProtocol.RESP_FAILED;
    }
    _params.withSegmentLocation(segmentLocation);

    SegmentCompletionProtocol.Response commitEndResponse =
        _protocolHandler.segmentCommitEndWithMetadata(_params, segmentBuildDescriptor.getMetadataFiles());

    if (!commitEndResponse.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS)) {
      _segmentLogger.warn("CommitEnd failed with response {}", commitEndResponse.toJsonString());
      return SegmentCompletionProtocol.RESP_FAILED;
    }
    return commitEndResponse;
  }

  // Return null iff the segment upload fails.
  protected String uploadSegment(File segmentTarFile, SegmentUploader segmentUploader,
      SegmentCompletionProtocol.Request.Params params) {
    URI segmentLocation = segmentUploader.uploadSegment(segmentTarFile, new LLCSegmentName(params.getSegmentName()));
    if (segmentLocation != null) {
      return segmentLocation.toString();
    }
    if (_peerDownloadScheme != null) {
        return StringUtil.join("/", CommonConstants.Segment.PEER_SEGMENT_DOWNLOAD_SCHEME,
            params.getSegmentName());
    }
    return null;
  }
}
