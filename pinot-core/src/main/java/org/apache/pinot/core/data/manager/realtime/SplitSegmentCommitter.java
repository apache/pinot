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
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;


/**
 * Sends segmentStart, segmentUpload, & segmentCommitEnd to the controller
 * If that succeeds, swap in-memory segment with the one built.
 */
public class SplitSegmentCommitter implements SegmentCommitter {
  private final SegmentCompletionProtocol.Request.Params _params;
  private final ServerSegmentCompletionProtocolHandler _protocolHandler;
  private final TableConfig _tableConfig;
  private final SegmentUploader _segmentUploader;
  private final Logger _segmentLogger;
  private final boolean _isEnableSplitCommitEndWithMetadata;

  public SplitSegmentCommitter(Logger segmentLogger, ServerSegmentCompletionProtocolHandler protocolHandler,
      TableConfig tableConfig, SegmentCompletionProtocol.Request.Params params, SegmentUploader segmentUploader,
      boolean isEnableSplitCommitEndWithMetadata) {
    _segmentLogger = segmentLogger;
    _protocolHandler = protocolHandler;
    _tableConfig = tableConfig;
    _params = new SegmentCompletionProtocol.Request.Params(params);
    _segmentUploader = segmentUploader;
    _isEnableSplitCommitEndWithMetadata = isEnableSplitCommitEndWithMetadata;
  }

  @Override
  public SegmentCompletionProtocol.Response commit(long currentOffset, int numRowsConsumed, LLRealtimeSegmentDataManager.SegmentBuildDescriptor segmentBuildDescriptor) {
    final File segmentTarFile = new File(segmentBuildDescriptor.getSegmentTarFilePath());

    SegmentCompletionProtocol.Response segmentCommitStartResponse = _protocolHandler.segmentCommitStart(_params);
    if (!segmentCommitStartResponse.getStatus()
        .equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE)) {
      _segmentLogger.warn("CommitStart failed  with response {}", segmentCommitStartResponse.toJsonString());
      return SegmentCompletionProtocol.RESP_FAILED;
    }

    URI segmentLocation = _segmentUploader.uploadSegment(segmentTarFile, new LLCSegmentName(_params.getSegmentName()));
    if (segmentLocation == null) {
      return SegmentCompletionProtocol.RESP_FAILED;
    }
    _params.withSegmentLocation(segmentLocation.toString());

    SegmentCompletionProtocol.Response commitEndResponse;
    if (_isEnableSplitCommitEndWithMetadata) {
      commitEndResponse =
          _protocolHandler.segmentCommitEndWithMetadata(_params, segmentBuildDescriptor.getMetadataFiles());
    } else {
      commitEndResponse = _protocolHandler.segmentCommitEnd(_params);
    }

    if (!commitEndResponse.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS)) {
      _segmentLogger.warn("CommitEnd failed with response {}", commitEndResponse.toJsonString());
      return SegmentCompletionProtocol.RESP_FAILED;
    }
    return commitEndResponse;
  }
}
