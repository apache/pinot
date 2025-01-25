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
import javax.annotation.Nullable;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.slf4j.Logger;


public class PauselessSegmentCommitter extends SplitSegmentCommitter {
  public PauselessSegmentCommitter(Logger segmentLogger, ServerSegmentCompletionProtocolHandler protocolHandler,
      SegmentCompletionProtocol.Request.Params params, SegmentUploader segmentUploader,
      @Nullable String peerDownloadScheme) {
    super(segmentLogger, protocolHandler, params, segmentUploader, peerDownloadScheme);
  }

  /**
   * Commits a built segment without executing the segmentCommitStart step. This method assumes that
   * segmentCommitStart has already been executed prior to building the segment.
   *
   * The commit process follows these steps:
   * 1. Uploads the segment tar file to the designated storage location
   * 2. Updates the parameters with the new segment location
   * 3. Executes the segment commit end protocol with associated metadata
   *
   * @param segmentBuildDescriptor Contains the built segment information including the tar file
   *                              and associated metadata files
   * @return A SegmentCompletionProtocol.Response object indicating the commit status:
   *         - Returns the successful commit response if all steps complete successfully
   *         - Returns RESP_FAILED if either the upload fails or the commit end protocol fails
   *
   * @see SegmentCompletionProtocol
   * @see RealtimeSegmentDataManager.SegmentBuildDescriptor
   */
  @Override
  public SegmentCompletionProtocol.Response commit(
      RealtimeSegmentDataManager.SegmentBuildDescriptor segmentBuildDescriptor) {
    File segmentTarFile = segmentBuildDescriptor.getSegmentTarFile();

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
}
