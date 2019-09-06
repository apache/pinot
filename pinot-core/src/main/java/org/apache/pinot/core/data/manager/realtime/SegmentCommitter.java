package org.apache.pinot.core.data.manager.realtime;

import java.io.File;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment committer class commits realtime segments through both split commit and regular commit pathways.
 */
public class SegmentCommitter {

  private boolean _isSplitCommit;
  private String _segmentNameStr;
  private ServerSegmentCompletionProtocolHandler _protocolHandler;
  private IndexLoadingConfig _indexLoadingConfig;
  private SegmentCompletionProtocol.Response _prevResponse;
  private SegmentCompletionProtocol.Request.Params _params;

  private final Logger SEGMENT_LOGGER;

  public SegmentCommitter(boolean isSplitCommit, String segmentNameStr, ServerSegmentCompletionProtocolHandler protocolHandler,
      IndexLoadingConfig indexLoadingConfig, SegmentCompletionProtocol.Response prevResponse, SegmentCompletionProtocol.Request.Params params) {
    _isSplitCommit = isSplitCommit;
    _segmentNameStr = segmentNameStr;
    _protocolHandler = protocolHandler;
    _indexLoadingConfig = indexLoadingConfig;
    _prevResponse = prevResponse;
    _params = params;

    SEGMENT_LOGGER = LoggerFactory.getLogger(LLRealtimeSegmentDataManager.class.getName() + "_" + _segmentNameStr);
  }

  public SegmentCompletionProtocol.Response commitSegment(LLRealtimeSegmentDataManager.SegmentBuildDescriptor segmentBuildDescriptor,
      long currentOffset, int numRowsConsumed) {
    if (_isSplitCommit) {
      // Send segmentStart, segmentUpload, & segmentCommitEnd to the controller
      // if that succeeds, swap in-memory segment with the one built.
      return doSplitCommit(segmentBuildDescriptor, currentOffset, numRowsConsumed);
    } else {
      // Send segmentCommit() to the controller
      // if that succeeds, swap in-memory segment with the one built.
      return postSegmentCommitMsg(segmentBuildDescriptor, currentOffset, numRowsConsumed);
    }
  }

  private SegmentCompletionProtocol.Response doSplitCommit(
      LLRealtimeSegmentDataManager.SegmentBuildDescriptor segmentBuildDescriptor, long currentOffset, int numRowsConsumed) {

    final File segmentTarFile = new File(segmentBuildDescriptor.getSegmentTarFilePath());

    SegmentCompletionProtocol.Response segmentCommitStartResponse = _protocolHandler.segmentCommitStart(_params);
    if (!segmentCommitStartResponse.getStatus()
        .equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE)) {
      SEGMENT_LOGGER.warn("CommitStart failed  with response {}", segmentCommitStartResponse.toJsonString());
      return SegmentCompletionProtocol.RESP_FAILED;
    }

    _params.withOffset(currentOffset);

    SegmentCompletionProtocol.Response segmentCommitUploadResponse =
        _protocolHandler.segmentCommitUpload(_params, segmentTarFile, _prevResponse.getControllerVipUrl());
    if (!segmentCommitUploadResponse.getStatus()
        .equals(SegmentCompletionProtocol.ControllerResponseStatus.UPLOAD_SUCCESS)) {
      SEGMENT_LOGGER.warn("Segment upload failed  with response {}", segmentCommitUploadResponse.toJsonString());
      return SegmentCompletionProtocol.RESP_FAILED;
    }

    _params.withSegmentLocation(segmentCommitUploadResponse.getSegmentLocation()).withNumRows(numRowsConsumed)
        .withBuildTimeMillis(segmentBuildDescriptor.getBuildTimeMillis())
        .withSegmentSizeBytes(segmentBuildDescriptor.getSegmentSizeBytes())
        .withWaitTimeMillis(segmentBuildDescriptor.getWaitTimeMillis());

    SegmentCompletionProtocol.Response commitEndResponse;
    if (_indexLoadingConfig.isEnableSplitCommitEndWithMetadata()) {
      commitEndResponse = _protocolHandler.segmentCommitEndWithMetadata(_params, segmentBuildDescriptor.getMetadataFiles());
    } else {
      commitEndResponse = _protocolHandler.segmentCommitEnd(_params);
    }

    if (!commitEndResponse.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS)) {
      SEGMENT_LOGGER.warn("CommitEnd failed  with response {}", commitEndResponse.toJsonString());
      return SegmentCompletionProtocol.RESP_FAILED;
    }
    return commitEndResponse;
  }

  protected SegmentCompletionProtocol.Response postSegmentCommitMsg(
      LLRealtimeSegmentDataManager.SegmentBuildDescriptor segmentBuildDescriptor, long currentOffset, int numRowsConsumed) {

    final File segmentTarFile = new File(segmentBuildDescriptor.getSegmentTarFilePath());
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();

    params.withNumRows(numRowsConsumed).withOffset(currentOffset)
        .withBuildTimeMillis(segmentBuildDescriptor.getBuildTimeMillis())
        .withSegmentSizeBytes(segmentBuildDescriptor.getSegmentSizeBytes())
        .withWaitTimeMillis(segmentBuildDescriptor.getWaitTimeMillis());

    SegmentCompletionProtocol.Response response = _protocolHandler.segmentCommit(params, segmentTarFile);
    if (!response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS)) {
      SEGMENT_LOGGER.warn("Commit failed  with response {}", response.toJsonString());
    }
    return response;
  }
}
