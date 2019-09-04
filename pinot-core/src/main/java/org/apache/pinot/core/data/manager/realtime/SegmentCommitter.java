package org.apache.pinot.core.data.manager.realtime;

import java.io.File;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment committer class commits realtime segments through both split commit and regular commit pathways.
 */
public class SegmentCommitter {

  private boolean _isSplitCommit;
  private LLRealtimeSegmentDataManager.SegmentBuildDescriptor _segmentBuildDescriptor;
  private String _segmentNameStr;
  private volatile long _currentOffset;
  private volatile int _numRowsConsumed;
  private String _instanceId;
  private boolean _isOffHeap;
  private ServerSegmentCompletionProtocolHandler _protocolHandler;
  private PinotDataBufferMemoryManager _memoryManager;
  private IndexLoadingConfig _indexLoadingConfig;
  private SegmentCompletionProtocol.Response _prevResponse;

  private final Logger SEGMENT_LOGGER;

  public SegmentCommitter(boolean isSplitCommit, LLRealtimeSegmentDataManager.SegmentBuildDescriptor segmentBuildDescriptor,
      String segmentNameStr, long currentOffset, int numRowsConsumed, String instanceId, boolean isOffHeap,
      ServerSegmentCompletionProtocolHandler protocolHandler, PinotDataBufferMemoryManager memoryManager,
      IndexLoadingConfig indexLoadingConfig, SegmentCompletionProtocol.Response prevResponse) {
    _isSplitCommit = isSplitCommit;
    _segmentBuildDescriptor = segmentBuildDescriptor;
    _segmentNameStr = segmentNameStr;
    _currentOffset = currentOffset;
    _numRowsConsumed = numRowsConsumed;
    _instanceId = instanceId;
    _isOffHeap = isOffHeap;
    _protocolHandler = protocolHandler;
    _memoryManager = memoryManager;
    _indexLoadingConfig = indexLoadingConfig;
    _prevResponse = prevResponse;

    SEGMENT_LOGGER = LoggerFactory.getLogger(LLRealtimeSegmentDataManager.class.getName() + "_" + _segmentNameStr);
  }

  public SegmentCompletionProtocol.Response commitSegment() {
    final File segmentTarFile = new File(_segmentBuildDescriptor.getSegmentTarFilePath());
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();

    params.withSegmentName(_segmentNameStr).withOffset(_currentOffset).withNumRows(_numRowsConsumed)
        .withInstanceId(_instanceId).withBuildTimeMillis(_segmentBuildDescriptor.getBuildTimeMillis())
        .withSegmentSizeBytes(_segmentBuildDescriptor.getSegmentSizeBytes())
        .withWaitTimeMillis(_segmentBuildDescriptor.getWaitTimeMillis());

    if (_isOffHeap) {
      params.withMemoryUsedBytes(_memoryManager.getTotalAllocatedBytes());
    }

    if (_isSplitCommit) {
      // Send segmentStart, segmentUpload, & segmentCommitEnd to the controller
      // if that succeeds, swap in-memory segment with the one built.
      return doSplitCommit(params, segmentTarFile);
    } else {
      // Send segmentCommit() to the controller
      // if that succeeds, swap in-memory segment with the one built.
      return postSegmentCommitMsg();
    }
  }

  private SegmentCompletionProtocol.Response doSplitCommit(SegmentCompletionProtocol.Request.Params params, File segmentTarFile) {

    SegmentCompletionProtocol.Response segmentCommitStartResponse = _protocolHandler.segmentCommitStart(params);
    if (!segmentCommitStartResponse.getStatus()
        .equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE)) {
      SEGMENT_LOGGER.warn("CommitStart failed  with response {}", segmentCommitStartResponse.toJsonString());
      return SegmentCompletionProtocol.RESP_FAILED;
    }

    params = new SegmentCompletionProtocol.Request.Params();
    params.withOffset(_currentOffset).withSegmentName(_segmentNameStr).withInstanceId(_instanceId);
    SegmentCompletionProtocol.Response segmentCommitUploadResponse =
        _protocolHandler.segmentCommitUpload(params, segmentTarFile, _prevResponse.getControllerVipUrl());
    if (!segmentCommitUploadResponse.getStatus()
        .equals(SegmentCompletionProtocol.ControllerResponseStatus.UPLOAD_SUCCESS)) {
      SEGMENT_LOGGER.warn("Segment upload failed  with response {}", segmentCommitUploadResponse.toJsonString());
      return SegmentCompletionProtocol.RESP_FAILED;
    }

    params = new SegmentCompletionProtocol.Request.Params();
    params.withInstanceId(_instanceId).withOffset(_currentOffset).withSegmentName(_segmentNameStr)
        .withSegmentLocation(segmentCommitUploadResponse.getSegmentLocation()).withNumRows(_numRowsConsumed)
        .withBuildTimeMillis(_segmentBuildDescriptor.getBuildTimeMillis())
        .withSegmentSizeBytes(_segmentBuildDescriptor.getSegmentSizeBytes())
        .withWaitTimeMillis(_segmentBuildDescriptor.getWaitTimeMillis());
    if (_isOffHeap) {
      params.withMemoryUsedBytes(_memoryManager.getTotalAllocatedBytes());
    }
    SegmentCompletionProtocol.Response commitEndResponse;
    if (_indexLoadingConfig.isEnableSplitCommitEndWithMetadata()) {
      commitEndResponse = _protocolHandler.segmentCommitEndWithMetadata(params, _segmentBuildDescriptor.getMetadataFiles());
    } else {
      commitEndResponse = _protocolHandler.segmentCommitEnd(params);
    }

    if (!commitEndResponse.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS)) {
      SEGMENT_LOGGER.warn("CommitEnd failed  with response {}", commitEndResponse.toJsonString());
      return SegmentCompletionProtocol.RESP_FAILED;
    }
    return commitEndResponse;
  }

  protected SegmentCompletionProtocol.Response postSegmentCommitMsg() {
    final File segmentTarFile = new File(_segmentBuildDescriptor.getSegmentTarFilePath());
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();

    params.withInstanceId(_instanceId).withOffset(_currentOffset).withSegmentName(_segmentNameStr)
        .withNumRows(_numRowsConsumed).withInstanceId(_instanceId)
        .withBuildTimeMillis(_segmentBuildDescriptor.getBuildTimeMillis())
        .withSegmentSizeBytes(_segmentBuildDescriptor.getSegmentSizeBytes())
        .withWaitTimeMillis(_segmentBuildDescriptor.getWaitTimeMillis());


    if (_isOffHeap) {
      params.withMemoryUsedBytes(_memoryManager.getTotalAllocatedBytes());
    }

    SegmentCompletionProtocol.Response response = _protocolHandler.segmentCommit(params, segmentTarFile);
    if (!response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS)) {
      SEGMENT_LOGGER.warn("Commit failed  with response {}", response.toJsonString());
    }
    return response;
  }
}
