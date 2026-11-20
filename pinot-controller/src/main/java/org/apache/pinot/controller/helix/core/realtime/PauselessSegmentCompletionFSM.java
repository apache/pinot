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
package org.apache.pinot.controller.helix.core.realtime;

import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.core.realtime.segment.CommittingSegmentDescriptor;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// FSM for the segments of a pauseless table. The commit start moves the segment to COMMITTING in ZK with its end
/// offset, creates the next consuming segment and updates the IdealState, all before the committer builds the segment.
/// From then on no other replica can commit the segment, so the commit deadline of the FSM no longer aborts it. The max
/// segment completion time bounds the commit instead: after it, the commit end fails and the validation manager repairs
/// the segment.
public class PauselessSegmentCompletionFSM extends BlockingSegmentCompletionFSM {

  public PauselessSegmentCompletionFSM(PinotLLCRealtimeSegmentManager segmentManager,
      SegmentCompletionManager segmentCompletionManager, LLCSegmentName segmentName,
      SegmentZKMetadata segmentMetadata) {
    super(segmentManager, segmentCompletionManager, segmentName, segmentMetadata);
  }

  @Override
  protected SegmentCompletionProtocol.Response committerNotifiedCommit(
      SegmentCompletionProtocol.Request.Params reqParams, long now) {
    String instanceId = reqParams.getInstanceId();
    StreamPartitionMsgOffset offset = _streamPartitionMsgOffsetFactory.create(reqParams.getStreamPartitionMsgOffset());
    SegmentCompletionProtocol.Response response = checkBadCommitRequest(instanceId, offset, now);
    if (response != null) {
      return response;
    }
    try {
      CommittingSegmentDescriptor committingSegmentDescriptor =
          CommittingSegmentDescriptor.fromSegmentCompletionReqParams(reqParams);
      _logger.info(
          "Starting to commit changes to ZK and ideal state for the segment:{} during pauseless ingestion as the "
              + "leader has been selected", _segmentName);
      _segmentManager.commitSegmentMetadataToCommitting(
          TableNameBuilder.REALTIME.tableNameWithType(_segmentName.getTableName()), committingSegmentDescriptor);
    } catch (Exception e) {
      // this aims to handle the failures during commitSegmentStartMetadata
      // we abort the state machine to allow commit protocol to start from the beginning
      // the server would then retry the commit protocol from the start
      _logger.error("Failed to commit segment metadata to COMMITTING for segment: {}", _segmentName, e);
      return abortAndReturnFailed();
    }
    _logger.info("{}:Uploading for instance={} offset={}", _state, instanceId, offset);
    _state = BlockingSegmentCompletionFSMState.COMMITTER_UPLOADING;
    long commitTimeMs = now - _startTimeMs;
    if (commitTimeMs > _initialCommitTimeMs) {
      // We assume that the commit time holds for all partitions. It is possible, though, that one partition
      // commits at a lower time than another partition, and the two partitions are going simultaneously,
      // and we may not get the maximum value all the time.
      _segmentCompletionManager.setCommitTime(_segmentName.getTableName(), commitTimeMs);
    }
    return SegmentCompletionProtocol.RESP_COMMIT_CONTINUE;
  }

  @Override
  public SegmentCompletionProtocol.Response extendBuildTime(final String instanceId,
      final StreamPartitionMsgOffset offset, final int extTimeSec) {
    final long now = _segmentCompletionManager.getCurrentTimeMs();
    synchronized (this) {
      _logger.info("Processing extendBuildTime({}, {}, {})", instanceId, offset, extTimeSec);
      switch (_state) {
        case PARTIAL_CONSUMING:
        case HOLDING:
        case COMMITTER_DECIDED:
        case COMMITTER_NOTIFIED:
          return fail(instanceId, offset);
        case COMMITTER_UPLOADING:
          return committerNotifiedExtendBuildTime(instanceId, offset, extTimeSec, now);
        case COMMITTING:
        case COMMITTED:
        case ABORTED:
        default:
          return fail(instanceId, offset);
      }
    }
  }

  /// Reached only in COMMITTER_UPLOADING (see [#extendBuildTime]). Accepts an extension from the committer even after
  /// the commit deadline or past [SegmentCompletionManager#getMaxCommitTimeForAllSegmentsSeconds()]. Fails an
  /// extension from any other server or for any other offset, without aborting the FSM.
  @Override
  protected SegmentCompletionProtocol.Response committerNotifiedExtendBuildTime(String instanceId,
      StreamPartitionMsgOffset offset, int extTimeSec, long now) {
    if (!instanceId.equals(_winner) || offset.compareTo(_winningOffset) != 0) {
      return fail(instanceId, offset);
    }
    logIfPastCommitDeadline(now, instanceId, offset);
    // After the commit start, the commit deadline only decides which requests are logged as late
    _maxTimeAllowedToCommitMs = now + extTimeSec * 1000L;
    return SegmentCompletionProtocol.RESP_PROCESSED;
  }

  /// After the commit start, the commit deadline does not abort the FSM: an abort cannot hand the commit to another
  /// replica, it only makes the commit end of the committer fail. Before the commit start, the deadline lets another
  /// replica take the commit over, as for a blocking table.
  @Override
  protected SegmentCompletionProtocol.Response abortIfTooLateAndReturnHold(long now, String instanceId,
      StreamPartitionMsgOffset offset) {
    if (_state != BlockingSegmentCompletionFSMState.COMMITTER_UPLOADING
        && _state != BlockingSegmentCompletionFSMState.COMMITTING) {
      return super.abortIfTooLateAndReturnHold(now, instanceId, offset);
    }
    logIfPastCommitDeadline(now, instanceId, offset);
    return null;
  }

  private void logIfPastCommitDeadline(long now, String instanceId, StreamPartitionMsgOffset offset) {
    if (now > _maxTimeAllowedToCommitMs) {
      _logger.info("{}:Ignoring commit deadline after commit start (too late) instance={} offset={} now={} start={}",
          _state, instanceId, offset, now, _startTimeMs);
    }
  }

  /// Checks the segment ZK metadata before the commit end moves the uploaded segment file to the deep store path of the
  /// segment: the segment must still be COMMITTING with the end offset of this commit, within the max segment
  /// completion time. The ZK metadata update after the move checks the status and the time too, but after the max
  /// segment completion time the validation manager may already have uploaded another replica's copy to that path,
  /// and the move would overwrite it.
  @Override
  protected SegmentCompletionProtocol.Response commitSegment(SegmentCompletionProtocol.Request.Params reqParams,
      CommittingSegmentDescriptor committingSegmentDescriptor) {
    String segmentName = _segmentName.getSegmentName();
    StreamPartitionMsgOffset offset = _streamPartitionMsgOffsetFactory.create(reqParams.getStreamPartitionMsgOffset());
    try {
      SegmentZKMetadata segmentZKMetadata = _segmentManager.getSegmentZKMetadata(_realtimeTableName, segmentName, null);
      String endOffset = segmentZKMetadata.getEndOffset();
      if (segmentZKMetadata.getStatus() != Status.COMMITTING || endOffset == null
          || _streamPartitionMsgOffsetFactory.create(endOffset).compareTo(offset) != 0) {
        _logger.warn("Rejecting commit end of segment: {} at offset: {}, segment ZK status: {}, end offset: {}",
            segmentName, offset, segmentZKMetadata.getStatus(), endOffset);
        return SegmentCompletionProtocol.RESP_FAILED;
      }
      if (_segmentManager.isExceededMaxSegmentCompletionTime(_realtimeTableName, segmentName,
          _segmentManager.getCurrentTimeMs())) {
        _logger.warn("Rejecting commit end of segment: {} after the max segment completion time", segmentName);
        return SegmentCompletionProtocol.RESP_FAILED;
      }
    } catch (Exception e) {
      _logger.error("Caught exception while checking segment ZK metadata before the commit end of segment: {}",
          segmentName, e);
      return SegmentCompletionProtocol.RESP_FAILED;
    }
    return super.commitSegment(reqParams, committingSegmentDescriptor);
  }

  @Override
  protected void commitSegmentMetadata(String realtimeTableName,
      CommittingSegmentDescriptor committingSegmentDescriptor) {
    _segmentManager.commitSegmentMetadataToDone(realtimeTableName, committingSegmentDescriptor);
  }

  @Override
  protected SegmentCompletionProtocol.Response handleNonWinnerCase(String instanceId, StreamPartitionMsgOffset offset) {
    // Common case: A different instance is reporting.
    if (offset.compareTo(_winningOffset) == 0) {
      // The winner has already updated the segment's ZK metadata for the committing segment.
      // Additionally, a new consuming segment has been created for pauseless ingestion.
      // Return "keep" to allow the server to build the segment and begin ingestion for the new consuming segment.
      return keep(instanceId, offset);
    } else if (offset.compareTo(_winningOffset) < 0) {
      return catchup(instanceId, offset);
    } else {
      // We have not yet committed, so ask the new responder to hold. They may be the new leader in case the
      // committer fails.
      return hold(instanceId, offset);
    }
  }
}
