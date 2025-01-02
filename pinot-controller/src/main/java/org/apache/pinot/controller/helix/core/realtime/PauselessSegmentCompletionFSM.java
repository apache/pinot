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
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.helix.core.realtime.segment.CommittingSegmentDescriptor;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class PauselessSegmentCompletionFSM extends BlockingSegmentCompletionFSM {
  public PauselessSegmentCompletionFSM(PinotLLCRealtimeSegmentManager segmentManager,
      SegmentCompletionManager segmentCompletionManager, LLCSegmentName segmentName,
      SegmentZKMetadata segmentMetadata) {
    super(segmentManager, segmentCompletionManager, segmentName, segmentMetadata);
    if (segmentMetadata.getStatus() == CommonConstants.Segment.Realtime.Status.COMMITTING) {
      StreamPartitionMsgOffsetFactory factory =
          _segmentCompletionManager.getStreamPartitionMsgOffsetFactory(_segmentName);
      StreamPartitionMsgOffset endOffset = factory.create(segmentMetadata.getEndOffset());
      _state = BlockingSegmentCompletionFSMState.COMMITTED;
      _winningOffset = endOffset;
      _winner = "UNKNOWN";
    }
  }

  /*
   * A server has sent segmentConsumed() message. The caller will save the segment if we return
   * COMMIT_CONTINUE. We need to verify that it is the same server that we notified as the winner
   * and the offset is the same as what is coming in with the commit. We can then move to
   * COMMITTER_UPLOADING and wait for the segmentCommitEnd() call.
   *
   * In case of discrepancy we move the state machine to ABORTED state so that this FSM is removed
   * from the map, and things start over. In this case, we respond to the server with a 'hold' so
   * that they re-transmit their segmentConsumed() message and start over.
   */
  @Override
  public SegmentCompletionProtocol.Response segmentCommitStart(SegmentCompletionProtocol.Request.Params reqParams) {
    String instanceId = reqParams.getInstanceId();
    StreamPartitionMsgOffset offset = _streamPartitionMsgOffsetFactory.create(reqParams.getStreamPartitionMsgOffset());
    long now = _segmentCompletionManager.getCurrentTimeMs();
    if (_excludedServerStateMap.contains(instanceId)) {
      _logger.warn("Not accepting commit from {} since it had stoppd consuming", instanceId);
      return SegmentCompletionProtocol.RESP_FAILED;
    }
    synchronized (this) {
      _logger.info("Processing segmentCommitStart({}, {})", instanceId, offset);
      switch (_state) {
        case PARTIAL_CONSUMING:
          return partialConsumingCommit(instanceId, offset, now);

        case HOLDING:
          return holdingCommit(instanceId, offset, now);

        case COMMITTER_DECIDED:
          return committerDecidedCommit(instanceId, offset, now);

        case COMMITTER_NOTIFIED:
          SegmentCompletionProtocol.Response response = committerNotifiedCommit(instanceId, offset, now);
          try {
            if (response == SegmentCompletionProtocol.RESP_COMMIT_CONTINUE) {
              CommittingSegmentDescriptor committingSegmentDescriptor =
                  CommittingSegmentDescriptor.fromSegmentCompletionReqParams(reqParams);
              LOGGER.info(
                  "Starting to commit changes to ZK and ideal state for the segment:{} as the leader has been selected",
                  _segmentName);
              _segmentManager.commitSegmentStartMetadata(
                  TableNameBuilder.REALTIME.tableNameWithType(_segmentName.getTableName()),
                  committingSegmentDescriptor);
            }
          } catch (Exception e) {
            // this aims to handle the failures during commitSegmentStartMetadata
            // we abort the state machine to allow commit protocol to start from the beginning
            // the server would then retry the commit protocol from the start
            return abortAndReturnFailed();
          }
          return response;
        case COMMITTER_UPLOADING:
          return committerUploadingCommit(instanceId, offset, now);

        case COMMITTING:
          return committingCommit(instanceId, offset, now);

        case COMMITTED:
          return committedCommit(instanceId, offset);

        case ABORTED:
          return hold(instanceId, offset);

        default:
          return fail(instanceId, offset);
      }
    }
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

  protected SegmentCompletionProtocol.Response commitSegment(SegmentCompletionProtocol.Request.Params reqParams,
      CommittingSegmentDescriptor committingSegmentDescriptor) {
    String instanceId = reqParams.getInstanceId();
    StreamPartitionMsgOffset offset =
        _streamPartitionMsgOffsetFactory.create(reqParams.getStreamPartitionMsgOffset());
    if (!_state.equals(BlockingSegmentCompletionFSMState.COMMITTER_UPLOADING)) {
      // State changed while we were out of sync. return a failed commit.
      _logger.warn("State change during upload: state={} segment={} winner={} winningOffset={}", _state,
          _segmentName.getSegmentName(), _winner, _winningOffset);
      return SegmentCompletionProtocol.RESP_FAILED;
    }
    _logger.info("Committing segment {} at offset {} winner {}", _segmentName.getSegmentName(), offset, instanceId);
    _state = BlockingSegmentCompletionFSMState.COMMITTING;
    // In case of splitCommit, the segment is uploaded to a unique file name indicated by segmentLocation,
    // so we need to move the segment file to its permanent location first before committing the metadata.
    // The committingSegmentDescriptor is then updated with the permanent segment location to be saved in metadata
    // store.
    try {
      _segmentManager.commitSegmentFile(_realtimeTableName, committingSegmentDescriptor);
    } catch (Exception e) {
      _logger.error("Caught exception while committing segment file for segment: {}", _segmentName.getSegmentName(),
          e);
      return SegmentCompletionProtocol.RESP_FAILED;
    }
    try {
      // Convert to a controller uri if the segment location uses local file scheme.
      if (CommonConstants.Segment.LOCAL_SEGMENT_SCHEME
          .equalsIgnoreCase(URIUtils.getUri(committingSegmentDescriptor.getSegmentLocation()).getScheme())) {
        committingSegmentDescriptor.setSegmentLocation(URIUtils
            .constructDownloadUrl(_controllerVipUrl, TableNameBuilder.extractRawTableName(_realtimeTableName),
                _segmentName.getSegmentName()));
      }
      _segmentManager.commitSegmentEndMetadata(_realtimeTableName, committingSegmentDescriptor);
    } catch (Exception e) {
      _logger
          .error("Caught exception while committing segment metadata for segment: {}", _segmentName.getSegmentName(),
              e);
      return SegmentCompletionProtocol.RESP_FAILED;
    }

    _state = BlockingSegmentCompletionFSMState.COMMITTED;
    _logger.info("Committed segment {} at offset {} winner {}", _segmentName.getSegmentName(), offset, instanceId);
    return SegmentCompletionProtocol.RESP_COMMIT_SUCCESS;
  }


  @Override
  // A common method when the state is > COMMITTER_NOTIFIED.
  protected SegmentCompletionProtocol.Response processConsumedAfterCommitStart(String instanceId,
      StreamPartitionMsgOffset offset, long now) {
    SegmentCompletionProtocol.Response response;
    // We have already picked a winner, and may or many not have heard from them.
    // Common case here is that another server is coming back to us with its offset. We either respond back with
    // HOLD or CATCHUP.
    // It may be that we never heard from the committer, or the committer is taking too long to commit the segment.
    // In that case, we abort the FSM and start afresh (i.e, return HOLD).
    // If the winner is coming back again, then we have some more conditions to look at.
    response = abortIfTooLateAndReturnHold(now, instanceId, offset);
    if (response != null) {
      return response;
    }
    if (instanceId.equals(_winner)) {
      // The winner is coming back to report its offset. Take a decision based on the offset reported, and whether we
      // already notified them
      // Winner is supposedly already in the commit call. Something wrong.
      LOGGER.warn(
          "{}:Aborting FSM because winner is reporting a segment while it is also committing instance={} offset={} "
              + "now={}", _state, instanceId, offset, now);
      // Ask them to hold, just in case the committer fails for some reason..
      return abortAndReturnHold(now, instanceId, offset);
    } else {
      // Common case: A different instance is reporting.
      if (offset.compareTo(_winningOffset) == 0) {
        // The winner has already updated the segment's ZK metadata for the committing segment.
        // Additionally, a new consuming segment has been created for pauseless ingestion.
        // Return "keep" to allow the server to build the segment and begin ingestion for the new consuming segment.
        response = keep(instanceId, offset);
      } else if (offset.compareTo(_winningOffset) < 0) {
        response = catchup(instanceId, offset);
      } else {
        // We have not yet committed, so ask the new responder to hold. They may be the new leader in case the
        // committer fails.
        response = hold(instanceId, offset);
      }
    }
    return response;
  }
}
