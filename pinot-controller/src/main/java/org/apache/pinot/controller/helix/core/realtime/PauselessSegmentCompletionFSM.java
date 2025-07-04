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
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


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
      LOGGER.info(
          "Starting to commit changes to ZK and ideal state for the segment:{} during pauseles ingestion as the "
              + "leader has been selected", _segmentName);
      _segmentManager.commitSegmentMetadataToCommitting(
          TableNameBuilder.REALTIME.tableNameWithType(_segmentName.getTableName()), committingSegmentDescriptor);
    } catch (Exception e) {
      // this aims to handle the failures during commitSegmentStartMetadata
      // we abort the state machine to allow commit protocol to start from the beginning
      // the server would then retry the commit protocol from the start
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
