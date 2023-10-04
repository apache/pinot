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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.realtime.segment.CommittingSegmentDescriptor;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a singleton class in the controller that drives the state machines for segments that are in the
 * committing stage.
 *
 * SegmentCompletionManager has a sub-class that represents the FSM that the segment goes through while
 * executing the segment completion protocol between pinot servers and pinot controller. The protocol is
 * described in SegmentCompletionProtocol.
 */
public class SegmentCompletionManager {
  // TODO Can we log using the segment name in the log message?
  public static final Logger LOGGER = LoggerFactory.getLogger(SegmentCompletionManager.class);

  private enum State {
    PARTIAL_CONSUMING,  // Indicates that at least one replica has reported that it has stopped consuming.
    HOLDING,          // the segment has started finalizing.
    COMMITTER_DECIDED, // We know who the committer will be, we will let them know next time they call segmentConsumed()
    COMMITTER_NOTIFIED, // we notified the committer to commit.
    COMMITTER_UPLOADING,  // committer is uploading.
    COMMITTING, // we are in the process of committing to zk
    COMMITTED,    // We already committed a segment.
    ABORTED,      // state machine is aborted. we will start a fresh one when the next segmentConsumed comes in.
  }

  private final HelixManager _helixManager;
  // A map that holds the FSM for each segment.
  private final Map<String, SegmentCompletionFSM> _fsmMap = new ConcurrentHashMap<>();
  private final Map<String, Long> _commitTimeMap = new ConcurrentHashMap<>();
  private final PinotLLCRealtimeSegmentManager _segmentManager;
  private final ControllerMetrics _controllerMetrics;
  private final LeadControllerManager _leadControllerManager;

  // Half hour max commit time for all segments
  private static final int MAX_COMMIT_TIME_FOR_ALL_SEGMENTS_SECONDS = 1800;

  public static int getMaxCommitTimeForAllSegmentsSeconds() {
    return MAX_COMMIT_TIME_FOR_ALL_SEGMENTS_SECONDS;
  }

  // TODO keep some history of past committed segments so that we can avoid looking up PROPERTYSTORE if some server
  //  comes in late.

  public SegmentCompletionManager(HelixManager helixManager, PinotLLCRealtimeSegmentManager segmentManager,
      ControllerMetrics controllerMetrics, LeadControllerManager leadControllerManager,
      int segmentCommitTimeoutSeconds) {
    _helixManager = helixManager;
    _segmentManager = segmentManager;
    _controllerMetrics = controllerMetrics;
    _leadControllerManager = leadControllerManager;
    SegmentCompletionProtocol.setMaxSegmentCommitTimeMs(
        TimeUnit.MILLISECONDS.convert(segmentCommitTimeoutSeconds, TimeUnit.SECONDS));
  }

  public String getControllerVipUrl() {
    return _segmentManager.getControllerVipUrl();
  }

  protected long getCurrentTimeMs() {
    return System.currentTimeMillis();
  }

  protected StreamPartitionMsgOffsetFactory getStreamPartitionMsgOffsetFactory(LLCSegmentName llcSegmentName) {
    String rawTableName = llcSegmentName.getTableName();
    TableConfig tableConfig = _segmentManager.getTableConfig(TableNameBuilder.REALTIME.tableNameWithType(rawTableName));
    StreamConfig streamConfig =
        new StreamConfig(tableConfig.getTableName(), IngestionConfigUtils.getStreamConfigMap(tableConfig));
    return StreamConsumerFactoryProvider.create(streamConfig).createStreamMsgOffsetFactory();
  }

  private SegmentCompletionFSM lookupOrCreateFsm(LLCSegmentName llcSegmentName, String msgType) {
    return _fsmMap.computeIfAbsent(llcSegmentName.getSegmentName(), k -> createFsm(llcSegmentName, msgType));
  }

  private SegmentCompletionFSM createFsm(LLCSegmentName llcSegmentName, String msgType) {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(llcSegmentName.getTableName());
    String segmentName = llcSegmentName.getSegmentName();
    SegmentZKMetadata segmentMetadata = _segmentManager.getSegmentZKMetadata(realtimeTableName, segmentName, null);
    Preconditions.checkState(segmentMetadata != null, "Failed to find ZK metadata for segment: %s", segmentName);
    SegmentCompletionFSM fsm;
    if (segmentMetadata.getStatus() == CommonConstants.Segment.Realtime.Status.DONE) {
      // Segment is already committed
      StreamPartitionMsgOffsetFactory factory = getStreamPartitionMsgOffsetFactory(llcSegmentName);
      StreamPartitionMsgOffset endOffset = factory.create(segmentMetadata.getEndOffset());
      fsm = SegmentCompletionFSM.fsmInCommit(_segmentManager, this, llcSegmentName, segmentMetadata.getNumReplicas(),
          endOffset);
    } else if (msgType.equals(SegmentCompletionProtocol.MSG_TYPE_STOPPED_CONSUMING)) {
      fsm = SegmentCompletionFSM.fsmStoppedConsuming(_segmentManager, this, llcSegmentName,
          segmentMetadata.getNumReplicas());
    } else {
      // Segment is in the process of completing, and this is the first one to respond. Create fsm
      fsm = SegmentCompletionFSM.fsmInHolding(_segmentManager, this, llcSegmentName, segmentMetadata.getNumReplicas());
    }
    LOGGER.info("Created FSM {}", fsm);
    return fsm;
  }

  /**
   * This method is to be called when a server calls in with the segmentConsumed() API, reporting an offset in the
   * stream
   * that it currently has (i.e. next offset that it will consume, if it continues to consume).
   */
  public SegmentCompletionProtocol.Response segmentConsumed(SegmentCompletionProtocol.Request.Params reqParams) {
    final String segmentNameStr = reqParams.getSegmentName();
    final LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
    final String tableName = segmentName.getTableName();
    if (!isLeader(tableName) || !_helixManager.isConnected()) {
      return SegmentCompletionProtocol.RESP_NOT_LEADER;
    }
    final String instanceId = reqParams.getInstanceId();
    final String stopReason = reqParams.getReason();
    final StreamPartitionMsgOffsetFactory factory = getStreamPartitionMsgOffsetFactory(segmentName);
    final StreamPartitionMsgOffset offset = factory.create(reqParams.getStreamPartitionMsgOffset());

    SegmentCompletionProtocol.Response response = SegmentCompletionProtocol.RESP_FAILED;
    SegmentCompletionFSM fsm = null;
    try {
      fsm = lookupOrCreateFsm(segmentName, SegmentCompletionProtocol.MSG_TYPE_CONSUMED);
      response = fsm.segmentConsumed(instanceId, offset, stopReason);
    } catch (Exception e) {
      LOGGER.error("Caught exception in segmentConsumed for segment {}", segmentNameStr, e);
    }
    if (fsm != null && fsm.isDone()) {
      LOGGER.info("Removing FSM (if present):{}", fsm.toString());
      _fsmMap.remove(segmentNameStr);
    }
    return response;
  }

  /**
   * This method is to be called when a server calls in with the segmentCommit() API. The server sends in the segment
   * along with the API, but it is the caller's responsibility to save the segment after this call (and before the
   * segmentCommitEnd() call).
   *
   * If successful, this method will return Response.COMMIT_CONTINUE, in which case, the caller should save the incoming
   * segment and then call segmentCommitEnd().
   *
   * Otherwise, this method will return a protocol response to be returned to the client right away (without saving the
   * incoming segment).
   */
  public SegmentCompletionProtocol.Response segmentCommitStart(
      final SegmentCompletionProtocol.Request.Params reqParams) {
    final String segmentNameStr = reqParams.getSegmentName();
    final LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
    final String tableName = segmentName.getTableName();
    if (!isLeader(tableName) || !_helixManager.isConnected()) {
      return SegmentCompletionProtocol.RESP_NOT_LEADER;
    }
    final String instanceId = reqParams.getInstanceId();
    final StreamPartitionMsgOffsetFactory factory = getStreamPartitionMsgOffsetFactory(segmentName);
    final StreamPartitionMsgOffset offset = factory.create(reqParams.getStreamPartitionMsgOffset());
    SegmentCompletionFSM fsm = null;
    SegmentCompletionProtocol.Response response = SegmentCompletionProtocol.RESP_FAILED;
    try {
      fsm = lookupOrCreateFsm(segmentName, SegmentCompletionProtocol.MSG_TYPE_COMMIT);
      response = fsm.segmentCommitStart(instanceId, offset);
    } catch (Exception e) {
      LOGGER.error("Caught exception in segmentCommitStart for segment {}", segmentNameStr, e);
    }
    if (fsm != null && fsm.isDone()) {
      LOGGER.info("Removing FSM (if present):{}", fsm.toString());
      _fsmMap.remove(segmentNameStr);
    }
    return response;
  }

  public SegmentCompletionProtocol.Response extendBuildTime(final SegmentCompletionProtocol.Request.Params reqParams) {
    final String segmentNameStr = reqParams.getSegmentName();
    final LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
    final String tableName = segmentName.getTableName();
    if (!isLeader(tableName) || !_helixManager.isConnected()) {
      return SegmentCompletionProtocol.RESP_NOT_LEADER;
    }
    final String instanceId = reqParams.getInstanceId();
    final StreamPartitionMsgOffsetFactory factory = getStreamPartitionMsgOffsetFactory(segmentName);
    final StreamPartitionMsgOffset offset = factory.create(reqParams.getStreamPartitionMsgOffset());
    final int extTimeSec = reqParams.getExtraTimeSec();
    SegmentCompletionFSM fsm = null;
    SegmentCompletionProtocol.Response response = SegmentCompletionProtocol.RESP_FAILED;
    try {
      fsm = lookupOrCreateFsm(segmentName, SegmentCompletionProtocol.MSG_TYPE_COMMIT);
      response = fsm.extendBuildTime(instanceId, offset, extTimeSec);
    } catch (Exception e) {
      LOGGER.error("Caught exception in extendBuildTime for segment {}", segmentNameStr, e);
    }
    if (fsm != null && fsm.isDone()) {
      LOGGER.info("Removing FSM (if present):{}", fsm.toString());
      _fsmMap.remove(segmentNameStr);
    }
    return response;
  }

  /**
   * This method is to be called when a server reports that it has stopped consuming a real-time segment.
   *
   * @return
   */
  public SegmentCompletionProtocol.Response segmentStoppedConsuming(
      SegmentCompletionProtocol.Request.Params reqParams) {
    final String segmentNameStr = reqParams.getSegmentName();
    final LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
    final String tableName = segmentName.getTableName();
    if (!isLeader(tableName) || !_helixManager.isConnected()) {
      return SegmentCompletionProtocol.RESP_NOT_LEADER;
    }
    final String instanceId = reqParams.getInstanceId();
    final StreamPartitionMsgOffsetFactory factory = getStreamPartitionMsgOffsetFactory(segmentName);
    final StreamPartitionMsgOffset offset = factory.create(reqParams.getStreamPartitionMsgOffset());
    final String reason = reqParams.getReason();
    SegmentCompletionFSM fsm = null;
    SegmentCompletionProtocol.Response response = SegmentCompletionProtocol.RESP_FAILED;
    try {
      fsm = lookupOrCreateFsm(segmentName, SegmentCompletionProtocol.MSG_TYPE_STOPPED_CONSUMING);
      response = fsm.stoppedConsuming(instanceId, offset, reason);
    } catch (Exception e) {
      LOGGER.error("Caught exception in segmentStoppedConsuming for segment {}", segmentNameStr, e);
    }
    if (fsm != null && fsm.isDone()) {
      LOGGER.info("Removing FSM (if present):{}", fsm.toString());
      _fsmMap.remove(segmentNameStr);
    }
    return response;
  }

  /**
   * This method is to be called when the segment sent in by the server has been saved locally in the correct path that
   * is downloadable by the servers.
   *
   * It returns a response code to be sent back to the client.
   *
   * If the repsonse code is not COMMIT_SUCCESS, then the caller may remove the segment that has been saved.
   *
   * @return
   */
  public SegmentCompletionProtocol.Response segmentCommitEnd(SegmentCompletionProtocol.Request.Params reqParams,
      boolean success, boolean isSplitCommit, CommittingSegmentDescriptor committingSegmentDescriptor) {
    final String segmentNameStr = reqParams.getSegmentName();
    final LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
    final String tableName = segmentName.getTableName();
    if (!isLeader(tableName) || !_helixManager.isConnected()) {
      return SegmentCompletionProtocol.RESP_NOT_LEADER;
    }
    SegmentCompletionFSM fsm = null;
    SegmentCompletionProtocol.Response response = SegmentCompletionProtocol.RESP_FAILED;
    try {
      fsm = lookupOrCreateFsm(segmentName, SegmentCompletionProtocol.MSG_TYPE_COMMIT);
      response = fsm.segmentCommitEnd(reqParams, success, isSplitCommit, committingSegmentDescriptor);
    } catch (Exception e) {
      LOGGER.error("Caught exception in segmentCommitEnd for segment {}", segmentNameStr, e);
    }
    if (fsm != null && fsm.isDone()) {
      LOGGER.info("Removing FSM (if present):{}", fsm.toString());
      _fsmMap.remove(segmentNameStr);
    }

    return response;
  }

  /**
   * This class implements the FSM on the controller side for each completing segment.
   *
   * An FSM is is created when we first hear about a segment (typically through the segmentConsumed message).
   * When an FSM is created, it may have one of two start states (HOLDING, or COMMITTED), depending on the
   * constructor used.
   *
   * We kick off an FSM in the COMMITTED state (rare) when we find that PROPERTYSTORE already has the segment
   * with the Status set to DONE.
   *
   * We kick off an FSM in the HOLDING state (typical) when a sementConsumed() message arrives from the
   * first server we hear from.
   *
   * The FSM does not have a timer. It is clocked by the servers, which, typically, are retransmitting their
   * segmentConsumed() message every so often (SegmentCompletionProtocol.MAX_HOLD_TIME_MS).
   *
   * See https://github.com/linkedin/pinot/wiki/Low-level-kafka-consumers
   */
  private static class SegmentCompletionFSM {
    // We will have some variation between hosts, so we add 10% to the max hold time to pick a winner.
    // If there is more than 10% variation, then it is handled as an error case (i.e. the first few to
    // come in will have a winner, and the later ones will just download the segment)
    private static final long MAX_TIME_TO_PICK_WINNER_MS =
        SegmentCompletionProtocol.MAX_HOLD_TIME_MS + (SegmentCompletionProtocol.MAX_HOLD_TIME_MS / 10);

    // Once we pick a winner, the winner may get notified in the next call, so add one hold time plus some.
    // It may be that the winner is not the server that we are currently processing a segmentConsumed()
    // message from. In that case, we will wait for the next segmetnConsumed() message from the picked winner.
    // If the winner does not come back to us within that time, we abort the state machine and start over.
    private static final long MAX_TIME_TO_NOTIFY_WINNER_MS =
        MAX_TIME_TO_PICK_WINNER_MS + SegmentCompletionProtocol.MAX_HOLD_TIME_MS + (
            SegmentCompletionProtocol.MAX_HOLD_TIME_MS / 10);

    public final Logger _logger;

    State _state = State.HOLDING;   // Typically start off in HOLDING state.
    final long _startTimeMs;
    private final LLCSegmentName _segmentName;
    private final String _rawTableName;
    private final String _realtimeTableName;
    private final int _numReplicas;
    private final Set<String> _excludedServerStateMap;
    private final Map<String, StreamPartitionMsgOffset> _commitStateMap;
    private final StreamPartitionMsgOffsetFactory _streamPartitionMsgOffsetFactory;
    private StreamPartitionMsgOffset _winningOffset = null;
    private String _winner;
    private final PinotLLCRealtimeSegmentManager _segmentManager;
    private final SegmentCompletionManager _segmentCompletionManager;
    private final long _maxTimeToPickWinnerMs;
    private final long _maxTimeToNotifyWinnerMs;
    private final long _initialCommitTimeMs;
    // Once the winner is notified, they are expected to commit right away. At this point, it is the segment build
    // time that we need to consider.
    // We may need to add some time here to allow for getting the lock? For now 0
    // We may need to add some time for the committer come back to us (after the build)? For now 0.
    private long _maxTimeAllowedToCommitMs;
    private final String _controllerVipUrl;

    public static SegmentCompletionFSM fsmInHolding(PinotLLCRealtimeSegmentManager segmentManager,
        SegmentCompletionManager segmentCompletionManager, LLCSegmentName segmentName, int numReplicas) {
      return new SegmentCompletionFSM(segmentManager, segmentCompletionManager, segmentName, numReplicas);
    }

    public static SegmentCompletionFSM fsmInCommit(PinotLLCRealtimeSegmentManager segmentManager,
        SegmentCompletionManager segmentCompletionManager, LLCSegmentName segmentName, int numReplicas,
        StreamPartitionMsgOffset winningOffset) {
      return new SegmentCompletionFSM(segmentManager, segmentCompletionManager, segmentName, numReplicas,
          winningOffset);
    }

    public static SegmentCompletionFSM fsmStoppedConsuming(PinotLLCRealtimeSegmentManager segmentManager,
        SegmentCompletionManager segmentCompletionManager, LLCSegmentName segmentName, int numReplicas) {
      SegmentCompletionFSM fsm =
          new SegmentCompletionFSM(segmentManager, segmentCompletionManager, segmentName, numReplicas);
      fsm._state = State.PARTIAL_CONSUMING;
      return fsm;
    }

    // Ctor that starts the FSM in HOLDING state
    private SegmentCompletionFSM(PinotLLCRealtimeSegmentManager segmentManager,
        SegmentCompletionManager segmentCompletionManager, LLCSegmentName segmentName, int numReplicas) {
      _segmentName = segmentName;
      _rawTableName = _segmentName.getTableName();
      _realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(_rawTableName);
      _numReplicas = numReplicas;
      _segmentManager = segmentManager;
      _commitStateMap = new HashMap<>(_numReplicas);
      _excludedServerStateMap = new HashSet<>(_numReplicas);
      _segmentCompletionManager = segmentCompletionManager;
      _startTimeMs = _segmentCompletionManager.getCurrentTimeMs();
      _maxTimeToPickWinnerMs = _startTimeMs + MAX_TIME_TO_PICK_WINNER_MS;
      _maxTimeToNotifyWinnerMs = _startTimeMs + MAX_TIME_TO_NOTIFY_WINNER_MS;
      _streamPartitionMsgOffsetFactory = _segmentCompletionManager.getStreamPartitionMsgOffsetFactory(_segmentName);
      long initialCommitTimeMs = MAX_TIME_TO_NOTIFY_WINNER_MS + _segmentManager.getCommitTimeoutMS(_realtimeTableName);
      Long savedCommitTime = _segmentCompletionManager._commitTimeMap.get(_rawTableName);
      if (savedCommitTime != null && savedCommitTime > initialCommitTimeMs) {
        initialCommitTimeMs = savedCommitTime;
      }
      _logger = LoggerFactory.getLogger("SegmentCompletionFSM_" + segmentName.getSegmentName());
      if (initialCommitTimeMs > MAX_COMMIT_TIME_FOR_ALL_SEGMENTS_SECONDS * 1000) {
        // The table has a really high value configured for max commit time. Set it to a higher value than default
        // and go from there.
        _logger
            .info("Configured max commit time {}s too high for table {}, changing to {}s", initialCommitTimeMs / 1000,
                _realtimeTableName, MAX_COMMIT_TIME_FOR_ALL_SEGMENTS_SECONDS);
        initialCommitTimeMs = MAX_COMMIT_TIME_FOR_ALL_SEGMENTS_SECONDS * 1000;
      }
      _initialCommitTimeMs = initialCommitTimeMs;
      _maxTimeAllowedToCommitMs = _startTimeMs + _initialCommitTimeMs;
      _controllerVipUrl = segmentCompletionManager.getControllerVipUrl();
    }

    // Ctor that starts the FSM in COMMITTED state
    private SegmentCompletionFSM(PinotLLCRealtimeSegmentManager segmentManager,
        SegmentCompletionManager segmentCompletionManager, LLCSegmentName segmentName, int numReplicas,
        StreamPartitionMsgOffset winningOffset) {
      // Constructor used when we get an event after a segment is committed.
      this(segmentManager, segmentCompletionManager, segmentName, numReplicas);
      _state = State.COMMITTED;
      _winningOffset = winningOffset;
      _winner = "UNKNOWN";
    }

    @Override
    public String toString() {
      return "{" + _segmentName.getSegmentName() + "," + _state + "," + _startTimeMs + "," + _winner + ","
          + _winningOffset + "," + _controllerVipUrl + "}";
    }

    // SegmentCompletionManager releases the FSM from the hashtable when it is done.
    public boolean isDone() {
      return _state.equals(State.COMMITTED) || _state.equals(State.ABORTED);
    }

    /*
     * We just heard from a server that it has reached completion stage, and is reporting the offset
     * that the server is at. Since multiple servers can come in at the same time for this segment,
     * we need to synchronize on the FSM to handle the messages. The processing time itself is small,
     * so we should be OK with this synchronization.
     */
    public SegmentCompletionProtocol.Response segmentConsumed(String instanceId, StreamPartitionMsgOffset offset,
        final String stopReason) {
      final long now = _segmentCompletionManager.getCurrentTimeMs();
      // We can synchronize the entire block for the SegmentConsumed message.
      synchronized (this) {
        _logger.info("Processing segmentConsumed({}, {})", instanceId, offset);
        if (_excludedServerStateMap.contains(instanceId)) {
          // Could be that the server was restarted, and it started consuming again, and somehow got to complete
          // consumption up to this point. We will accept it.
          _logger.info("Marking instance {} alive again", instanceId);
          _excludedServerStateMap.remove(instanceId);
        }
        _commitStateMap.put(instanceId, offset);
        switch (_state) {
          case PARTIAL_CONSUMING:
            return partialConsumingConsumed(instanceId, offset, now, stopReason);

          case HOLDING:
            return holdingConsumed(instanceId, offset, now, stopReason);

          case COMMITTER_DECIDED: // This must be a retransmit
            return committerDecidedConsumed(instanceId, offset, now);

          case COMMITTER_NOTIFIED:
            return committerNotifiedConsumed(instanceId, offset, now);

          case COMMITTER_UPLOADING:
            return committerUploadingConsumed(instanceId, offset, now);

          case COMMITTING:
            return committingConsumed(instanceId, offset, now);

          case COMMITTED:
            return committedConsumed(instanceId, offset);

          case ABORTED:
            // FSM has been aborted, just return HOLD
            return hold(instanceId, offset);

          default:
            return fail(instanceId, offset);
        }
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
    public SegmentCompletionProtocol.Response segmentCommitStart(String instanceId, StreamPartitionMsgOffset offset) {
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
            return committerNotifiedCommit(instanceId, offset, now);

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

    public SegmentCompletionProtocol.Response stoppedConsuming(String instanceId, StreamPartitionMsgOffset offset,
        String reason) {
      synchronized (this) {
        _logger.info("Processing stoppedConsuming({}, {})", instanceId, offset);
        _excludedServerStateMap.add(instanceId);
        switch (_state) {
          case PARTIAL_CONSUMING:
            return partialConsumingStoppedConsuming(instanceId, offset, reason);

          case HOLDING:
            return holdingStoppedConsuming(instanceId, offset, reason);

          case COMMITTER_DECIDED:
            return committerDecidedStoppedConsuming(instanceId, offset, reason);

          case COMMITTER_NOTIFIED:
            return committerNotifiedStoppedConsuming(instanceId, offset, reason);

          case COMMITTER_UPLOADING:
            return committerUploadingStoppedConsuming(instanceId, offset, reason);

          case COMMITTING:
            return committingStoppedConsuming(instanceId, offset, reason);

          case COMMITTED:
            return committedStoppedConsuming(instanceId, offset, reason);

          case ABORTED:
            _logger.info("Ignoring StoppedConsuming message from {} in state {}", instanceId, _state);
            return SegmentCompletionProtocol.RESP_PROCESSED;

          default:
            return fail(instanceId, offset);
        }
      }
    }

    public SegmentCompletionProtocol.Response extendBuildTime(final String instanceId,
        final StreamPartitionMsgOffset offset, final int extTimeSec) {
      final long now = _segmentCompletionManager.getCurrentTimeMs();
      synchronized (this) {
        _logger.info("Processing extendBuildTime({}, {}, {})", instanceId, offset, extTimeSec);
        switch (_state) {
          case PARTIAL_CONSUMING:
          case HOLDING:
          case COMMITTER_DECIDED:
            return fail(instanceId, offset);
          case COMMITTER_NOTIFIED:
            return committerNotifiedExtendBuildTime(instanceId, offset, extTimeSec, now);
          case COMMITTER_UPLOADING:
          case COMMITTING:
          case COMMITTED:
          case ABORTED:
          default:
            return fail(instanceId, offset);
        }
      }
    }

    /*
     * We can get this call only when the state is COMMITTER_UPLOADING. Also, the instanceId should be equal to
     * the _winner.
     */
    public SegmentCompletionProtocol.Response segmentCommitEnd(SegmentCompletionProtocol.Request.Params reqParams,
        boolean success, boolean isSplitCommit, CommittingSegmentDescriptor committingSegmentDescriptor) {
      String instanceId = reqParams.getInstanceId();
      StreamPartitionMsgOffset offset =
          _streamPartitionMsgOffsetFactory.create(reqParams.getStreamPartitionMsgOffset());
      synchronized (this) {
        if (_excludedServerStateMap.contains(instanceId)) {
          _logger.warn("Not accepting commitEnd from {} since it had stoppd consuming", instanceId);
          return abortAndReturnFailed();
        }
        _logger.info("Processing segmentCommitEnd({}, {})", instanceId, offset);
        if (!_state.equals(State.COMMITTER_UPLOADING) || !instanceId.equals(_winner)
            || offset.compareTo(_winningOffset) != 0) {
          // State changed while we were out of sync. return a failed commit.
          _logger.warn("State change during upload: state={} segment={} winner={} winningOffset={}", _state,
              _segmentName.getSegmentName(), _winner, _winningOffset);
          return abortAndReturnFailed();
        }
        if (!success) {
          _logger.error("Segment upload failed");
          return abortAndReturnFailed();
        }
        SegmentCompletionProtocol.Response response =
            commitSegment(reqParams, isSplitCommit, committingSegmentDescriptor);
        if (!response.equals(SegmentCompletionProtocol.RESP_COMMIT_SUCCESS)) {
          return abortAndReturnFailed();
        } else {
          return response;
        }
      }
    }

    // Helper methods that log the current state and the response sent
    private SegmentCompletionProtocol.Response fail(String instanceId, StreamPartitionMsgOffset offset) {
      _logger.info("{}:FAIL for instance={} offset={}", _state, instanceId, offset);
      return SegmentCompletionProtocol.RESP_FAILED;
    }

    private SegmentCompletionProtocol.Response commit(String instanceId, StreamPartitionMsgOffset offset) {
      long allowedBuildTimeSec = (_maxTimeAllowedToCommitMs - _startTimeMs) / 1000;
      _logger
          .info("{}:COMMIT for instance={} offset={} buldTimeSec={}", _state, instanceId, offset, allowedBuildTimeSec);
      SegmentCompletionProtocol.Response.Params params =
          new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(offset.toString())
              .withBuildTimeSeconds(allowedBuildTimeSec)
              .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT)
              .withControllerVipUrl(_controllerVipUrl);
      return new SegmentCompletionProtocol.Response(params);
    }

    private SegmentCompletionProtocol.Response discard(String instanceId, StreamPartitionMsgOffset offset) {
      _logger.warn("{}:DISCARD for instance={} offset={}", _state, instanceId, offset);
      return SegmentCompletionProtocol.RESP_DISCARD;
    }

    private SegmentCompletionProtocol.Response keep(String instanceId, StreamPartitionMsgOffset offset) {
      _logger.info("{}:KEEP for instance={} offset={}", _state, instanceId, offset);
      return new SegmentCompletionProtocol.Response(
          new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(offset.toString())
              .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.KEEP));
    }

    private SegmentCompletionProtocol.Response catchup(String instanceId, StreamPartitionMsgOffset offset) {
      _logger.info("{}:CATCHUP for instance={} offset={}", _state, instanceId, offset);
      return new SegmentCompletionProtocol.Response(
          new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(_winningOffset.toString())
              .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP));
    }

    private SegmentCompletionProtocol.Response hold(String instanceId, StreamPartitionMsgOffset offset) {
      _logger.info("{}:HOLD for instance={} offset={}", _state, instanceId, offset);
      return new SegmentCompletionProtocol.Response(new SegmentCompletionProtocol.Response.Params()
          .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.HOLD)
          .withStreamPartitionMsgOffset(offset.toString()));
    }

    private SegmentCompletionProtocol.Response abortAndReturnHold(long now, String instanceId,
        StreamPartitionMsgOffset offset) {
      _state = State.ABORTED;
      _segmentCompletionManager._controllerMetrics
          .addMeteredTableValue(_rawTableName, ControllerMeter.LLC_STATE_MACHINE_ABORTS, 1);
      return hold(instanceId, offset);
    }

    private SegmentCompletionProtocol.Response abortAndReturnFailed() {
      _state = State.ABORTED;
      _segmentCompletionManager._controllerMetrics
          .addMeteredTableValue(_rawTableName, ControllerMeter.LLC_STATE_MACHINE_ABORTS, 1);
      return SegmentCompletionProtocol.RESP_FAILED;
    }

    private SegmentCompletionProtocol.Response abortIfTooLateAndReturnHold(long now, String instanceId,
        StreamPartitionMsgOffset offset) {
      if (now > _maxTimeAllowedToCommitMs) {
        _logger
            .warn("{}:Aborting FSM (too late) instance={} offset={} now={} start={}", _state, instanceId, offset, now,
                _startTimeMs);
        return abortAndReturnHold(now, instanceId, offset);
      }
      return null;
    }

    private int numReplicasToLookFor() {
      return _numReplicas - _excludedServerStateMap.size();
    }

    private SegmentCompletionProtocol.Response partialConsumingConsumed(String instanceId,
        StreamPartitionMsgOffset offset, long now, final String stopReason) {
      // This is the first time we are getting segmentConsumed() for this segment.
      // Some instance thinks we can close this segment, so go to HOLDING state, and process as normal.
      // We will just be looking for less replicas.
      _state = State.HOLDING;
      return holdingConsumed(instanceId, offset, now, stopReason);
    }

    /*
     * This is not a good state to get a commit message, but it is possible that the controller failed while in
     * COMMITTER_NOTIFIED state, and the first message we got in the new controller was a stoppedConsuming
     * message. As long as the committer is not the one who stopped consuming (which we have already checked before
     * coming here), we will trust the server that this is a valid commit.
     */
    private SegmentCompletionProtocol.Response partialConsumingCommit(String instanceId,
        StreamPartitionMsgOffset offset, long now) {
      // Do the same as HOLDING__commit
      return processCommitWhileHoldingOrPartialConsuming(instanceId, offset, now);
    }

    private SegmentCompletionProtocol.Response partialConsumingStoppedConsuming(String instanceId,
        StreamPartitionMsgOffset offset, String reason) {
      return processStoppedConsuming(instanceId, offset, reason, true);
    }

    /*
     * If we have waited "enough", or we have all replicas reported, then we can pick a winner.
     *
     * Otherwise, we ask the server that is reporting to come back again later until one of these conditions hold.
     *
     * If we can pick a winner then we go to COMMITTER_DECIDED or COMMITTER_NOTIIFIED (if the instance
     * in this call is the same as winner).
     *
     * If we can go to COMMITTER_NOTIFIED then we respond with a COMMIT message, otherwise with a HOLD message.
     */
    private SegmentCompletionProtocol.Response holdingConsumed(String instanceId, StreamPartitionMsgOffset offset,
        long now, final String stopReason) {
      SegmentCompletionProtocol.Response response;
      // If we are past the max time to pick a winner, or we have heard from all replicas,
      // we are ready to pick a winner.
      if (isWinnerPicked(instanceId, now, stopReason)) {
        if (_winner.equals(instanceId)) {
          _logger.info("{}:Committer notified winner instance={} offset={}", _state, instanceId, offset);
          response = commit(instanceId, offset);
          _state = State.COMMITTER_NOTIFIED;
        } else {
          _logger.info("{}:Committer decided winner={} offset={}", _state, _winner, _winningOffset);
          response = catchup(instanceId, offset);
          _state = State.COMMITTER_DECIDED;
        }
      } else {
        response = hold(instanceId, offset);
      }
      return response;
    }

    /*
     * This not a good state to receive a commit message, but then it may be that the controller
     * failed over while in the COMMITTER_NOTIFIED state...
     */
    private SegmentCompletionProtocol.Response holdingCommit(String instanceId, StreamPartitionMsgOffset offset,
        long now) {
      return processCommitWhileHoldingOrPartialConsuming(instanceId, offset, now);
    }

    private SegmentCompletionProtocol.Response holdingStoppedConsuming(String instanceId,
        StreamPartitionMsgOffset offset, String reason) {
      return processStoppedConsuming(instanceId, offset, reason, true);
    }

    /*
     * We have already decided who the committer is, but have not let them know yet. If this is the committer that
     * we decided, then respond back with COMMIT. Otherwise, if the offset is smaller, respond back with a CATCHUP.
     * Otherwise, just have the server HOLD. Since the segment is not committed yet, we cannot ask them to KEEP or
     * DISCARD etc. If the committer fails for any reason, we will need a new committer.
     */
    private SegmentCompletionProtocol.Response committerDecidedConsumed(String instanceId,
        StreamPartitionMsgOffset offset, long now) {
      if (offset.compareTo(_winningOffset) > 0) {
        _logger.warn("{}:Aborting FSM (offset larger than winning) instance={} offset={} now={} winning={}", _state,
            instanceId, offset, now, _winningOffset);
        return abortAndReturnHold(now, instanceId, offset);
      }
      SegmentCompletionProtocol.Response response;
      if (_winner.equals(instanceId)) {
        if (_winningOffset.compareTo(offset) == 0) {
          _logger.info("{}:Notifying winner instance={} offset={}", _state, instanceId, offset);
          response = commit(instanceId, offset);
          _state = State.COMMITTER_NOTIFIED;
        } else {
          // Winner coming back with a different offset.
          _logger
              .warn("{}:Winner coming back with different offset for instance={} offset={} prevWinnOffset={}", _state,
                  instanceId, offset, _winningOffset);
          response = abortAndReturnHold(now, instanceId, offset);
        }
      } else if (offset.compareTo(_winningOffset) == 0) {
        // Wait until winner has posted the segment.
        response = hold(instanceId, offset);
      } else {
        response = catchup(instanceId, offset);
      }
      if (now > _maxTimeToNotifyWinnerMs) {
        // Winner never got back to us. Abort the completion protocol and start afresh.
        // We can potentially optimize here to see if this instance has the highest so far, and re-elect them to
        // be winner, but for now, we will abort it and restart
        response = abortAndReturnHold(now, instanceId, offset);
      }
      return response;
    }

    /*
     * We have already decided who the committer is, but have not let them know yet. So, we don't expect
     * a commit() call here.
     */
    private SegmentCompletionProtocol.Response committerDecidedCommit(String instanceId,
        StreamPartitionMsgOffset offset, long now) {
      return processCommitWhileHoldingOrPartialConsuming(instanceId, offset, now);
    }

    private SegmentCompletionProtocol.Response committerDecidedStoppedConsuming(String instanceId,
        StreamPartitionMsgOffset offset, String reason) {
      return processStoppedConsuming(instanceId, offset, reason, false);
    }

    /*
     * We have notified the committer. If we get a consumed message from another server, we can ask them to
     * catchup (if the offset is lower). If anything else, then we pretty much ask them to hold.
     */
    private SegmentCompletionProtocol.Response committerNotifiedConsumed(String instanceId,
        StreamPartitionMsgOffset offset, long now) {
      SegmentCompletionProtocol.Response response;
      // We have already picked a winner and notified them but we have not heard from them yet.
      // Common case here is that another server is coming back to us with its offset. We either respond back with
      // HOLD or CATCHUP.
      // If the winner is coming back again, then we have some more conditions to look at.
      response = abortIfTooLateAndReturnHold(now, instanceId, offset);
      if (response != null) {
        return response;
      }
      if (instanceId.equals(_winner)) {
        // Winner is coming back to after holding. Somehow they never heard us return COMMIT.
        // Allow them to be winner again, since we are still within time to pick a winner.
        if (offset.compareTo(_winningOffset) == 0) {
          response = commit(instanceId, offset);
        } else {
          // Something seriously wrong. Abort the FSM
          response = discard(instanceId, offset);
          _logger.warn("{}:Aborting for instance={} offset={}", _state, instanceId, offset);
          _state = State.ABORTED;
        }
      } else {
        // Common case: A different instance is reporting.
        if (offset.compareTo(_winningOffset) == 0) {
          // Wait until winner has posted the segment before asking this server to KEEP the segment.
          response = hold(instanceId, offset);
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

    /*
     * We have notified the committer. If we get a consumed message from another server, we can ask them to
     * catchup (if the offset is lower). If anything else, then we pretty much ask them to hold.
     */
    private SegmentCompletionProtocol.Response committerNotifiedCommit(String instanceId,
        StreamPartitionMsgOffset offset, long now) {
      SegmentCompletionProtocol.Response response = null;
      response = checkBadCommitRequest(instanceId, offset, now);
      if (response != null) {
        return response;
      }
      _logger.info("{}:Uploading for instance={} offset={}", _state, instanceId, offset);
      _state = State.COMMITTER_UPLOADING;
      long commitTimeMs = now - _startTimeMs;
      if (commitTimeMs > _initialCommitTimeMs) {
        // We assume that the commit time holds for all partitions. It is possible, though, that one partition
        // commits at a lower time than another partition, and the two partitions are going simultaneously,
        // and we may not get the maximum value all the time.
        _segmentCompletionManager._commitTimeMap.put(_segmentName.getTableName(), commitTimeMs);
      }
      return SegmentCompletionProtocol.RESP_COMMIT_CONTINUE;
    }

    private SegmentCompletionProtocol.Response committerNotifiedStoppedConsuming(String instanceId,
        StreamPartitionMsgOffset offset, String reason) {
      return processStoppedConsuming(instanceId, offset, reason, false);
    }

    private SegmentCompletionProtocol.Response committerNotifiedExtendBuildTime(String instanceId,
        StreamPartitionMsgOffset offset, int extTimeSec, long now) {
      SegmentCompletionProtocol.Response response = abortIfTooLateAndReturnHold(now, instanceId, offset);
      if (response == null) {
        long maxTimeAllowedToCommitMs = now + extTimeSec * 1000;
        if (maxTimeAllowedToCommitMs > _startTimeMs + MAX_COMMIT_TIME_FOR_ALL_SEGMENTS_SECONDS * 1000) {
          _logger.warn("Not accepting lease extension from {} startTime={} requestedTime={}", instanceId, _startTimeMs,
              maxTimeAllowedToCommitMs);
          return abortAndReturnFailed();
        }
        _maxTimeAllowedToCommitMs = maxTimeAllowedToCommitMs;
        response = SegmentCompletionProtocol.RESP_PROCESSED;
      }
      return response;
    }

    private SegmentCompletionProtocol.Response committerUploadingConsumed(String instanceId,
        StreamPartitionMsgOffset offset, long now) {
      return processConsumedAfterCommitStart(instanceId, offset, now);
    }

    private SegmentCompletionProtocol.Response committerUploadingCommit(String instanceId,
        StreamPartitionMsgOffset offset, long now) {
      return processCommitWhileUploading(instanceId, offset, now);
    }

    private SegmentCompletionProtocol.Response committerUploadingStoppedConsuming(String instanceId,
        StreamPartitionMsgOffset offset, String reason) {
      return processStoppedConsuming(instanceId, offset, reason, false);
    }

    private SegmentCompletionProtocol.Response committingConsumed(String instanceId, StreamPartitionMsgOffset offset,
        long now) {
      return processConsumedAfterCommitStart(instanceId, offset, now);
    }

    private SegmentCompletionProtocol.Response committingCommit(String instanceId, StreamPartitionMsgOffset offset,
        long now) {
      return processCommitWhileUploading(instanceId, offset, now);
    }

    private SegmentCompletionProtocol.Response committingStoppedConsuming(String instanceId,
        StreamPartitionMsgOffset offset, String reason) {
      return processStoppedConsuming(instanceId, offset, reason, false);
    }

    private SegmentCompletionProtocol.Response committedConsumed(String instanceId, StreamPartitionMsgOffset offset) {
      // Server reporting an offset on an already completed segment. Depending on the offset, either KEEP or DISCARD.
      SegmentCompletionProtocol.Response response;
      if (offset.compareTo(_winningOffset) == 0) {
        response = keep(instanceId, offset);
      } else {
        // Return DISCARD. It is hard to say how long the server will take to complete things.
        response = discard(instanceId, offset);
      }
      return response;
    }

    private SegmentCompletionProtocol.Response committedCommit(String instanceId, StreamPartitionMsgOffset offset) {
      if (offset.compareTo(_winningOffset) == 0) {
        return keep(instanceId, offset);
      }
      return discard(instanceId, offset);
    }

    private SegmentCompletionProtocol.Response committedStoppedConsuming(String instanceId,
        StreamPartitionMsgOffset offset, String reason) {
      return processStoppedConsuming(instanceId, offset, reason, false);
    }

    private SegmentCompletionProtocol.Response processStoppedConsuming(String instanceId,
        StreamPartitionMsgOffset offset, String reason, boolean createNew) {
      _logger
          .info("Instance {} stopped consuming segment {} at offset {}, state {}, createNew: {}, reason:{}", instanceId,
              _segmentName, offset, _state, createNew, reason);
      try {
        _segmentManager.segmentStoppedConsuming(_segmentName, instanceId);
      } catch (Exception e) {
        _logger.error("Caught exception while processing stopped CONSUMING segment: {} on instance: {}",
            _segmentName.getSegmentName(), instanceId, e);
        return SegmentCompletionProtocol.RESP_FAILED;
      }
      return SegmentCompletionProtocol.RESP_PROCESSED;
    }

    // A common method when the state is > COMMITTER_NOTIFIED.
    private SegmentCompletionProtocol.Response processConsumedAfterCommitStart(String instanceId,
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
        return null;
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
          // Wait until winner has posted the segment before asking this server to KEEP the segment.
          response = hold(instanceId, offset);
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

    private SegmentCompletionProtocol.Response commitSegment(SegmentCompletionProtocol.Request.Params reqParams,
        boolean isSplitCommit, CommittingSegmentDescriptor committingSegmentDescriptor) {
      String instanceId = reqParams.getInstanceId();
      StreamPartitionMsgOffset offset =
          _streamPartitionMsgOffsetFactory.create(reqParams.getStreamPartitionMsgOffset());
      if (!_state.equals(State.COMMITTER_UPLOADING)) {
        // State changed while we were out of sync. return a failed commit.
        _logger.warn("State change during upload: state={} segment={} winner={} winningOffset={}", _state,
            _segmentName.getSegmentName(), _winner, _winningOffset);
        return SegmentCompletionProtocol.RESP_FAILED;
      }
      _logger.info("Committing segment {} at offset {} winner {}", _segmentName.getSegmentName(), offset, instanceId);
      _state = State.COMMITTING;
      // In case of splitCommit, the segment is uploaded to a unique file name indicated by segmentLocation,
      // so we need to move the segment file to its permanent location first before committing the metadata.
      // The committingSegmentDescriptor is then updated with the permanent segment location to be saved in metadata
      // store.
      if (isSplitCommit) {
        try {
          _segmentManager.commitSegmentFile(_realtimeTableName, committingSegmentDescriptor);
        } catch (Exception e) {
          _logger.error("Caught exception while committing segment file for segment: {}", _segmentName.getSegmentName(),
              e);
          return SegmentCompletionProtocol.RESP_FAILED;
        }
      }
      try {
        // Convert to a controller uri if the segment location uses local file scheme.
        if (CommonConstants.Segment.LOCAL_SEGMENT_SCHEME
            .equalsIgnoreCase(URIUtils.getUri(committingSegmentDescriptor.getSegmentLocation()).getScheme())) {
          committingSegmentDescriptor.setSegmentLocation(URIUtils
              .constructDownloadUrl(_controllerVipUrl, TableNameBuilder.extractRawTableName(_realtimeTableName),
                  _segmentName.getSegmentName()));
        }
        _segmentManager.commitSegmentMetadata(_realtimeTableName, committingSegmentDescriptor);
      } catch (Exception e) {
        _logger
            .error("Caught exception while committing segment metadata for segment: {}", _segmentName.getSegmentName(),
                e);
        return SegmentCompletionProtocol.RESP_FAILED;
      }

      _state = State.COMMITTED;
      _logger.info("Committed segment {} at offset {} winner {}", _segmentName.getSegmentName(), offset, instanceId);
      return SegmentCompletionProtocol.RESP_COMMIT_SUCCESS;
    }

    private SegmentCompletionProtocol.Response processCommitWhileUploading(String instanceId,
        StreamPartitionMsgOffset offset, long now) {
      _logger.info("Processing segmentCommit({}, {})", instanceId, offset);
      SegmentCompletionProtocol.Response response = abortIfTooLateAndReturnHold(now, instanceId, offset);
      if (response != null) {
        return response;
      }
      // Another committer (or same) came in while one was uploading. Ask them to hold in case this one fails.
      return new SegmentCompletionProtocol.Response(
          new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(offset.toString())
              .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.HOLD));
    }

    private SegmentCompletionProtocol.Response checkBadCommitRequest(String instanceId, StreamPartitionMsgOffset offset,
        long now) {
      SegmentCompletionProtocol.Response response = abortIfTooLateAndReturnHold(now, instanceId, offset);
      if (response != null) {
        return response;
      } else if (instanceId.equals(_winner) && offset.compareTo(_winningOffset) != 0) {
        // Hmm. Committer has been notified, but either a different one is committing, or offset is different
        _logger.warn("{}:Aborting FSM (bad commit req) instance={} offset={} now={} winning={}", _state, instanceId,
            offset, now, _winningOffset);
        return abortAndReturnHold(now, instanceId, offset);
      }
      return null;
    }

    private SegmentCompletionProtocol.Response processCommitWhileHoldingOrPartialConsuming(String instanceId,
        StreamPartitionMsgOffset offset, long now) {
      _logger.info("Processing segmentCommit({}, {})", instanceId, offset);
      SegmentCompletionProtocol.Response response = abortIfTooLateAndReturnHold(now, instanceId, offset);
      if (response != null) {
        return response;
      }
      // We cannot get a commit if we are in this state, so ask them to hold. Maybe we are starting after a failover.
      // The server will re-send the segmentConsumed message.
      return hold(instanceId, offset);
    }

    /**
     * Pick a winner if we can, preferring the instance that we are handling right now,
     *
     * We accept the first server to report an offset as long as the server stopped consumption
     * due to row limit. The premise is that other servers will also stop at row limit, and there
     * is no need to wait for them to report an offset in order to decide on a winner. The state machine takes care
     * of the cases where other servers may report different offsets (just in case).
     *
     * If the above condition is not satisfied (i.e. either this is not the first server, or it did not reach
     * row limit), then we can pick a winner only if it is too late to pick a winner, or we have heard from all
     * servers.
     *
     * Otherwise, we wait to hear from more servers.
     *
     * @param preferredInstance The instance that is reporting in this thread.
     * @param now current time
     * @param stopReason reason reported by instance for stopping consumption.
     * @return true if winner picked, false otherwise.
     */
    private boolean isWinnerPicked(String preferredInstance, long now, final String stopReason) {
      if ((SegmentCompletionProtocol.REASON_ROW_LIMIT.equals(stopReason)
          || SegmentCompletionProtocol.REASON_END_OF_PARTITION_GROUP.equals(stopReason))
          && _commitStateMap.size() == 1) {
        _winner = preferredInstance;
        _winningOffset = _commitStateMap.get(preferredInstance);
        return true;
      } else if (now > _maxTimeToPickWinnerMs || _commitStateMap.size() == numReplicasToLookFor()) {
        _logger.info("{}:Picking winner time={} size={}", _state, now - _startTimeMs, _commitStateMap.size());
        StreamPartitionMsgOffset maxOffsetSoFar = null;
        String winnerSoFar = null;
        for (Map.Entry<String, StreamPartitionMsgOffset> entry : _commitStateMap.entrySet()) {
          if (maxOffsetSoFar == null || entry.getValue().compareTo(maxOffsetSoFar) > 0) {
            maxOffsetSoFar = entry.getValue();
            winnerSoFar = entry.getKey();
          }
        }
        _winningOffset = maxOffsetSoFar;
        if (_commitStateMap.get(preferredInstance).compareTo(maxOffsetSoFar) == 0) {
          winnerSoFar = preferredInstance;
        }
        _winner = winnerSoFar;
        return true;
      }
      return false;
    }
  }

  @VisibleForTesting
  protected boolean isLeader(String tableName) {
    return _leadControllerManager.isLeaderForTable(tableName);
  }
}
