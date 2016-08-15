/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.controller.helix.core.realtime;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;


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
  public static Logger LOGGER = LoggerFactory.getLogger(SegmentCompletionManager.class);
  private enum State {
    HOLDING,          // the segment has started finalizing.
    COMMITTER_DECIDED, // We know who the committer will be, we will let them know next time they call segmentConsumed()
    COMMITTER_NOTIFIED, // we notified the committer to commit.
    COMMITTER_UPLOADING,  // committer is uploading.
    COMMITTING, // we are in the process of committing to zk
    COMMITTED,    // We already committed a segment.
    ABORTED,      // state machine is aborted. we will start a fresh one when the next segmentConsumed comes in.
  }

  private static SegmentCompletionManager _instance = null;

  private final HelixManager _helixManager;
  // A map that holds the FSM for each segment.
  private final Map<String, SegmentCompletionFSM> _fsmMap = new ConcurrentHashMap<>();
  private final PinotLLCRealtimeSegmentManager _segmentManager;

  // TODO keep some history of past committed segments so that we can avoid looking up PROPERTYSTORE if some server comes in late.

  protected SegmentCompletionManager(HelixManager helixManager, PinotLLCRealtimeSegmentManager segmentManager) {
    _helixManager = helixManager;
    _segmentManager = segmentManager;
  }

  public static SegmentCompletionManager create(HelixManager helixManager, PinotLLCRealtimeSegmentManager segmentManager) {
    if (_instance != null) {
      throw new RuntimeException("Cannot create multiple instances");
    }
    _instance = new SegmentCompletionManager(helixManager, segmentManager);
    return _instance;
  }

  public static SegmentCompletionManager getInstance() {
    if (_instance == null) {
      throw new RuntimeException("Not yet created");
    }
    return _instance;
  }

  protected long getCurrentTimeMs() {
    return System.currentTimeMillis();
  }

  // We need to make sure that we never create multiple FSMs for the same segment, so this method must be synchronized.
  private synchronized SegmentCompletionFSM lookupOrCreateFsm(final LLCSegmentName segmentName, String msgType,
      long offset) {
    final String segmentNameStr = segmentName.getSegmentName();
    SegmentCompletionFSM fsm = _fsmMap.get(segmentNameStr);
    if (fsm == null) {
      // Look up propertystore to see if this is a completed segment
      ZNRecord segment;
      try {
        // TODO if we keep a list of last few committed segments, we don't need to go to zk for this.
        final String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(segmentName.getTableName());
        LLCRealtimeSegmentZKMetadata segmentMetadata = _segmentManager.getRealtimeSegmentZKMetadata(
            realtimeTableName, segmentName.getSegmentName());
        if (segmentMetadata.getStatus().equals(CommonConstants.Segment.Realtime.Status.DONE)) {
          // Best to go through the state machine for this case as well, so that all code regarding state handling is in one place
          // Also good for synchronization, because it is possible that multiple threads take this path, and we don't want
          // multiple instances of the FSM to be created for the same commit sequence at the same time.
          final long endOffset = segmentMetadata.getEndOffset();
          fsm = new SegmentCompletionFSM(_segmentManager, this, segmentName, segmentMetadata.getNumReplicas(), endOffset);
        } else {
          // Segment is finalizing, and this is the first one to respond. Create an entry
          fsm = new SegmentCompletionFSM(_segmentManager, this, segmentName, segmentMetadata.getNumReplicas());
        }
        LOGGER.info("Created FSM {}", fsm);
        _fsmMap.put(segmentNameStr, fsm);
      } catch (Exception e) {
        // Server gone wonky. Segment does not exist in propstore
        LOGGER.error("Exception reading segment read from propertystore {}", segmentNameStr, e);
        throw new RuntimeException("Segment read from propertystore " + segmentNameStr, e);
      }
    }
    return fsm;
  }

  /**
   * This method is to be called when a server calls in with the segmentConsumed() API, reporting an offset in kafka
   * that it currently has (i.e. next offset that it will consume, if it continues to consume).
   *
   * @param segmentNameStr Name of the LLC segment
   * @param instanceId Instance that sent the segmentConsumed() request
   * @param offset Kafka offset reported by the instance
   * @return the protocol repsonse to be returned to the server.
   */
  public SegmentCompletionProtocol.Response segmentConsumed(final String segmentNameStr, final String instanceId, final long offset) {
    if (!_helixManager.isLeader()) {
      return SegmentCompletionProtocol.RESP_NOT_LEADER;
    }
    LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
    SegmentCompletionFSM fsm = lookupOrCreateFsm(segmentName, SegmentCompletionProtocol.MSG_TYPE_CONSUMED, offset);
    SegmentCompletionProtocol.Response response = fsm.segmentConsumed(instanceId, offset);
    if (fsm.isDone()) {
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
   *
   * @param segmentNameStr  Name of the LLC segment
   * @param instanceId  Instance that sent the segmentCommit() request
   * @param offset  Kafka offset reported by the instance.
   * @return
   */
  public SegmentCompletionProtocol.Response segmentCommitStart(final String segmentNameStr, final String instanceId, final long offset) {
    if (!_helixManager.isLeader()) {
      return SegmentCompletionProtocol.RESP_NOT_LEADER;
    }
    LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
    SegmentCompletionFSM fsm = lookupOrCreateFsm(segmentName, SegmentCompletionProtocol.MSG_TYPE_CONSUMED, offset);
    SegmentCompletionProtocol.Response response = fsm.segmentCommitStart(instanceId, offset);
    if (fsm.isDone()) {
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
   * @param segmentNameStr  Name of the LLC segment
   * @param instanceId Instance that sent the segmentConsumed() request.
   * @param offset Kafka offset reported by the client.
   * @param success whether saving the segment was successful or not.
   * @return
   */
  public SegmentCompletionProtocol.Response segmentCommitEnd(final String segmentNameStr, final String instanceId,
      final long offset, boolean success) {
    if (!_helixManager.isLeader()) {
      return SegmentCompletionProtocol.RESP_NOT_LEADER;
    }
    LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
    SegmentCompletionFSM fsm = lookupOrCreateFsm(segmentName, SegmentCompletionProtocol.MSG_TYPE_CONSUMED, offset);
    SegmentCompletionProtocol.Response response = fsm.segmentCommitEnd(instanceId, offset, success);
    if (fsm.isDone()) {
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
    public static final long  MAX_TIME_TO_PICK_WINNER_MS =
      SegmentCompletionProtocol.MAX_HOLD_TIME_MS + (SegmentCompletionProtocol.MAX_HOLD_TIME_MS / 10);

    // Once we pick a winner, the winner may get notified in the next call, so add one hold time plus some.
    public static final long MAX_TIME_TO_NOTIFY_WINNER_MS = MAX_TIME_TO_PICK_WINNER_MS +
      SegmentCompletionProtocol.MAX_HOLD_TIME_MS + (SegmentCompletionProtocol.MAX_HOLD_TIME_MS / 10);

    // Once the winner is notified, the are expected to commit right away. At this point, it is the segment commit
    // time that we need to consider.
    // We may need to add some time here to allow for getting the lock? For now 0
    // We may need to add some time for the committer come back to us? For now 0.
    public static final long MAX_TIME_ALLOWED_TO_COMMIT_MS = MAX_TIME_TO_NOTIFY_WINNER_MS + SegmentCompletionProtocol.MAX_SEGMENT_COMMIT_TIME_MS;

    public final Logger LOGGER;

    State _state = State.HOLDING;   // Typically start off in HOLDING state.
    final long _startTime;
    private final LLCSegmentName _segmentName;
    private final int _numReplicas;
    private final Map<String, Long> _commitStateMap;
    private long _winningOffset = -1L;
    private String _winner;
    private final PinotLLCRealtimeSegmentManager _segmentManager;
    private final SegmentCompletionManager _segmentCompletionManager;

    // Ctor that starts the FSM in HOLDING state
    public SegmentCompletionFSM(PinotLLCRealtimeSegmentManager segmentManager,
        SegmentCompletionManager segmentCompletionManager, LLCSegmentName segmentName, int numReplicas) {
      _segmentName = segmentName;
      _numReplicas = numReplicas;
      _segmentManager = segmentManager;
      _commitStateMap = new HashMap<>(_numReplicas);
      _segmentCompletionManager = segmentCompletionManager;
      _startTime = _segmentCompletionManager.getCurrentTimeMs();
      LOGGER = LoggerFactory.getLogger("SegmentFinalizerFSM_"  + segmentName.getSegmentName());
    }

    // Ctor that starts the FSM in COMMITTED state
    public SegmentCompletionFSM(PinotLLCRealtimeSegmentManager segmentManager,
        SegmentCompletionManager segmentCompletionManager, LLCSegmentName segmentName, int numReplicas,
        long winningOffset) {
      // Constructor used when we get an event after a segment is committed.
      this(segmentManager, segmentCompletionManager, segmentName, numReplicas);
      _state = State.COMMITTED;
      _winningOffset = winningOffset;
      _winner = "UNKNOWN";
    }

    @Override
    public String toString() {
      return "{" + _segmentName.getSegmentName() + "," + _state + "," + _startTime + "," + _winner + "," + _winningOffset + "}";
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
    public SegmentCompletionProtocol.Response segmentConsumed(String instanceId, long offset) {
      final long now = _segmentCompletionManager.getCurrentTimeMs();
      // We can synchronize the entire block for the SegmentConsumed message.
      synchronized (this) {
        LOGGER.info("Processing segmentConsumed({}, {})", instanceId, offset);
        _commitStateMap.put(instanceId, offset);
        switch (_state) {
          case HOLDING:
            return HOLDING__consumed(instanceId, offset, now);

          case COMMITTER_DECIDED: // This must be a retransmit
            return COMMITTER_DECIDED__consumed(instanceId, offset, now);

          case COMMITTER_NOTIFIED:
            return COMMITTER_NOTIFIED__consumed(instanceId, offset, now);

          case COMMITTER_UPLOADING:
            return COMMITTER_UPLOADING__consumed(instanceId, offset, now);

          case COMMITTING:
            return COMMITTING__consumed(instanceId, offset, now);

          case COMMITTED:
            return COMMITTED__consumed(instanceId, offset);

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
     * In case of descrepancy we move the state machine to ABORTED state so that this FSM is removed
     * from the map, and things start over. In this case, we respond to the server with a 'hold' so
     * that they re-transmit their segmentConsumed() message and start over.
     */
    public SegmentCompletionProtocol.Response segmentCommitStart(String instanceId, long offset) {
      long now = _segmentCompletionManager.getCurrentTimeMs();
      synchronized (this) {
        LOGGER.info("Processing segmentCommit({}, {})", instanceId, offset);
        switch (_state) {
          case HOLDING:
            return HOLDING__commit(instanceId, offset, now);

          case COMMITTER_DECIDED:
            return COMMITTER_DECIDED__commit(instanceId, offset, now);

          case COMMITTER_NOTIFIED:
            return COMMITTER_NOTIFIED__commit(instanceId, offset, now);

          case COMMITTER_UPLOADING:
            return COMMITTER_UPLOADING__commit(instanceId, offset, now);

          case COMMITTING:
            return COMMITTING__commit(instanceId, offset, now);

          case COMMITTED:
            return COMMITTED__commit(instanceId, offset);

          case ABORTED:
            return hold(instanceId, offset);

          default:
            return fail(instanceId, offset);
        }
      }
    }

    /*
     * We can get this call only when the state is COMMITTER_UPLOADING. Also, the instanceId should be equal to
     * the _winner.
     */
    public SegmentCompletionProtocol.Response segmentCommitEnd(String instanceId, long offset, boolean success) {
      synchronized (this) {
        LOGGER.info("Processing segmentCommit({}, {})", instanceId, offset);
        if (!_state.equals(State.COMMITTER_UPLOADING) || !instanceId.equals(_winner)) {
          // State changed while we were out of sync. return a failed commit.
          LOGGER.warn("State change during upload: state={} segment={} winner={} winningOffset={}",
              _state, _segmentName.getSegmentName(), _winner, _winningOffset);
          _state = State.ABORTED;
          return SegmentCompletionProtocol.RESP_FAILED;
        }
        if (!success) {
          LOGGER.error("Segment upload failed");
          _state = State.ABORTED;
          return SegmentCompletionProtocol.RESP_FAILED;
        }
        SegmentCompletionProtocol.Response response = updateZk(instanceId, offset);
        if (response != null) {
          return response;
        }
      }
      return new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.FAILED, -1L);
    }


    // Helper methods that log the current state and the response sent
    private SegmentCompletionProtocol.Response fail(String instanceId, long offset) {
      LOGGER.info("{}:FAIL for instance={} offset={}", _state, instanceId, offset);
      return SegmentCompletionProtocol.RESP_FAILED;
    }

    private SegmentCompletionProtocol.Response commit(String instanceId, long offset) {
      LOGGER.info("{}:COMMIT for instance={} offset={}", _state, instanceId, offset);
      return new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT, offset);
    }

    private SegmentCompletionProtocol.Response discard(String instanceId, long offset) {
      LOGGER.warn("{}:DISCARD for instance={} offset={}", _state, instanceId, offset);
      return SegmentCompletionProtocol.RESP_DISCARD;
    }

    private SegmentCompletionProtocol.Response keep(String instanceId, long offset) {
      LOGGER.info("{}:KEEP for instance={} offset={}", _state, instanceId, offset);
      return new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.KEEP, offset);
    }

    private SegmentCompletionProtocol.Response catchup(String instanceId, long offset) {
      LOGGER.info("{}:CATCHUP for instance={} offset={}", _state, instanceId, offset);
      return new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP,
          _winningOffset);
    }

    private SegmentCompletionProtocol.Response hold(String instanceId, long offset) {
      LOGGER.info("{}:HOLD for instance={} offset={}", _state, instanceId, offset);
      return new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.HOLD, offset);
    }

    private SegmentCompletionProtocol.Response abortAndReturnHold(long now, String instanceId, long offset) {
      _state = State.ABORTED;
      return hold(instanceId, offset);
    }

    private SegmentCompletionProtocol.Response abortIfTooLateAndReturnHold(long now, String instanceId, long offset) {
      if (now > _startTime + MAX_TIME_ALLOWED_TO_COMMIT_MS) {
        LOGGER.warn("{}:Aborting FSM (too late) instance={} offset={} now={} start={}", _state, instanceId,
            offset, now, _startTime);
        return abortAndReturnHold(now, instanceId, offset);
      }
      return null;
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
    private SegmentCompletionProtocol.Response HOLDING__consumed(String instanceId, long offset, long now) {
      SegmentCompletionProtocol.Response response;
      // If we are past the max time to pick a winner, or we have heard from all replicas,
      // we are ready to pick a winner.
      if (now > _startTime + MAX_TIME_TO_PICK_WINNER_MS || _commitStateMap.size() == _numReplicas) {
        LOGGER.info("{}:Picking winner time={} size={}", _state, now-_startTime, _commitStateMap.size());
        pickWinner(instanceId);
        if (_winner.equals(instanceId)) {
          LOGGER.info("{}:Committer notified winner instance={} offset={}", _state, instanceId, offset);
          response = commit(instanceId, offset);
          _state = State.COMMITTER_NOTIFIED;
        } else {
          LOGGER.info("{}:Committer decided winner={} offset={}", _state, _winner, _winningOffset);
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
    private SegmentCompletionProtocol.Response HOLDING__commit(String instanceId, long offset, long now) {
      return processCommitWhileHolding(instanceId, offset, now);
    }

    /*
     * We have already decided who the committer is, but have not let them know yet. If this is the committer that
     * we decided, then respond back with COMMIT. Otherwise, if the offset is smaller, respond back with a CATCHUP.
     * Otherwise, just have the server HOLD. Since the segment is not committed yet, we cannot ask them to KEEP or
     * DISCARD etc. If the committer fails for any reason, we will need a new committer.
     */
    private SegmentCompletionProtocol.Response COMMITTER_DECIDED__consumed(String instanceId, long offset, long now) {
      if (offset > _winningOffset) {
        LOGGER.warn("{}:Aborting FSM (offset larger than winning) instance={} offset={} now={} winning={}", _state, instanceId,
            offset, now, _winningOffset);
        return abortAndReturnHold(now, instanceId, offset);
      }
      SegmentCompletionProtocol.Response response;
      if (_winner.equals(instanceId)) {
        if (_winningOffset == offset) {
          LOGGER.info("{}:Notifying winner instance={} offset={}", _state, instanceId, offset);
          response = commit(instanceId, offset);
          _state = State.COMMITTER_NOTIFIED;
        } else {
          // Winner coming back with a different offset.
          LOGGER.warn("{}:Winner coming back with different offset for instance={} offset={} prevWinnOffset={}", _state,
              instanceId, offset, _winningOffset);
          response = abortAndReturnHold(now, instanceId, offset);
        }
      } else  if (offset == _winningOffset) {
        // Wait until winner has posted the segment.
        response = hold(instanceId, offset);
      } else {
        response = catchup(instanceId, offset);
      }
      if (now > _startTime + MAX_TIME_TO_NOTIFY_WINNER_MS) {
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
    private SegmentCompletionProtocol.Response COMMITTER_DECIDED__commit(String instanceId, long offset,
        long now) {
      return processCommitWhileHolding(instanceId, offset, now);
    }

    /*
     * We have notified the committer. If we get a consumed message from another server, we can ask them to 
     * catchup (if the offset is lower). If anything else, then we pretty much ask them to hold.
     */
    private SegmentCompletionProtocol.Response COMMITTER_NOTIFIED__consumed(String instanceId, long offset, long now) {
      SegmentCompletionProtocol.Response response;
      // We have already picked a winner and notified them but we have not heard from them yet.
      // Common case here is that another server is coming back to us with its offset. We either respond back with HOLD or CATCHUP.
      // If the winner is coming back again, then we have some more conditions to look at.
      response = abortIfTooLateAndReturnHold(now, instanceId, offset);
      if (response != null) {
        return response;
      }
      if (instanceId.equals(_winner)) {
        // Winner is coming back to after holding. Somehow they never heard us return COMMIT.
        // Allow them to be winner again, since we are still within time to pick a winner.
        if (offset == _winningOffset) {
          response = commit(instanceId, offset);
        } else {
          // Something seriously wrong. Abort the FSM
          response = discard(instanceId, offset);
          LOGGER.warn("{}:Aborting for instance={} offset={}", _state, instanceId, offset);
          _state = State.ABORTED;
        }
      } else {
        // Common case: A different instance is reporting.
        if (offset == _winningOffset) {
          // Wait until winner has posted the segment before asking this server to KEEP the segment.
          response = hold(instanceId, offset);
        } else if (offset < _winningOffset) {
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
    private SegmentCompletionProtocol.Response COMMITTER_NOTIFIED__commit(String instanceId, long offset, long now) {
      SegmentCompletionProtocol.Response response = null;
      response = checkBadCommitRequest(instanceId, offset, now);
      if (response != null) {
        return response;
      }
      LOGGER.info("{}:Uploading for instance={} offset={}", _state, instanceId, offset);
      _state = State.COMMITTER_UPLOADING;
      return SegmentCompletionProtocol.RESP_COMMIT_CONTINUE;
    }

    private SegmentCompletionProtocol.Response COMMITTER_UPLOADING__consumed(String instanceId, long offset, long now) {
      return processConsumedAfterCommitStart(instanceId, offset, now);
    }

    private SegmentCompletionProtocol.Response COMMITTER_UPLOADING__commit(String instanceId, long offset,
        long now) {
      return processCommitWhileUploading(instanceId, offset, now);
    }

    private SegmentCompletionProtocol.Response COMMITTING__consumed(String instanceId, long offset, long now) {
      return processConsumedAfterCommitStart(instanceId, offset, now);
    }

    private SegmentCompletionProtocol.Response COMMITTING__commit(String instanceId, long offset,
        long now) {
      return processCommitWhileUploading(instanceId, offset, now);
    }

    private SegmentCompletionProtocol.Response COMMITTED__consumed(String instanceId, long offset) {
      SegmentCompletionProtocol.Response
          response;// Server reporting an offset on an already completed segment. Depending on the offset, either KEEP or DISCARD.
      if (offset == _winningOffset) {
        response = keep(instanceId, offset);
      } else {
        // Return DISCARD. It is hard to say how long the server will take to complete things.
        response = discard(instanceId, offset);
      }
      return response;
    }

    private SegmentCompletionProtocol.Response COMMITTED__commit(String instanceId, long offset) {
      if (offset == _winningOffset) {
        return keep(instanceId, offset);
      }
      return discard(instanceId, offset);
    }


    // A common method when the state is > COMMITTER_NOTIFIED.
    private SegmentCompletionProtocol.Response processConsumedAfterCommitStart(String instanceId, long offset, long now) {
      SegmentCompletionProtocol.Response response;
      // We have already picked a winner, and may or many not have heard from them.
      // Common case here is that another server is coming back to us with its offset. We either respond back with HOLD or CATCHUP.
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
        LOGGER.warn("{}:Aborting FSM because winner is reporting a segment while it is also committing instance={} offset={} now={}",
            _state, instanceId, offset, now);
        // Ask them to hold, just in case the committer fails for some reason..
        return abortAndReturnHold(now, instanceId, offset);
      } else {
        // Common case: A different instance is reporting.
        if (offset == _winningOffset) {
          // Wait until winner has posted the segment before asking this server to KEEP the segment.
          response = hold(instanceId, offset);
        } else if (offset < _winningOffset) {
          response = catchup(instanceId, offset);
        } else {
          // We have not yet committed, so ask the new responder to hold. They may be the new leader in case the
          // committer fails.
          response = hold(instanceId, offset);
        }
      }
      return response;
    }

    private SegmentCompletionProtocol.Response updateZk(String instanceId, long offset) {
      boolean success;
      if (!_state.equals(State.COMMITTER_UPLOADING)) {
        // State changed while we were out of sync. return a failed commit.
        LOGGER.warn("State change during upload: state={} segment={} winner={} winningOffset={}",
            _state, _segmentName.getSegmentName(), _winner, _winningOffset);
        return SegmentCompletionProtocol.RESP_FAILED;
      }
      LOGGER.info("Committing segment {} at offset {} winner {}", _segmentName.getSegmentName(), offset, instanceId);
      _state = State.COMMITTING;
      success = _segmentManager.commitSegment(_segmentName.getTableName(), _segmentName.getSegmentName(), _winningOffset);
      if (success) {
        _state = State.COMMITTED;
        LOGGER.info("Committed segment {} at offset {} winner {}", _segmentName.getSegmentName(), offset, instanceId);
        return SegmentCompletionProtocol.RESP_COMMIT_SUCCESS;
      }
      return null;
    }


    private SegmentCompletionProtocol.Response processCommitWhileUploading(String instanceId, long offset, long now) {
      LOGGER.info("Processing segmentCommit({}, {})", instanceId, offset);
      SegmentCompletionProtocol.Response response = abortIfTooLateAndReturnHold(now, instanceId, offset);
      if (response != null) {
        return response;
      }
      // Another committer (or same) came in while one was uploading. Ask them to hold in case this one fails.
      return new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.HOLD, offset);
    }

    private SegmentCompletionProtocol.Response checkBadCommitRequest(String instanceId, long offset, long now) {
      SegmentCompletionProtocol.Response response = abortIfTooLateAndReturnHold(now, instanceId, offset);
      if (response != null) {
        return response;
      } else  if (instanceId.equals(_winner) && offset != _winningOffset) {
        // Hmm. Committer has been notified, but either a different one is committing, or offset is different
        LOGGER.warn("{}:Aborting FSM (bad commit req) instance={} offset={} now={} winning={}", _state, instanceId,
            offset, now, _winningOffset);
        return abortAndReturnHold(now, instanceId, offset);
      }
      return null;
    }

    private SegmentCompletionProtocol.Response processCommitWhileHolding(String instanceId, long offset, long now) {
      LOGGER.info("Processing segmentCommit({}, {})", instanceId, offset);
      SegmentCompletionProtocol.Response response = abortIfTooLateAndReturnHold(now, instanceId, offset);
      if (response != null) {
        return response;
      }
      // We cannot get a commit if we are in this state, so ask them to hold. Maybe we are starting after a failover.
      // The server will re-send the segmentConsumed message.
      return hold(instanceId, offset);
    }

    // TODO TBD whether the segment is saved here or before entering the FSM.
    private boolean saveTheSegment() throws InterruptedException {
      // XXX: this can fail
      return true;
    }

    // Pick a winner, preferring this instance if tied for highest.
    // Side-effect: Sets the _winner and _winningOffset
    private void pickWinner(String preferredInstance) {
      long maxSoFar = -1;
      String winner = null;
      for (Map.Entry<String, Long> entry : _commitStateMap.entrySet()) {
        if (entry.getValue() > maxSoFar) {
          maxSoFar = entry.getValue();
          winner = entry.getKey();
        }
      }
      _winningOffset = maxSoFar;
      if (_commitStateMap.get(preferredInstance) == maxSoFar) {
        winner = preferredInstance;
      }
      _winner =  winner;
    }
  }
}
