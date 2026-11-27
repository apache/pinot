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
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.Striped;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.assignment.ReplicateModeAssignmentUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.PauselessConsumptionUtils;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionFSM.RecoveryStatus;
import org.apache.pinot.controller.helix.core.realtime.segment.CommittingSegmentDescriptor;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// This is a singleton class in the controller that drives the state machines for segments that are in the
/// committing stage.
///
/// SegmentCompletionManager has a sub-class that represents the FSM that the segment goes through while
/// executing the segment completion protocol between pinot servers and pinot controller. The protocol is
/// described in SegmentCompletionProtocol.
public class SegmentCompletionManager {
  // TODO Can we log using the segment name in the log message?
  public static final Logger LOGGER = LoggerFactory.getLogger(SegmentCompletionManager.class);

  private final HelixManager _helixManager;
  // A map that holds the FSM for each segment.
  private final Map<String, SegmentCompletionFSM> _fsmMap = new ConcurrentHashMap<>();
  // Weak identity keys retain the mode while any request still holds the FSM, even after conditional map cleanup.
  // Removing a name-keyed mode entry with its FSM would let an already admitted stale request bypass membership.
  private final Map<SegmentCompletionFSM, Boolean> _replicateModeFsms = new MapMaker().weakKeys().makeMap();
  private final Striped<Lock> _segmentCompletionLocks = Striped.lock(1024);
  private final Map<String, Long> _commitTimeMap = new ConcurrentHashMap<>();
  private final PinotLLCRealtimeSegmentManager _segmentManager;
  private final ControllerMetrics _controllerMetrics;
  private final LeadControllerManager _leadControllerManager;
  private final SegmentCompletionConfig _segmentCompletionConfig;


  // Half hour max commit time for all segments
  private static final int MAX_COMMIT_TIME_FOR_ALL_SEGMENTS_SECONDS = 1800;

  public static int getMaxCommitTimeForAllSegmentsSeconds() {
    return MAX_COMMIT_TIME_FOR_ALL_SEGMENTS_SECONDS;
  }

  // TODO keep some history of past committed segments so that we can avoid looking up PROPERTYSTORE if some server
  //  comes in late.

  public SegmentCompletionManager(HelixManager helixManager, PinotLLCRealtimeSegmentManager segmentManager,
      ControllerMetrics controllerMetrics, LeadControllerManager leadControllerManager,
      int segmentCommitTimeoutSeconds, SegmentCompletionConfig segmentCompletionConfig) {
    _helixManager = helixManager;
    _segmentManager = segmentManager;
    _controllerMetrics = controllerMetrics;
    _leadControllerManager = leadControllerManager;
    SegmentCompletionProtocol.setMaxSegmentCommitTimeMs(
        TimeUnit.MILLISECONDS.convert(segmentCommitTimeoutSeconds, TimeUnit.SECONDS));
    _segmentCompletionConfig = segmentCompletionConfig;

    // Initialize the FSM Factory
    SegmentCompletionFSMFactory.init(_segmentCompletionConfig);
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
    StreamConfig streamConfig = IngestionConfigUtils.getFirstStreamConfig(tableConfig);
    return StreamConsumerFactoryProvider.create(streamConfig).createStreamMsgOffsetFactory();
  }

  public Long getCommitTime(String tableName) {
    return _commitTimeMap.get(tableName);
  }

  public void setCommitTime(String tableName, long commitTime) {
    _commitTimeMap.put(tableName, commitTime);
  }

  public ControllerMetrics getControllerMetrics() {
    return _controllerMetrics;
  }

  /// Observes a locally known completion attempt without creating an FSM or reading ZooKeeper. Absence only means
  /// that this Controller has no local attempt; recovery must still inspect persisted segment state.
  public RecoveryStatus getRecoveryStatus(String segmentName) {
    SegmentCompletionFSM fsm = _fsmMap.get(segmentName);
    return fsm != null ? fsm.getRecoveryStatus() : RecoveryStatus.NO_ACTIVE_COMMIT;
  }

  private SegmentCompletionFSM lookupOrCreateFsm(LLCSegmentName llcSegmentName, String msgType,
      @Nullable SegmentZKMetadata metadata) {
    String segmentName = llcSegmentName.getSegmentName();
    // Look up without locking first. computeIfAbsent() can lock the hash bin even when the key is present, so it can
    // wait for the creation of another segment's FSM, which reads ZooKeeper.
    SegmentCompletionFSM fsm = _fsmMap.get(segmentName);
    return fsm != null ? fsm : _fsmMap.computeIfAbsent(segmentName, k -> createFsm(llcSegmentName, msgType, metadata));
  }

  /// Parses an offset reported for the segment with the offset factory of its FSM, so that a request for an existing
  /// FSM does not read the table config from ZooKeeper.
  private StreamPartitionMsgOffset parseOffset(SegmentCompletionFSM fsm, LLCSegmentName llcSegmentName,
      String offset) {
    StreamPartitionMsgOffsetFactory factory = fsm.getStreamPartitionMsgOffsetFactory();
    if (factory == null) {
      factory = getStreamPartitionMsgOffsetFactory(llcSegmentName);
    }
    return factory.create(offset);
  }

  private SegmentCompletionFSM createFsm(LLCSegmentName llcSegmentName, String msgType,
      @Nullable SegmentZKMetadata metadata) {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(llcSegmentName.getTableName());
    String segmentName = llcSegmentName.getSegmentName();
    SegmentZKMetadata segmentMetadata = metadata != null ? metadata
        : _segmentManager.getSegmentZKMetadata(realtimeTableName, segmentName, null);

    TableConfig tableConfig = _segmentManager.getTableConfig(realtimeTableName);
    String factoryName;
    if (PauselessConsumptionUtils.isPauselessEnabled(tableConfig)) {
      factoryName = _segmentCompletionConfig.getDefaultPauselessFsmScheme();
      _controllerMetrics.setValueOfTableGauge(realtimeTableName, ControllerGauge.PAUSELESS_CONSUMPTION_ENABLED, 1);
    } else {
      factoryName = _segmentCompletionConfig.getDefaultFsmScheme();
      _controllerMetrics.setValueOfTableGauge(realtimeTableName, ControllerGauge.PAUSELESS_CONSUMPTION_ENABLED, 0);
    }

    Preconditions.checkState(SegmentCompletionFSMFactory.isFactoryTypeSupported(factoryName),
        "No FSM registered for name: " + factoryName);

    SegmentCompletionFSM fsm =
        SegmentCompletionFSMFactory.createFSM(factoryName, this, _segmentManager, llcSegmentName, segmentMetadata);
    fsm.transitionToInitialState(msgType);
    if (segmentMetadata.isReplicateMode()) {
      _replicateModeFsms.put(fsm, true);
    }

    LOGGER.info("Created FSM {}", fsm);
    return fsm;
  }

  /// Serializes local replicate-mode admission and assignment changes. Recovery performs its final validation and
  /// CAS inside this operation. Locks are bounded and reentrant; accepted commitEnd I/O runs outside the gate because
  /// that exact attempt may legitimately finish after reassignment. This does not replace ZooKeeper version checks.
  public <T> T withSegmentCompletionLock(String segmentName, Supplier<T> operation) {
    Lock lock = _segmentCompletionLocks.get(segmentName);
    lock.lock();
    try {
      return operation.get();
    } finally {
      lock.unlock();
    }
  }

  public SegmentCompletionProtocol.Response segmentConsumed(SegmentCompletionProtocol.Request.Params params) {
    LLCSegmentName name = new LLCSegmentName(params.getSegmentName());
    return processRequest(params, RequestKind.CONSUMED, fsm -> fsm.segmentConsumed(params.getInstanceId(),
        parseOffset(fsm, name, params.getStreamPartitionMsgOffset()), params.getReason()));
  }

  public SegmentCompletionProtocol.Response segmentCommitStart(SegmentCompletionProtocol.Request.Params params) {
    return processRequest(params, RequestKind.COMMIT_START, fsm -> fsm.segmentCommitStart(params));
  }

  public SegmentCompletionProtocol.Response extendBuildTime(SegmentCompletionProtocol.Request.Params params) {
    LLCSegmentName name = new LLCSegmentName(params.getSegmentName());
    return processRequest(params, RequestKind.EXTEND, fsm -> fsm.extendBuildTime(params.getInstanceId(),
        parseOffset(fsm, name, params.getStreamPartitionMsgOffset()), params.getExtraTimeSec()));
  }

  public SegmentCompletionProtocol.Response reduceSegmentSizeAndReset(SegmentCompletionProtocol.Request.Params params) {
    return processRequest(params, RequestKind.RESET, fsm -> {
      if (fsm != null && fsm.isImmutableSegmentCreated()) {
        LOGGER.warn("Segment {} cannot build is a false alert", params.getSegmentName());
        return SegmentCompletionProtocol.RESP_DISCARD;
      }
      _segmentManager.reduceSegmentSizeAndReset(new LLCSegmentName(params.getSegmentName()), params.getNumRows());
      removeFsm(params.getSegmentName(), fsm);
      return SegmentCompletionProtocol.RESP_PROCESSED;
    });
  }

  public SegmentCompletionProtocol.Response segmentStoppedConsuming(SegmentCompletionProtocol.Request.Params params) {
    LLCSegmentName name = new LLCSegmentName(params.getSegmentName());
    return processRequest(params, RequestKind.STOPPED, fsm -> fsm.stoppedConsuming(params.getInstanceId(),
        parseOffset(fsm, name, params.getStreamPartitionMsgOffset()), params.getReason()));
  }

  /// Completes an existing accepted attempt. A stale server cannot create an FSM to regain commit authorization.
  /// Once metadata is DONE, a matching retry succeeds without republishing files or changing the selected artifact.
  public SegmentCompletionProtocol.Response segmentCommitEnd(SegmentCompletionProtocol.Request.Params params,
      CommittingSegmentDescriptor descriptor) {
    LLCSegmentName name = new LLCSegmentName(params.getSegmentName());
    if (!isLeader(name.getTableName()) || !_helixManager.isConnected()) {
      return SegmentCompletionProtocol.RESP_NOT_LEADER;
    }
    try {
      SegmentCompletionFSM existing = _fsmMap.get(name.getSegmentName());
      SegmentZKMetadata metadata = existing == null ? getSegmentMetadata(name) : null;
      if (!isReplicateMode(existing, metadata)) {
        return invokeFsm(name, RequestKind.COMMIT_END, metadata, fsm -> fsm.segmentCommitEnd(params, descriptor));
      }
      CommitEndAdmission admission = withSegmentCompletionLock(name.getSegmentName(), () -> {
        if (!isLeader(name.getTableName()) || !_helixManager.isConnected()) {
          return new CommitEndAdmission(null, SegmentCompletionProtocol.RESP_NOT_LEADER);
        }
        SegmentZKMetadata currentMetadata = getSegmentMetadata(name);
        if (currentMetadata == null || !currentMetadata.isReplicateMode()) {
          return new CommitEndAdmission(null, SegmentCompletionProtocol.RESP_FAILED);
        }
        if (currentMetadata.getStatus() == Status.DONE) {
          return new CommitEndAdmission(null,
              completedResponse(name, currentMetadata, params, RequestKind.COMMIT_END, descriptor));
        }
        SegmentCompletionFSM fsm = _fsmMap.get(name.getSegmentName());
        if (fsm == null || !fsm.isAcceptedCommitter(params.getInstanceId(), params.getStreamPartitionMsgOffset())) {
          return new CommitEndAdmission(null, SegmentCompletionProtocol.RESP_FAILED);
        }
        return new CommitEndAdmission(fsm, null);
      });
      if (admission.response() != null) {
        return admission.response();
      }
      SegmentCompletionFSM fsm = admission.fsm();
      try {
        return fsm.segmentCommitEnd(params, descriptor);
      } finally {
        if (fsm.isDone()) {
          removeFsm(name.getSegmentName(), fsm);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception in segmentCommitEnd for segment {}", name.getSegmentName(), e);
      return SegmentCompletionProtocol.RESP_FAILED;
    }
  }

  private SegmentCompletionProtocol.Response processRequest(SegmentCompletionProtocol.Request.Params params,
      RequestKind kind, Function<SegmentCompletionFSM, SegmentCompletionProtocol.Response> operation) {
    LLCSegmentName name = new LLCSegmentName(params.getSegmentName());
    // Preserve the ordinary reset endpoint's existing behavior; replicate-mode resets are checked inside the gate.
    if (kind != RequestKind.RESET && (!isLeader(name.getTableName()) || !_helixManager.isConnected())) {
      return SegmentCompletionProtocol.RESP_NOT_LEADER;
    }
    try {
      SegmentCompletionFSM existing = _fsmMap.get(name.getSegmentName());
      SegmentZKMetadata metadata = existing == null ? getSegmentMetadata(name) : null;
      if (!isReplicateMode(existing, metadata)) {
        return invokeFsm(name, kind, metadata, operation);
      }
      return withFsmAdmissionLock(name.getSegmentName(), () -> {
        if (!isLeader(name.getTableName()) || !_helixManager.isConnected()) {
          return SegmentCompletionProtocol.RESP_NOT_LEADER;
        }
        SegmentZKMetadata currentMetadata = getSegmentMetadata(name);
        if (currentMetadata == null || !currentMetadata.isReplicateMode()) {
          return SegmentCompletionProtocol.RESP_FAILED;
        }
        if (currentMetadata.getStatus() == Status.DONE) {
          return completedResponse(name, currentMetadata, params, kind, null);
        }
        SegmentCompletionFSM fsm = _fsmMap.get(name.getSegmentName());
        if (!isCurrentMember(name, currentMetadata, params, kind, fsm)) {
          if (kind == RequestKind.STOPPED) {
            return SegmentCompletionProtocol.RESP_PROCESSED;
          }
          return kind == RequestKind.CONSUMED || kind == RequestKind.RESET
              ? SegmentCompletionProtocol.RESP_DISCARD : SegmentCompletionProtocol.RESP_FAILED;
        }
        return invokeFsm(name, kind, currentMetadata, operation);
      });
    } catch (Exception e) {
      LOGGER.error("Caught exception processing {} for segment {}", kind, name.getSegmentName(), e);
      return SegmentCompletionProtocol.RESP_FAILED;
    }
  }

  /// Never hold the assignment gate while waiting for an existing FSM's commit monitor. Once both are held, verify
  /// that cleanup did not replace that FSM while we waited. Recovery uses only the gate and nonblocking observations.
  private SegmentCompletionProtocol.Response withFsmAdmissionLock(String segmentName,
      Supplier<SegmentCompletionProtocol.Response> operation) {
    while (true) {
      SegmentCompletionFSM expected = _fsmMap.get(segmentName);
      SegmentCompletionProtocol.Response response;
      if (expected == null) {
        response = withSegmentCompletionLock(segmentName,
            () -> _fsmMap.get(segmentName) == null ? operation.get() : null);
      } else {
        synchronized (expected) {
          response = withSegmentCompletionLock(segmentName,
              () -> _fsmMap.get(segmentName) == expected ? operation.get() : null);
        }
      }
      if (response != null) {
        return response;
      }
    }
  }

  private boolean isCurrentMember(LLCSegmentName name, SegmentZKMetadata metadata,
      SegmentCompletionProtocol.Request.Params params, RequestKind kind, @Nullable SegmentCompletionFSM fsm) {
    IdealState idealState =
        _segmentManager.getIdealState(TableNameBuilder.REALTIME.tableNameWithType(name.getTableName()));
    if (idealState == null) {
      return false;
    }
    Map<String, String> assignment = idealState.getInstanceStateMap(name.getSegmentName());
    if (assignment == null || assignment.size() != 1) {
      return false;
    }
    String state = assignment.get(params.getInstanceId());
    if (metadata.getStatus() == Status.IN_PROGRESS) {
      return SegmentStateModel.CONSUMING.equals(state) && params.getInstanceId().equals(
          ReplicateModeAssignmentUtils.getLeader(idealState, name.getPartitionGroupId()));
    }
    if (metadata.getStatus() != Status.COMMITTING || metadata.getEndOffset() == null) {
      return false;
    }
    boolean consumingOwner = SegmentStateModel.CONSUMING.equals(state);
    if (consumingOwner) {
      // Commit start persists COMMITTING before publishing the successor and the old segment's ONLINE assignment.
      // A failure in that window must let its current owner resume the native commit-start protocol.
      if ((kind != RequestKind.CONSUMED && kind != RequestKind.COMMIT_START) || !params.getInstanceId().equals(
          ReplicateModeAssignmentUtils.getLeader(idealState, name.getPartitionGroupId()))) {
        return false;
      }
    } else if (!SegmentStateModel.ONLINE.equals(state)) {
      return false;
    }
    if (kind != RequestKind.CONSUMED && kind != RequestKind.COMMIT_START && kind != RequestKind.EXTEND) {
      return false;
    }
    StreamPartitionMsgOffsetFactory factory = fsm != null ? fsm.getStreamPartitionMsgOffsetFactory() : null;
    if (factory == null) {
      factory = getStreamPartitionMsgOffsetFactory(name);
    }
    if (factory.create(params.getStreamPartitionMsgOffset()).compareTo(factory.create(metadata.getEndOffset())) != 0) {
      return false;
    }
    // Pauseless uploading belongs to this segment's sole ONLINE owner. The newer consuming segment can already
    // have another partition leader after an explicit rebalance; that must not revoke this uploader's lease.
    if (!consumingOwner && (kind == RequestKind.COMMIT_START || kind == RequestKind.EXTEND) && fsm != null
        && fsm.isAcceptedCommitter(params.getInstanceId(), params.getStreamPartitionMsgOffset())) {
      return true;
    }
    // A Controller restart loses its FSM, while this segment's owner may still have the complete built segment.
    // Resume native commit start only for its exact persisted range; an old nonmember cannot regain authorization.
    if (kind == RequestKind.CONSUMED || kind == RequestKind.COMMIT_START) {
      return !_segmentManager.isExceededMaxSegmentCompletionTime(
          TableNameBuilder.REALTIME.tableNameWithType(name.getTableName()), name.getSegmentName(), getCurrentTimeMs());
    }
    return false;
  }

  private SegmentCompletionProtocol.Response completedResponse(LLCSegmentName name, SegmentZKMetadata metadata,
      SegmentCompletionProtocol.Request.Params params, RequestKind kind,
      @Nullable CommittingSegmentDescriptor descriptor) {
    if (kind == RequestKind.STOPPED) {
      return SegmentCompletionProtocol.RESP_PROCESSED;
    }
    if (kind == RequestKind.RESET || kind == RequestKind.CONSUMED || kind == RequestKind.COMMIT_START) {
      // KEEP makes the Server install a local build without checking the authoritative artifact's CRC.
      return SegmentCompletionProtocol.RESP_DISCARD;
    }
    if (kind == RequestKind.EXTEND) {
      return SegmentCompletionProtocol.RESP_FAILED;
    }
    StreamPartitionMsgOffsetFactory factory = getStreamPartitionMsgOffsetFactory(name);
    boolean matches = metadata.getEndOffset() != null
        && factory.create(params.getStreamPartitionMsgOffset()).compareTo(factory.create(metadata.getEndOffset())) == 0;
    return matches && descriptor != null && descriptor.getSegmentMetadata() != null && metadata.getCrc() >= 0
        && descriptor.getSegmentMetadata().getCrc() == metadata.getCrc()
        ? SegmentCompletionProtocol.RESP_COMMIT_SUCCESS : SegmentCompletionProtocol.RESP_FAILED;
  }

  private boolean isReplicateMode(@Nullable SegmentCompletionFSM fsm, @Nullable SegmentZKMetadata metadata) {
    return fsm != null ? _replicateModeFsms.containsKey(fsm) : metadata != null && metadata.isReplicateMode();
  }

  @Nullable
  private SegmentZKMetadata getSegmentMetadata(LLCSegmentName name) {
    return _segmentManager.getSegmentZKMetadata(TableNameBuilder.REALTIME.tableNameWithType(name.getTableName()),
        name.getSegmentName(), null);
  }

  private SegmentCompletionProtocol.Response invokeFsm(LLCSegmentName name, RequestKind kind,
      @Nullable SegmentZKMetadata metadata,
      Function<SegmentCompletionFSM, SegmentCompletionProtocol.Response> operation) {
    if (kind != RequestKind.RESET && metadata != null && metadata.isReplicateMode()
        && _fsmMap.get(name.getSegmentName()) == null) {
      // Replicate-mode callers hold the segment gate. Own the new FSM's monitor before publishing it: otherwise
      // another request could acquire the published monitor and wait for our gate while we wait for its monitor.
      SegmentCompletionFSM created = createFsm(name, kind._messageType, metadata);
      synchronized (created) {
        Preconditions.checkState(_fsmMap.putIfAbsent(name.getSegmentName(), created) == null,
            "Completion FSM changed while holding the admission gate for segment: %s", name.getSegmentName());
        return invokeAndCleanUp(name.getSegmentName(), created, operation);
      }
    }
    SegmentCompletionFSM fsm = kind == RequestKind.RESET ? _fsmMap.get(name.getSegmentName())
        : lookupOrCreateFsm(name, kind._messageType, metadata);
    return invokeAndCleanUp(name.getSegmentName(), fsm, operation);
  }

  private SegmentCompletionProtocol.Response invokeAndCleanUp(String segmentName, @Nullable SegmentCompletionFSM fsm,
      Function<SegmentCompletionFSM, SegmentCompletionProtocol.Response> operation) {
    try {
      return operation.apply(fsm);
    } finally {
      if (fsm != null && fsm.isDone()) {
        removeFsm(segmentName, fsm);
      }
    }
  }

  private void removeFsm(String name, @Nullable SegmentCompletionFSM fsm) {
    if (fsm != null && _fsmMap.remove(name, fsm)) {
      LOGGER.info("Removed FSM: {}", fsm);
    }
  }

  private enum RequestKind {
    CONSUMED(SegmentCompletionProtocol.MSG_TYPE_CONSUMED),
    COMMIT_START(SegmentCompletionProtocol.MSG_TYPE_COMMIT),
    COMMIT_END(SegmentCompletionProtocol.MSG_TYPE_COMMIT),
    EXTEND(SegmentCompletionProtocol.MSG_TYPE_COMMIT),
    STOPPED(SegmentCompletionProtocol.MSG_TYPE_STOPPED_CONSUMING),
    RESET(SegmentCompletionProtocol.MSG_TYPE_COMMIT);

    private final String _messageType;

    RequestKind(String messageType) {
      _messageType = messageType;
    }
  }

  private record CommitEndAdmission(@Nullable SegmentCompletionFSM fsm,
                                    @Nullable SegmentCompletionProtocol.Response response) {
  }

  @VisibleForTesting
  protected boolean isLeader(String tableName) {
    return _leadControllerManager.isLeaderForTable(tableName);
  }
}
