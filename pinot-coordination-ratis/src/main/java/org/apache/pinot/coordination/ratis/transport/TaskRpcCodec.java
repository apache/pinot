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
package org.apache.pinot.coordination.ratis.transport;

import java.io.IOException;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Capability;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.AttemptState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.AttemptStatus;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Input;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.OutputPolicy;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Progress;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.PublicationState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.QueueKey;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.QueueState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.ScheduleState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.TaskState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.TaskStatus;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.WorkSnapshot;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.WorkerOperation;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.transport.ParticipantAccessPolicy.ParticipantIdentity;


/// Closed task commands and projections. Worker identities and durable request client IDs are derived from the
/// authenticated MINION policy; payloads cannot select another participant or generic metadata mutation.
final class TaskRpcCodec {
  private TaskRpcCodec() {
  }

  static MetadataProtocol.TaskSessionCommand command(MetadataRpc.TaskCommandRequest request,
      ParticipantIdentity identity) throws IOException {
    MetadataRpcCodec.rejectUnknown(request);
    require(request.hasSession(), "Minion session required");
    UUID history = UUID.fromString(request.getHistoryId());
    ParticipantToken minion = minion(history, request.getSession(), identity);
    String clientId = TaskProtocol.clientId(minion);
    MetadataProtocol.Command command = switch (request.getCommandCase()) {
      case REGISTER -> {
        require(minion.incarnation().toString().equals(request.getRegister().getIncarnation()),
            "Task request registration must use the participant incarnation");
        yield new MetadataProtocol.RegisterClient(history, clientId, minion.incarnation(),
            request.getRegister().getExpectedEpoch());
      }
      case ACKNOWLEDGE -> new MetadataProtocol.Acknowledge(history, clientId,
          request.getAcknowledge().getClientEpoch(), request.getAcknowledge().getStreamId(),
          request.getAcknowledge().getThroughSequence());
      case OPERATION -> {
        MetadataRpc.SequencedTaskOperation operation = request.getOperation();
        yield new MetadataProtocol.NativeTaskCommand(history, clientId, operation.getClientEpoch(),
            operation.getStreamId(), operation.getSequence(), minion, operation(operation));
      }
      default -> throw new IllegalArgumentException("Typed task command required");
    };
    MetadataProtocol.TaskSessionCommand guarded = new MetadataProtocol.TaskSessionCommand(minion, command);
    MetadataProtocol.encode(guarded);
    return guarded;
  }

  private static WorkerOperation operation(MetadataRpc.SequencedTaskOperation request) {
    return switch (request.getOperationCase()) {
      case CLAIM -> {
        MetadataRpc.TaskClaim claim = request.getClaim();
        yield new TaskProtocol.Claim(UUID.fromString(claim.getTaskId()), claim.getExpectedVersion());
      }
      case CHECKPOINT -> {
        MetadataRpc.TaskCheckpoint checkpoint = request.getCheckpoint();
        require(checkpoint.hasProgress(), "Task progress required");
        yield new TaskProtocol.Checkpoint(UUID.fromString(checkpoint.getTaskId()), checkpoint.getExpectedVersion(),
            checkpoint.getAttempt(), progress(checkpoint.getProgress()));
      }
      case COMPLETE -> {
        MetadataRpc.TaskComplete complete = request.getComplete();
        require(complete.hasProgress(), "Task completion progress required");
        yield new TaskProtocol.Complete(UUID.fromString(complete.getTaskId()), complete.getExpectedVersion(),
            complete.getAttempt(), AttemptStatus.valueOf(complete.getResult().name()),
            progress(complete.getProgress()));
      }
      case PUBLISH -> {
        MetadataRpc.TaskPublishOfflineOutputs publish = request.getPublish();
        require(publish.hasProgress() && publish.getOutputsCount() > 0
            && publish.getOutputsCount() <= TaskProtocol.MAX_OUTPUTS,
            "Bounded task outputs and progress required");
        yield new TaskProtocol.PublishOfflineOutputs(UUID.fromString(publish.getTaskId()), publish.getExpectedVersion(),
            publish.getAttempt(), publish.getOutputsList().stream().map(OfflineSegmentRpcCodec::content).toList(),
            progress(publish.getProgress()));
      }
      default -> throw new IllegalArgumentException("Typed task operation required");
    };
  }

  static MetadataProtocol.TaskSessionQuery outcome(MetadataRpc.TaskOutcomeRequest request,
      ParticipantIdentity identity) {
    MetadataRpcCodec.rejectUnknown(request);
    require(request.hasSession() && request.hasOutcome(), "Minion session and outcome identity required");
    UUID history = UUID.fromString(request.getHistoryId());
    ParticipantToken minion = minion(history, request.getSession(), identity);
    return new MetadataProtocol.TaskSessionQuery(minion, new MetadataProtocol.GetOutcome(history,
        TaskProtocol.clientId(minion), request.getOutcome().getClientEpoch(), request.getOutcome().getStreamId(),
        request.getOutcome().getSequence()));
  }

  static ParticipantToken work(MetadataRpc.TaskWorkRequest request, ParticipantIdentity identity) {
    MetadataRpcCodec.rejectUnknown(request);
    require(request.hasSession(), "Minion session required");
    require(request.getMaxRecords() > 0 && request.getMaxRecords() <= TaskProtocol.MAX_SNAPSHOT_RECORDS
        && request.getMaxBytes() >= TaskProtocol.MIN_SNAPSHOT_BYTES
        && request.getMaxBytes() <= TaskProtocol.MAX_SNAPSHOT_BYTES, "Task projection bounds required");
    return minion(UUID.fromString(request.getHistoryId()), request.getSession(), identity);
  }

  static MetadataRpc.TaskWorkResult work(WorkSnapshot snapshot) throws IOException {
    MetadataRpc.TaskWorkResult.Builder result = MetadataRpc.TaskWorkResult.newBuilder()
        .setStatus(MetadataRpc.ResultStatus.valueOf(snapshot.status().name())).setDetail(snapshot.detail())
        .setRevision(MetadataRpc.Revision.newBuilder().setHistoryId(snapshot.revision().historyId().toString())
            .setIndex(snapshot.revision().index()));
    if (snapshot.participant() != null) {
      result.setParticipant(MembershipRpcCodec.participant(snapshot.participant()));
    }
    if (snapshot.control() != null) {
      result.setControl(MembershipRpcCodec.control(snapshot.control()));
    }
    for (MetadataProtocol.CatalogRecord record : snapshot.records()) {
      result.addRecords(MetadataRpcCodec.record(record));
    }
    return result.build();
  }

  private static ParticipantToken minion(UUID history, MetadataRpc.ParticipantSession session,
      ParticipantIdentity identity) {
    require(identity.role() == Role.MINION && identity.capabilities().contains(Capability.MINION_TASK)
        && session.getRegistrationEpoch() > 0, "Authorized MINION task session required");
    return new ParticipantToken(history, identity.participantId(), Role.MINION,
        UUID.fromString(session.getIncarnation()), session.getRegistrationEpoch());
  }

  private static Progress progress(MetadataRpc.TaskProgress progress) {
    require(progress.getCompletedUnits() >= 0 && progress.getDetail().length() <= TaskProtocol.MAX_DETAIL_LENGTH,
        "Invalid task progress");
    return new Progress(progress.getCompletedUnits(), progress.getDetail());
  }

  static MetadataRpc.TaskQueueState queue(QueueState state) {
    return MetadataRpc.TaskQueueState.newBuilder().setHistoryId(state.historyId().toString())
        .setQueue(key(state.queue())).setPaused(state.paused())
        .setController(ControllerRpcCodec.owner(state.controller()))
        .build();
  }

  static QueueState queue(MetadataRpc.TaskQueueState state) {
    MetadataRpcCodec.rejectUnknown(state);
    require(state.hasQueue() && state.hasController(), "Task queue identity and owner are required");
    return new QueueState(UUID.fromString(state.getHistoryId()), key(state.getQueue()), state.getPaused(),
        ControllerRpcCodec.owner(state.getController()));
  }

  static MetadataRpc.TaskExecutionState task(TaskState state) {
    return MetadataRpc.TaskExecutionState.newBuilder().setHistoryId(state.historyId().toString())
        .setTaskId(state.taskId().toString()).setInput(input(state.input()))
        .addAllDependencies(state.dependencies().stream().map(TaskRpcCodec::version).toList())
        .setController(ControllerRpcCodec.owner(state.controller()))
        .setState(MetadataRpc.TaskExecutionState.State.valueOf(state.status().name()))
        .setAttempt(state.attempt()).build();
  }

  static TaskState task(MetadataRpc.TaskExecutionState state) {
    MetadataRpcCodec.rejectUnknown(state);
    require(state.hasInput() && state.hasController() && state.getDependenciesCount() >= 2
        && state.getDependenciesCount() <= TaskProtocol.MAX_DEPENDENCIES && state.getAttempt() >= 0
        && state.getAttempt() <= state.getInput().getMaxAttempts(), "Invalid task identity, dependencies or attempt");
    return new TaskState(UUID.fromString(state.getHistoryId()), UUID.fromString(state.getTaskId()),
        input(state.getInput()),
        state.getDependenciesList().stream().map(TaskRpcCodec::version).toList(),
        ControllerRpcCodec.owner(state.getController()), TaskStatus.valueOf(state.getState().name()),
        state.getAttempt());
  }

  static MetadataRpc.TaskAttemptState attempt(AttemptState state) {
    return MetadataRpc.TaskAttemptState.newBuilder().setHistoryId(state.historyId().toString())
        .setTaskId(state.taskId().toString()).setAttempt(state.attempt())
        .setParticipantAtClaim(MembershipRpcCodec.participant(state.participantAtClaim()))
        .setParticipantVersion(version(state.participantVersion())).setClaimedIndex(state.claimedIndex())
        .setState(MetadataRpc.TaskAttemptState.State.valueOf(state.status().name()))
        .setProgress(MetadataRpc.TaskProgress.newBuilder().setCompletedUnits(state.progress().completedUnits())
            .setDetail(state.progress().detail())).build();
  }

  static AttemptState attempt(MetadataRpc.TaskAttemptState state) {
    MetadataRpcCodec.rejectUnknown(state);
    require(state.hasParticipantAtClaim() && state.hasParticipantVersion() && state.hasProgress()
        && state.getAttempt() > 0 && state.getAttempt() <= TaskProtocol.MAX_ATTEMPTS && state.getClaimedIndex() > 0
        && state.getProgress().getCompletedUnits() >= 0
        && state.getProgress().getDetail().length() <= TaskProtocol.MAX_DETAIL_LENGTH,
        "Invalid task attempt projection");
    var participant = MembershipRpcCodec.participant(state.getParticipantAtClaim());
    require(participant.participant().role() == Role.MINION
        && participant.lastModifiedIndex() <= state.getClaimedIndex(),
        "Task claim requires a prior minion registration");
    return new AttemptState(UUID.fromString(state.getHistoryId()), UUID.fromString(state.getTaskId()),
        state.getAttempt(),
        participant, version(state.getParticipantVersion()), state.getClaimedIndex(),
        AttemptStatus.valueOf(state.getState().name()),
        new Progress(state.getProgress().getCompletedUnits(), state.getProgress().getDetail()));
  }

  static MetadataRpc.TaskScheduleState schedule(ScheduleState state) {
    return MetadataRpc.TaskScheduleState.newBuilder().setHistoryId(state.historyId().toString())
        .setScheduleId(state.scheduleId().toString()).setInput(input(state.input()))
        .addAllDependencies(state.dependencies().stream().map(TaskRpcCodec::version).toList())
        .setController(ControllerRpcCodec.owner(state.controller())).setFirstRound(state.firstRound())
        .setIntervalRounds(state.intervalRounds()).setMaxOccurrences(state.maxOccurrences())
        .setFiredOccurrences(state.firedOccurrences()).setPolicyVersion(state.policyVersion()).build();
  }

  static ScheduleState schedule(MetadataRpc.TaskScheduleState state) {
    MetadataRpcCodec.rejectUnknown(state);
    require(state.hasInput() && state.hasController() && state.getDependenciesCount() >= 2
        && state.getDependenciesCount() <= TaskProtocol.MAX_DEPENDENCIES && state.getFirstRound() >= 0
        && state.getIntervalRounds() >= 0 && (state.getMaxOccurrences() == 1 || state.getIntervalRounds() > 0)
        && state.getMaxOccurrences() > 0
        && state.getMaxOccurrences() <= TaskProtocol.MAX_OCCURRENCES && state.getFiredOccurrences() >= 0
        && state.getFiredOccurrences() <= state.getMaxOccurrences() && state.getPolicyVersion() > 0,
        "Invalid task schedule projection");
    return new ScheduleState(UUID.fromString(state.getHistoryId()), UUID.fromString(state.getScheduleId()),
        input(state.getInput()), state.getDependenciesList().stream().map(TaskRpcCodec::version).toList(),
        ControllerRpcCodec.owner(state.getController()), state.getFirstRound(), state.getIntervalRounds(),
        state.getMaxOccurrences(), state.getFiredOccurrences(), state.getPolicyVersion());
  }

  static MetadataRpc.TaskPublicationState publication(PublicationState state) {
    return MetadataRpc.TaskPublicationState.newBuilder().setHistoryId(state.historyId().toString())
        .setTaskId(state.taskId().toString()).setAttempt(state.attempt())
        .setMinion(MembershipRpcCodec.participant(state.minion()))
        .addAllOutputs(state.outputs().stream().map(OfflineSegmentRpcCodec::content).toList())
        .setCommittedIndex(state.committedIndex()).build();
  }

  static PublicationState publication(MetadataRpc.TaskPublicationState state) {
    MetadataRpcCodec.rejectUnknown(state);
    require(state.hasMinion() && state.getAttempt() > 0 && state.getAttempt() <= TaskProtocol.MAX_ATTEMPTS
        && state.getOutputsCount() > 0 && state.getOutputsCount() <= TaskProtocol.MAX_OUTPUTS
        && state.getCommittedIndex() > 0, "Invalid task publication projection");
    ParticipantToken minion = MembershipRpcCodec.participant(state.getMinion());
    require(minion.role() == Role.MINION, "Task publication requires a MINION session");
    return new PublicationState(UUID.fromString(state.getHistoryId()), UUID.fromString(state.getTaskId()),
        state.getAttempt(), minion, state.getOutputsList().stream().map(OfflineSegmentRpcCodec::content).toList(),
        state.getCommittedIndex());
  }

  private static MetadataRpc.TaskQueueKey key(QueueKey key) {
    return MetadataRpc.TaskQueueKey.newBuilder().setTableName(key.tableName())
        .setTableObjectId(key.tableObjectId().toString()).setTaskType(key.taskType()).build();
  }

  private static QueueKey key(MetadataRpc.TaskQueueKey key) {
    QueueKey result = new QueueKey(key.getTableName(), UUID.fromString(key.getTableObjectId()), key.getTaskType());
    TaskProtocol.queueName(result);
    return result;
  }

  private static MetadataRpc.TaskInput input(Input input) {
    MetadataRpc.TaskInput.Builder result = MetadataRpc.TaskInput.newBuilder().setQueue(key(input.queue()))
        .setMinionId(input.minionId()).putAllConfigs(input.configs()).setMaxAttempts(input.maxAttempts());
    if (input.outputPolicy() != null) {
      result.setOutputPolicy(MetadataRpc.TaskOutputPolicy.newBuilder().setRootUri(input.outputPolicy().rootUri())
          .setMaxOutputs(input.outputPolicy().maxOutputs()));
    }
    return result.build();
  }

  private static Input input(MetadataRpc.TaskInput input) {
    require(input.hasQueue() && !input.getMinionId().isBlank() && input.getMinionId().length() <= 128
        && input.getMaxAttempts() > 0 && input.getMaxAttempts() <= TaskProtocol.MAX_ATTEMPTS
        && input.getConfigsCount() <= TaskProtocol.MAX_CONFIGS, "Invalid task input projection");
    input.getConfigsMap().forEach((key, value) -> require(
        !key.isBlank() && key.length() <= 256 && value.length() <= 16_384,
        "Task configuration exceeds supported bounds"));
    OutputPolicy output = null;
    if (input.hasOutputPolicy()) {
      require(!input.getOutputPolicy().getRootUri().isBlank() && input.getOutputPolicy().getMaxOutputs() > 0
          && input.getOutputPolicy().getMaxOutputs() <= TaskProtocol.MAX_OUTPUTS, "Invalid task output policy");
      output = new OutputPolicy(input.getOutputPolicy().getRootUri(), input.getOutputPolicy().getMaxOutputs());
    }
    return new Input(key(input.getQueue()), input.getMinionId(), input.getConfigsMap(), input.getMaxAttempts(), output);
  }

  private static MetadataRpc.ObjectVersion version(ObjectVersion version) {
    return MetadataRpc.ObjectVersion.newBuilder().setHistoryId(version.historyId().toString())
        .setObjectId(version.objectId().toString()).setVersion(version.version()).build();
  }

  private static ObjectVersion version(MetadataRpc.ObjectVersion version) {
    require(version.getVersion() > 0, "Task dependency version must be positive");
    return new ObjectVersion(UUID.fromString(version.getHistoryId()), UUID.fromString(version.getObjectId()),
        version.getVersion());
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }
}
