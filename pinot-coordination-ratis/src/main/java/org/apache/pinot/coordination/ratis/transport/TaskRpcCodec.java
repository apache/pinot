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

import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.AttemptState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.AttemptStatus;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Input;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Progress;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.QueueKey;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.QueueState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.TaskState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.TaskStatus;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;


/// Typed controller read/feed projections for task ledgers. This codec grants no MINION identity or mutation API.
final class TaskRpcCodec {
  private TaskRpcCodec() {
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
    return MetadataRpc.TaskInput.newBuilder().setQueue(key(input.queue())).setMinionId(input.minionId())
        .putAllConfigs(input.configs()).setMaxAttempts(input.maxAttempts()).build();
  }

  private static Input input(MetadataRpc.TaskInput input) {
    require(input.hasQueue() && !input.getMinionId().isBlank() && input.getMinionId().length() <= 128
        && input.getMaxAttempts() > 0 && input.getMaxAttempts() <= TaskProtocol.MAX_ATTEMPTS
        && input.getConfigsCount() <= TaskProtocol.MAX_CONFIGS, "Invalid task input projection");
    input.getConfigsMap().forEach((key, value) -> require(
        !key.isBlank() && key.length() <= 256 && value.length() <= 16_384,
        "Task configuration exceeds supported bounds"));
    return new Input(key(input.getQueue()), input.getMinionId(), input.getConfigsMap(), input.getMaxAttempts());
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
