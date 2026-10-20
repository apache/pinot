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
package org.apache.pinot.coordination.ratis.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.AttemptState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.AttemptStatus;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Cancel;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Checkpoint;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Claim;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Complete;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.ControllerOperation;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Input;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Progress;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.QueueKey;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.QueueState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Retry;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.SetQueuePaused;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Submit;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.TaskState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.TaskStatus;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.WorkerOperation;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Deterministic task execution staging over one committed view. No scheduler, plugin I/O or output publication runs
/// here. Every returned record is committed atomically with its durable request outcome by [MetadataCatalog].
final class TaskCatalog {
  private TaskCatalog() {
  }

  interface View extends OwnershipCatalog.View {
    CatalogRecord record(UUID objectId);

    default CatalogRecord retainedRecord(ObjectVersion version) {
      CatalogRecord current = record(version.objectId());
      return current != null && current.version() == version.version() ? current : null;
    }
  }

  static List<CatalogRecord> stage(ControllerOperation operation, View view, long index) throws IOException {
    require(operation != null && index > 0, Status.INVALID_REQUEST, "Task operation and committed index required");
    return switch (operation) {
      case Submit submit -> submit(submit, view, index);
      case SetQueuePaused pause -> pause(pause, view, index);
      case Cancel cancel -> cancel(cancel, view, index);
      case Retry retry -> retry(retry, view, index);
    };
  }

  static List<CatalogRecord> stage(WorkerOperation operation, ParticipantToken minion, View view, long index)
      throws IOException {
    require(operation != null && index > 0, Status.INVALID_REQUEST, "Task operation and committed index required");
    validateSession(minion, view);
    return switch (operation) {
      case Claim claim -> claim(claim, minion, view, index);
      case Checkpoint checkpoint -> checkpoint(checkpoint, minion, view, index);
      case Complete complete -> complete(complete, minion, view, index);
    };
  }

  static void validateSession(ParticipantToken minion, View view) {
    validateMinionToken(minion, view.historyId());
    require(currentSession(minion, view), Status.FENCED, "Minion session is replaced, retired or expired");
  }

  private static boolean currentSession(ParticipantToken minion, View view) {
    ParticipantState current = view.participant(minion.participantId());
    ControlState control = view.control();
    return current != null && current.participant().equals(minion) && control != null
        && control.controlEpoch() != null && control.policy() != null && !current.retired()
        && current.expiryRound() > control.round();
  }

  private static List<CatalogRecord> submit(Submit request, View view, long index) throws IOException {
    require(request.taskId() != null, Status.INVALID_REQUEST, "Task identity is required");
    validateInput(request.input());
    QueueKey key = request.input().queue();
    OwnershipCatalog.validateTableOwner(request.owner(), key.tableName(), view);
    CatalogRecord table = currentTable(key, view);
    CatalogRecord schema = view.record(RecordKind.SCHEMA, TableNameBuilder.extractRawTableName(key.tableName()));
    require(schema != null && !schema.deleted(), Status.NOT_FOUND, "Task schema is absent");
    ParticipantState assigned = view.participant(request.input().minionId());
    require(assigned != null && assigned.participant().role() == Role.MINION, Status.NOT_FOUND,
        "Task assignment requires a known minion identity");
    String name = TaskProtocol.taskName(request.taskId());
    require(view.record(RecordKind.TASK, name) == null
        && view.record(TaskProtocol.objectId(view.historyId(), RecordKind.TASK, name)) == null, Status.CONFLICT,
        "Task identities cannot be reused");
    require(request.dependencies() != null && request.dependencies().size() <= TaskProtocol.MAX_DEPENDENCIES - 2,
        Status.INVALID_REQUEST, "Task input dependency bound exceeded");
    Map<UUID, ObjectVersion> dependencies = new LinkedHashMap<>();
    dependencies.put(table.objectId(), version(view, table));
    dependencies.put(schema.objectId(), version(view, schema));
    for (ObjectVersion dependency : request.dependencies()) {
      validateVersion(dependency, view.historyId());
      CatalogRecord current = view.record(dependency.objectId());
      require(current != null && supportedDependency(current.kind()), Status.INVALID_REQUEST,
          "Task input dependency has an unsupported record kind");
      ObjectVersion previous = dependencies.putIfAbsent(dependency.objectId(), dependency);
      require(previous == null || previous.equals(dependency), Status.CONFLICT, "Task input versions differ");
    }
    List<ObjectVersion> captured = List.copyOf(dependencies.values());
    currentDependencies(captured, view);
    List<CatalogRecord> records = new ArrayList<>();
    if (view.record(RecordKind.TASK_QUEUE, TaskProtocol.queueName(key)) == null) {
      records.add(queueRecord(new QueueState(view.historyId(), key, false, request.owner()), 1, index));
    }
    records.add(taskRecord(new TaskState(view.historyId(), request.taskId(), request.input(), captured,
        request.owner(), TaskStatus.READY, 0), 1, index));
    return records;
  }

  private static List<CatalogRecord> pause(SetQueuePaused request, View view, long index) throws IOException {
    validateQueue(request.queue());
    OwnershipCatalog.validateTableOwner(request.owner(), request.queue().tableName(), view);
    currentTable(request.queue(), view);
    CatalogRecord previous = view.record(RecordKind.TASK_QUEUE, TaskProtocol.queueName(request.queue()));
    require(request.expectedVersion() >= 0
        && (previous == null ? request.expectedVersion() == 0 : previous.version() == request.expectedVersion()),
        Status.CONFLICT, "Task queue version changed");
    return List.of(queueRecord(new QueueState(view.historyId(), request.queue(), request.paused(), request.owner()),
        nextVersion(previous), index));
  }

  private static List<CatalogRecord> cancel(Cancel request, View view, long index) throws IOException {
    detail(request.detail());
    CatalogRecord previous = currentTask(request.taskId(), request.expectedVersion(), view);
    TaskState task = TaskProtocol.decodeTask(previous.value());
    OwnershipCatalog.validateTableOwner(request.owner(), task.input().queue().tableName(), view);
    require(task.status() != TaskStatus.CANCELLED && task.status() != TaskStatus.FATAL_FAILED, Status.CONFLICT,
        "Task is already cancelled or fatally failed");
    List<CatalogRecord> records = new ArrayList<>();
    if (task.status() == TaskStatus.RUNNING) {
      CatalogRecord attempt = attempt(task, view);
      AttemptState state = TaskProtocol.decodeAttempt(attempt.value());
      records.add(attemptRecord(new AttemptState(state.historyId(), state.taskId(), state.attempt(),
          state.participantAtClaim(), state.participantVersion(),
          state.claimedIndex(), AttemptStatus.FENCED,
          new Progress(state.progress().completedUnits(), request.detail())),
          nextVersion(attempt), index));
    }
    records.add(taskRecord(new TaskState(task.historyId(), task.taskId(), task.input(), task.dependencies(),
        request.owner(), TaskStatus.CANCELLED, task.attempt()), nextVersion(previous), index));
    return records;
  }

  private static List<CatalogRecord> retry(Retry request, View view, long index) throws IOException {
    CatalogRecord previous = currentTask(request.taskId(), request.expectedVersion(), view);
    TaskState task = TaskProtocol.decodeTask(previous.value());
    OwnershipCatalog.validateTableOwner(request.owner(), task.input().queue().tableName(), view);
    require(task.attempt() < task.input().maxAttempts(), Status.CONFLICT, "Task attempt budget exhausted");
    currentDependencies(task.dependencies(), view);
    List<CatalogRecord> records = new ArrayList<>();
    if (task.status() == TaskStatus.RUNNING) {
      CatalogRecord attempt = attempt(task, view);
      AttemptState state = TaskProtocol.decodeAttempt(attempt.value());
      require(!currentSession(state.minion(), view), Status.CONFLICT, "A live task attempt cannot be retried");
      records.add(attemptRecord(new AttemptState(state.historyId(), state.taskId(), state.attempt(),
          state.participantAtClaim(), state.participantVersion(),
          state.claimedIndex(), AttemptStatus.FENCED, state.progress()), nextVersion(attempt), index));
    } else {
      require(task.status() == TaskStatus.RETRYABLE_FAILED, Status.CONFLICT, "Task has no retryable attempt");
    }
    records.add(taskRecord(new TaskState(task.historyId(), task.taskId(), task.input(), task.dependencies(),
        request.owner(), TaskStatus.READY, task.attempt()), nextVersion(previous), index));
    return records;
  }

  private static List<CatalogRecord> claim(Claim request, ParticipantToken minion, View view, long index)
      throws IOException {
    ParticipantState participant = view.participant(minion.participantId());
    require(participant.ready() && !participant.draining(), Status.FENCED, "Minion is unready or draining");
    CatalogRecord previous = currentTask(request.taskId(), request.expectedVersion(), view);
    TaskState task = TaskProtocol.decodeTask(previous.value());
    require(task.status() == TaskStatus.READY && task.attempt() < task.input().maxAttempts(), Status.CONFLICT,
        "Task is not ready or its attempt budget is exhausted");
    require(task.input().minionId().equals(minion.participantId()), Status.FENCED,
        "Task is assigned to another minion identity");
    CatalogRecord queue = view.record(RecordKind.TASK_QUEUE, TaskProtocol.queueName(task.input().queue()));
    require(queue != null && !TaskProtocol.decodeQueue(queue.value()).paused(), Status.NOT_READY,
        "Task queue is absent or paused");
    currentDependencies(task.dependencies(), view);
    long number = task.attempt() + 1;
    String name = TaskProtocol.attemptName(task.taskId(), number);
    require(view.record(RecordKind.TASK_ATTEMPT, name) == null
        && view.record(TaskProtocol.objectId(view.historyId(), RecordKind.TASK_ATTEMPT, name)) == null,
        Status.CONFLICT, "Task attempt identity cannot be reused");
    TaskState running = new TaskState(task.historyId(), task.taskId(), task.input(), task.dependencies(),
        task.controller(), TaskStatus.RUNNING, number);
    CatalogRecord participantRecord = view.record(RecordKind.PARTICIPANT, minion.participantId());
    require(participantRecord != null, Status.NOT_READY, "Minion registration record is absent");
    AttemptState attempt = new AttemptState(task.historyId(), task.taskId(), number, participant,
        version(view, participantRecord), index, AttemptStatus.RUNNING, new Progress(0, ""));
    return List.of(taskRecord(running, nextVersion(previous), index), attemptRecord(attempt, 1, index));
  }

  private static List<CatalogRecord> checkpoint(Checkpoint request, ParticipantToken minion, View view, long index)
      throws IOException {
    return report(request, request.attempt(), request.progress(), AttemptStatus.RUNNING, minion, view, index);
  }

  private static List<CatalogRecord> complete(Complete request, ParticipantToken minion, View view, long index)
      throws IOException {
    require(request.result() == AttemptStatus.EXECUTION_FINISHED || request.result() == AttemptStatus.RETRYABLE_FAILED
        || request.result() == AttemptStatus.FATAL_FAILED, Status.INVALID_REQUEST,
        "Completion reports execution only; output publication is unsupported");
    return report(request, request.attempt(), request.progress(), request.result(), minion, view, index);
  }

  private static List<CatalogRecord> report(WorkerOperation request, long number, Progress progress,
      AttemptStatus result, ParticipantToken minion, View view, long index) throws IOException {
    validateProgress(progress);
    CatalogRecord previous = currentTask(request.taskId(), request.expectedVersion(), view);
    TaskState task = TaskProtocol.decodeTask(previous.value());
    require(task.status() == TaskStatus.RUNNING && task.attempt() == number, Status.FENCED,
        "Task attempt is no longer current and running");
    CatalogRecord attempt = attempt(task, view);
    AttemptState state = TaskProtocol.decodeAttempt(attempt.value());
    require(state.status() == AttemptStatus.RUNNING && state.minion().equals(minion), Status.FENCED,
        "Task attempt belongs to another minion session");
    require(progress.completedUnits() >= state.progress().completedUnits(), Status.CONFLICT,
        "Task progress cannot move backwards");
    currentDependencies(task.dependencies(), view);
    TaskStatus taskStatus = switch (result) {
      case RUNNING -> TaskStatus.RUNNING;
      case EXECUTION_FINISHED -> TaskStatus.EXECUTION_FINISHED;
      case RETRYABLE_FAILED -> TaskStatus.RETRYABLE_FAILED;
      case FATAL_FAILED -> TaskStatus.FATAL_FAILED;
      case FENCED -> throw new CoordinationRejection(Status.INVALID_REQUEST, "Worker cannot fence task attempts");
    };
    return List.of(taskRecord(new TaskState(task.historyId(), task.taskId(), task.input(), task.dependencies(),
        task.controller(), taskStatus, task.attempt()), nextVersion(previous), index),
        attemptRecord(new AttemptState(state.historyId(), state.taskId(), state.attempt(), state.participantAtClaim(),
            state.participantVersion(), state.claimedIndex(), result, progress), nextVersion(attempt), index));
  }

  private static CatalogRecord currentTask(UUID taskId, long expectedVersion, View view) {
    require(taskId != null && expectedVersion > 0, Status.INVALID_REQUEST, "Task identity and version are required");
    CatalogRecord record = view.record(RecordKind.TASK, TaskProtocol.taskName(taskId));
    require(record != null, Status.NOT_FOUND, "Task is absent");
    require(!record.deleted() && record.version() == expectedVersion, Status.CONFLICT, "Task version changed");
    return record;
  }

  private static CatalogRecord attempt(TaskState task, View view) {
    CatalogRecord record =
        view.record(RecordKind.TASK_ATTEMPT, TaskProtocol.attemptName(task.taskId(), task.attempt()));
    require(record != null && !record.deleted(), Status.CONFLICT, "Current task attempt is absent");
    return record;
  }

  private static CatalogRecord currentTable(QueueKey key, View view) {
    CatalogRecord table = view.record(RecordKind.TABLE_CONFIG, key.tableName());
    require(table != null && !table.deleted() && key.tableObjectId().equals(table.objectId()), Status.CONFLICT,
        "Task table incarnation changed or was deleted");
    return table;
  }

  private static void currentDependencies(List<ObjectVersion> dependencies, View view) {
    for (ObjectVersion dependency : dependencies) {
      CatalogRecord current = view.record(dependency.objectId());
      require(current != null && !current.deleted() && current.version() == dependency.version(), Status.CONFLICT,
          "Captured task input changed or was deleted");
    }
  }

  private static boolean supportedDependency(RecordKind kind) {
    return kind == RecordKind.TABLE_CONFIG || kind == RecordKind.SCHEMA || kind == RecordKind.CLUSTER_CONFIG
        || kind == RecordKind.OFFLINE_SEGMENT_CONTENT;
  }

  static void validateQueue(QueueKey key) {
    require(key != null && key.tableObjectId() != null, Status.INVALID_REQUEST, "Task queue identity is required");
    OwnershipCatalog.identifier(key.tableName(), 1024);
    require(TableNameBuilder.isTableResource(key.tableName()), Status.INVALID_REQUEST,
        "Task queue requires a canonical typed table identity");
    OwnershipCatalog.identifier(key.taskType(), 128);
    require(key.taskType().matches("[A-Za-z0-9][A-Za-z0-9_.-]*"), Status.INVALID_REQUEST,
        "Task type contains unsupported characters");
  }

  private static void validateInput(Input input) {
    require(input != null, Status.INVALID_REQUEST, "Task input is required");
    validateQueue(input.queue());
    OwnershipCatalog.identifier(input.minionId(), 128);
    require(input.maxAttempts() > 0 && input.maxAttempts() <= TaskProtocol.MAX_ATTEMPTS, Status.INVALID_REQUEST,
        "Task attempt limit is outside the supported bounds");
    require(input.configs() != null && input.configs().size() <= TaskProtocol.MAX_CONFIGS, Status.INVALID_REQUEST,
        "Task configuration bound exceeded");
    input.configs().forEach((key, value) -> {
      OwnershipCatalog.identifier(key, 256);
      require(value != null && value.length() <= 16_384, Status.INVALID_REQUEST,
          "Task configuration value is absent or too long");
    });
  }

  private static void validateMinionToken(ParticipantToken token, UUID history) {
    require(token != null && history.equals(token.historyId()), Status.HISTORY_MISMATCH,
        "Minion participant history differs");
    OwnershipCatalog.identifier(token.participantId(), 128);
    require(token.role() == Role.MINION && token.incarnation() != null && token.registrationEpoch() > 0,
        Status.FENCED, "Task execution requires a complete MINION participant token");
  }

  private static void validateVersion(ObjectVersion version, UUID history) {
    require(version != null && history.equals(version.historyId()), Status.HISTORY_MISMATCH,
        "Task dependency history differs");
    require(version.objectId() != null && version.version() > 0, Status.INVALID_REQUEST,
        "Task dependency requires an object identity and positive version");
  }

  private static void validateProgress(Progress progress) {
    require(progress != null && progress.completedUnits() >= 0, Status.INVALID_REQUEST, "Invalid task progress");
    detail(progress.detail());
  }

  private static void detail(String value) {
    require(value != null && value.length() <= TaskProtocol.MAX_DETAIL_LENGTH, Status.INVALID_REQUEST,
        "Task detail is absent or too long");
  }

  static void validateRecord(CatalogRecord record, UUID history, long snapshotIndex) throws IOException {
    require(record != null && taskKind(record.kind()) && !record.deleted() && record.value() != null
        && record.version() > 0 && record.schemaVersion() == MetadataProtocol.FORMAT_VERSION
        && record.lastModifiedIndex() > 0 && record.lastModifiedIndex() <= snapshotIndex
        && TaskProtocol.objectId(history, record.kind(), record.name()).equals(record.objectId()),
        Status.INVALID_REQUEST, "Invalid task record envelope");
    switch (record.kind()) {
      case TASK_QUEUE -> {
        QueueState queue = TaskProtocol.decodeQueue(record.value());
        require(history.equals(queue.historyId()), Status.HISTORY_MISMATCH, "Task queue history differs");
        validateQueue(queue.queue());
        require(TaskProtocol.queueName(queue.queue()).equals(record.name()), Status.INVALID_REQUEST,
            "Task queue name differs");
        OwnershipCatalog.validateStoredTableOwnerShape(queue.controller(), queue.queue().tableName(), history);
      }
      case TASK -> {
        TaskState task = TaskProtocol.decodeTask(record.value());
        require(history.equals(task.historyId()) && task.taskId() != null, Status.HISTORY_MISMATCH,
            "Task history or identity differs");
        require(TaskProtocol.taskName(task.taskId()).equals(record.name()), Status.INVALID_REQUEST,
            "Task name differs");
        validateInput(task.input());
        OwnershipCatalog.validateStoredTableOwnerShape(task.controller(), task.input().queue().tableName(), history);
        require(task.status() != null && task.attempt() >= 0 && task.attempt() <= task.input().maxAttempts()
            && (task.attempt() > 0 || task.status() == TaskStatus.READY || task.status() == TaskStatus.CANCELLED),
            Status.INVALID_REQUEST, "Invalid task state or attempt");
        require(task.dependencies() != null && task.dependencies().size() >= 2
            && task.dependencies().size() <= TaskProtocol.MAX_DEPENDENCIES, Status.INVALID_REQUEST,
            "Task dependency bound exceeded");
        Set<UUID> identities = new HashSet<>();
        for (ObjectVersion dependency : task.dependencies()) {
          validateVersion(dependency, history);
          require(identities.add(dependency.objectId()), Status.INVALID_REQUEST, "Duplicate task dependency");
        }
        require(identities.contains(task.input().queue().tableObjectId()), Status.INVALID_REQUEST,
            "Task omits its target table dependency");
      }
      case TASK_ATTEMPT -> {
        AttemptState attempt = TaskProtocol.decodeAttempt(record.value());
        require(history.equals(attempt.historyId()) && attempt.taskId() != null && attempt.attempt() > 0
            && attempt.attempt() <= TaskProtocol.MAX_ATTEMPTS && attempt.status() != null, Status.INVALID_REQUEST,
            "Invalid task attempt identity or state");
        require(TaskProtocol.attemptName(attempt.taskId(), attempt.attempt()).equals(record.name()),
            Status.INVALID_REQUEST, "Task attempt name differs");
        validateMinionToken(attempt.minion(), history);
        validateVersion(attempt.participantVersion(), history);
        require(attempt.claimedIndex() > 0 && attempt.claimedIndex() <= record.lastModifiedIndex(),
            Status.INVALID_REQUEST, "Task claim index is absent or in the future");
        require(record.version() > 1 || attempt.claimedIndex() == record.lastModifiedIndex(), Status.INVALID_REQUEST,
            "Initial task attempt must be created at its claim index");
        MembershipState.validateParticipant(attempt.participantAtClaim(), history, attempt.claimedIndex());
        require(attempt.participantAtClaim().ready() && !attempt.participantAtClaim().draining()
            && !attempt.participantAtClaim().retired(), Status.INVALID_REQUEST,
            "Task claim captured an ineligible minion registration");
        validateProgress(attempt.progress());
      }
      default -> throw new CoordinationRejection(Status.INVALID_REQUEST, "Unsupported task record kind");
    }
  }

  /// Validate current and historical journal records without restoring historical participant authority.
  static void validateReferences(CatalogRecord record, View view) throws IOException {
    switch (record.kind()) {
      case TASK_QUEUE -> {
        QueueState state = TaskProtocol.decodeQueue(record.value());
        storedTable(state.queue(), view);
        OwnershipCatalog.validateStoredTableOwner(state.controller(), state.queue().tableName(), view);
      }
      case TASK -> validateTaskReferences(record, view);
      case TASK_ATTEMPT -> validateAttemptReferences(record, view);
      default -> throw new CoordinationRejection(Status.INVALID_REQUEST, "Unsupported task reference kind");
    }
  }

  private static void validateTaskReferences(CatalogRecord record, View view) throws IOException {
    TaskState state = TaskProtocol.decodeTask(record.value());
    storedTable(state.input().queue(), view);
    OwnershipCatalog.validateStoredTableOwner(state.controller(), state.input().queue().tableName(), view);
    CatalogRecord currentRecord = view.record(record.objectId());
    TaskState current = TaskProtocol.decodeTask(currentRecord.value());
    require(current.input().equals(state.input()) && current.dependencies().equals(state.dependencies())
        && current.attempt() >= state.attempt(), Status.INVALID_REQUEST,
        "Task immutable inputs or attempt history differ");
    CatalogRecord queue = view.record(RecordKind.TASK_QUEUE, TaskProtocol.queueName(state.input().queue()));
    require(queue != null && !queue.deleted()
        && TaskProtocol.decodeQueue(queue.value()).queue().equals(state.input().queue()), Status.INVALID_REQUEST,
        "Task queue is absent or differs");
    boolean schemaFound = false;
    for (ObjectVersion dependency : state.dependencies()) {
      CatalogRecord referenced = view.record(dependency.objectId());
      require(referenced != null && supportedDependency(referenced.kind())
          && referenced.version() >= dependency.version()
          && (referenced.version() > dependency.version()
              || referenced.lastModifiedIndex() <= record.lastModifiedIndex()),
          Status.INVALID_REQUEST, "Task references absent, unsupported or future input metadata");
      schemaFound |= referenced.kind() == RecordKind.SCHEMA
          && referenced.name().equals(TableNameBuilder.extractRawTableName(state.input().queue().tableName()));
    }
    require(schemaFound, Status.INVALID_REQUEST, "Task omits its table schema dependency");
    for (long number = 1; number <= state.attempt(); number++) {
      CatalogRecord attemptRecord =
          view.record(RecordKind.TASK_ATTEMPT, TaskProtocol.attemptName(state.taskId(), number));
      require(attemptRecord != null && !attemptRecord.deleted(), Status.INVALID_REQUEST,
          "Task attempt history has a gap");
      AttemptState attempt = TaskProtocol.decodeAttempt(attemptRecord.value());
      require(number == current.attempt() || attempt.status() != AttemptStatus.RUNNING, Status.INVALID_REQUEST,
          "A superseded task attempt remains running");
      if (record.equals(currentRecord) && number == state.attempt()) {
        require(statusMatches(state.status(), attempt.status()), Status.INVALID_REQUEST,
            "Current task and attempt states disagree");
      }
    }
  }

  private static boolean statusMatches(TaskStatus task, AttemptStatus attempt) {
    return switch (task) {
      case READY -> attempt == AttemptStatus.RETRYABLE_FAILED || attempt == AttemptStatus.FENCED;
      case RUNNING -> attempt == AttemptStatus.RUNNING;
      case EXECUTION_FINISHED -> attempt == AttemptStatus.EXECUTION_FINISHED;
      case RETRYABLE_FAILED -> attempt == AttemptStatus.RETRYABLE_FAILED;
      case FATAL_FAILED -> attempt == AttemptStatus.FATAL_FAILED;
      case CANCELLED -> attempt != AttemptStatus.RUNNING;
    };
  }

  private static void validateAttemptReferences(CatalogRecord record, View view) throws IOException {
    AttemptState state = TaskProtocol.decodeAttempt(record.value());
    CatalogRecord taskRecord = view.record(RecordKind.TASK, TaskProtocol.taskName(state.taskId()));
    require(taskRecord != null && !taskRecord.deleted(), Status.INVALID_REQUEST, "Attempt task is absent");
    TaskState task = TaskProtocol.decodeTask(taskRecord.value());
    require(task.attempt() >= state.attempt() && task.input().minionId().equals(state.minion().participantId()),
        Status.INVALID_REQUEST, "Task attempt exceeds its task or assignment");
    ParticipantState participant = view.participant(state.minion().participantId());
    require(participant != null && participant.participant().role() == Role.MINION
        && participant.participant().registrationEpoch() >= state.minion().registrationEpoch()
        && (participant.participant().registrationEpoch() > state.minion().registrationEpoch()
            || participant.participant().equals(state.minion())), Status.INVALID_REQUEST,
        "Task attempt references an absent or future participant session");
    CatalogRecord registration = view.record(RecordKind.PARTICIPANT, state.minion().participantId());
    require(registration != null && registration.objectId().equals(state.participantVersion().objectId())
        && registration.version() >= state.participantVersion().version(), Status.INVALID_REQUEST,
        "Task attempt references an absent or future participant record version");
    CatalogRecord captured = view.retainedRecord(state.participantVersion());
    if (captured != null) {
      require(MembershipProtocol.decodeParticipant(captured.value()).equals(state.participantAtClaim())
          && captured.lastModifiedIndex() <= state.claimedIndex(), Status.INVALID_REQUEST,
          "Task attempt precedes or changes its participant registration");
    }
    AttemptState current = TaskProtocol.decodeAttempt(view.record(record.objectId()).value());
    require(current.participantAtClaim().equals(state.participantAtClaim())
        && current.participantVersion().equals(state.participantVersion())
        && current.claimedIndex() == state.claimedIndex()
        && current.progress().completedUnits() >= state.progress().completedUnits()
        && (state.status() == AttemptStatus.RUNNING || state.status() == current.status()), Status.INVALID_REQUEST,
        "Task attempt identity, progress or terminal outcome changed");
  }

  private static void storedTable(QueueKey key, View view) {
    CatalogRecord table = view.record(key.tableObjectId());
    require(table != null && table.kind() == RecordKind.TABLE_CONFIG && table.name().equals(key.tableName()),
        Status.INVALID_REQUEST, "Task references an absent or different table incarnation");
  }

  static boolean taskKind(RecordKind kind) {
    return kind == RecordKind.TASK_QUEUE || kind == RecordKind.TASK || kind == RecordKind.TASK_ATTEMPT;
  }

  private static ObjectVersion version(View view, CatalogRecord record) {
    return new ObjectVersion(view.historyId(), record.objectId(), record.version());
  }

  private static long nextVersion(CatalogRecord previous) {
    require(previous == null || previous.version() < Long.MAX_VALUE, Status.LIMIT_EXCEEDED,
        "Task record version exhausted");
    return previous == null ? 1 : previous.version() + 1;
  }

  private static CatalogRecord queueRecord(QueueState state, long version, long index) {
    return record(RecordKind.TASK_QUEUE, TaskProtocol.queueName(state.queue()), state.historyId(), state, version,
        index);
  }

  private static CatalogRecord taskRecord(TaskState state, long version, long index) {
    return record(RecordKind.TASK, TaskProtocol.taskName(state.taskId()), state.historyId(), state, version, index);
  }

  private static CatalogRecord attemptRecord(AttemptState state, long version, long index) {
    return record(RecordKind.TASK_ATTEMPT, TaskProtocol.attemptName(state.taskId(), state.attempt()), state.historyId(),
        state, version, index);
  }

  private static CatalogRecord record(RecordKind kind, String name, UUID history, Object value, long version,
      long index) {
    return new CatalogRecord(kind, name, TaskProtocol.objectId(history, kind, name), version,
        MetadataProtocol.FORMAT_VERSION, index, false, MetadataProtocol.toTree(value));
  }

  private static void require(boolean condition, Status status, String detail) {
    if (!condition) {
      throw new CoordinationRejection(status, detail);
    }
  }
}
