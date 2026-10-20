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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.pinot.common.coordination.NativeTaskOutputIdentity;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.OwnerToken;


/// Durable task scheduling, execution and immutable OFFLINE output publication. Legacy plugins are not enabled.
/// Participant tokens identify sessions; a future authenticated transport must derive them from its caller identity.
public final class TaskProtocol {
  public static final int MAX_ATTEMPTS = 16;
  public static final int MAX_DEPENDENCIES = 32;
  public static final int MAX_CONFIGS = 128;
  public static final int MAX_DETAIL_LENGTH = 1024;
  public static final int MAX_OUTPUTS = 16;
  public static final int MAX_OCCURRENCES = 1024;
  public static final int MAX_SNAPSHOT_RECORDS = 128;
  public static final int MIN_SNAPSHOT_BYTES = 1024;
  public static final int MAX_SNAPSHOT_BYTES = 2 * 1024 * 1024;
  public static final String NATIVE_SEGMENT_GENERATION_TASK = "NativeSegmentGenerationTask";

  private TaskProtocol() {
  }

  /// EXECUTION_FINISHED records worker execution only. It does not certify output publication or task success.
  public enum TaskStatus {
    READY, RUNNING, EXECUTION_FINISHED, RETRYABLE_FAILED, FATAL_FAILED, CANCELLED, SUCCEEDED
  }

  /// FENCED revokes further reports. It does not assert that a cancelled or unavailable worker stopped executing.
  public enum AttemptStatus {
    RUNNING, EXECUTION_FINISHED, RETRYABLE_FAILED, FATAL_FAILED, FENCED, SUCCEEDED
  }

  public record QueueKey(String tableName, UUID tableObjectId, String taskType) {
  }

  /// Explicit assignment remains independent of plugin discovery. Only an explicit typed output policy grants
  /// publication authority; old inputs remain readable and cannot publish outputs.
  public record Input(QueueKey queue, String minionId, Map<String, String> configs, int maxAttempts,
                      @Nullable @JsonInclude(JsonInclude.Include.NON_NULL) OutputPolicy outputPolicy) {
    public Input {
      configs = configs == null ? null : Map.copyOf(configs);
    }

    public Input(QueueKey queue, String minionId, Map<String, String> configs, int maxAttempts) {
      this(queue, minionId, configs, maxAttempts, null);
    }

    /// Preserve replay of pre-publication commands without weakening missing-field validation for other records.
    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public static Input fromJson(JsonNode value) throws IOException {
      Set<String> fields = Set.of("queue", "minionId", "configs", "maxAttempts", "outputPolicy");
      if (!value.isObject() || !value.has("queue") || !value.has("minionId") || !value.has("configs")
          || (!value.path("minionId").isTextual() && !value.path("minionId").isNull())
          || (!value.path("configs").isObject() && !value.path("configs").isNull())
          || !value.path("maxAttempts").isIntegralNumber()
          || !value.path("maxAttempts").canConvertToInt()) {
        throw new IOException("Invalid task input fields");
      }
      Map<String, String> configs = new HashMap<>();
      for (var entry : value.properties()) {
        if (!fields.contains(entry.getKey())) {
          throw new IOException("Unsupported task input field");
        }
      }
      for (var entry : value.path("configs").properties()) {
        if (!entry.getValue().isTextual()) {
          throw new IOException("Task config must be a string");
        }
        configs.put(entry.getKey(), entry.getValue().textValue());
      }
      JsonNode policy = value.get("outputPolicy");
      return new Input(MetadataProtocol.fromTree(value.get("queue"), QueueKey.class),
          value.get("minionId").textValue(), value.get("configs").isNull() ? null : configs,
          value.get("maxAttempts").intValue(),
          policy == null || policy.isNull() ? null : MetadataProtocol.fromTree(policy, OutputPolicy.class));
    }
  }

  /// Controller-approved immutable artifact namespace. Local workers separately enforce their configured I/O roots.
  public record OutputPolicy(String rootUri, int maxOutputs) {
  }

  /// A schedule identity is its immutable configuration generation. One FireSchedule creates at most one occurrence;
  /// finite CATCH_UP scheduling uses committed membership rounds and never wall-clock values supplied by a worker.
  public record ScheduleState(UUID historyId, UUID scheduleId, Input input, List<ObjectVersion> dependencies,
                              OwnerToken controller, long firstRound, long intervalRounds, long maxOccurrences,
                              long firedOccurrences, long policyVersion) {
    public ScheduleState {
      dependencies = dependencies == null ? null : List.copyOf(dependencies);
    }
  }

  /// The manifest and successful terminal task/attempt records share one committed transaction with these contents.
  /// It certifies catalog publication, not installed replicas, serving eligibility or physical file reclamation.
  public record PublicationState(UUID historyId, UUID taskId, long attempt, ParticipantToken minion,
                                 List<Content> outputs, long committedIndex) {
    public PublicationState {
      outputs = outputs == null ? null : List.copyOf(outputs);
    }
  }

  public record WorkSnapshot(Status status, Revision revision, String detail, ParticipantState participant,
                             List<CatalogRecord> records, ControlState control) {
    public WorkSnapshot {
      records = List.copyOf(records);
    }
  }

  public record QueueState(UUID historyId, QueueKey queue, boolean paused, OwnerToken controller) {
  }

  public record TaskState(UUID historyId, UUID taskId, Input input, List<ObjectVersion> dependencies,
                          OwnerToken controller, TaskStatus status, long attempt) {
    public TaskState {
      dependencies = dependencies == null ? null : List.copyOf(dependencies);
    }
  }

  public record Progress(long completedUnits, String detail) {
  }

  /// The captured participant record and claim index prevent a later registration from being backdated on restore.
  public record AttemptState(UUID historyId, UUID taskId, long attempt, ParticipantState participantAtClaim,
                             ObjectVersion participantVersion, long claimedIndex, AttemptStatus status,
                             Progress progress) {
    public ParticipantToken minion() {
      return participantAtClaim == null ? null : participantAtClaim.participant();
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "operation")
  @JsonSubTypes({@JsonSubTypes.Type(value = Submit.class, name = "SUBMIT"),
      @JsonSubTypes.Type(value = SetQueuePaused.class, name = "SET_QUEUE_PAUSED"),
      @JsonSubTypes.Type(value = Cancel.class, name = "CANCEL"),
      @JsonSubTypes.Type(value = Retry.class, name = "RETRY"),
      @JsonSubTypes.Type(value = CreateSchedule.class, name = "CREATE_SCHEDULE"),
      @JsonSubTypes.Type(value = FireSchedule.class, name = "FIRE_SCHEDULE")})
  public sealed interface ControllerOperation
      permits Submit, SetQueuePaused, Cancel, Retry, CreateSchedule, FireSchedule {
    OwnerToken owner();
  }

  /// Captures current table and schema versions in addition to these explicit input dependencies.
  public record Submit(OwnerToken owner, UUID taskId, Input input, List<ObjectVersion> dependencies)
      implements ControllerOperation {
    public Submit {
      dependencies = dependencies == null ? null : List.copyOf(dependencies);
    }
  }

  public record CreateSchedule(OwnerToken owner, UUID scheduleId, Input input, List<ObjectVersion> dependencies,
                               long firstRound, long intervalRounds, long maxOccurrences)
      implements ControllerOperation {
    public CreateSchedule {
      dependencies = dependencies == null ? null : List.copyOf(dependencies);
    }
  }

  public record FireSchedule(OwnerToken owner, UUID scheduleId, long expectedVersion)
      implements ControllerOperation {
  }

  /// Version zero creates the queue; existing queues require their exact current version.
  public record SetQueuePaused(OwnerToken owner, QueueKey queue, long expectedVersion, boolean paused)
      implements ControllerOperation {
  }

  public record Cancel(OwnerToken owner, UUID taskId, long expectedVersion, String detail)
      implements ControllerOperation {
  }

  /// Retry is permitted after a retryable result or after the previous running session expires or is fenced.
  /// It never changes captured inputs, assignment identity or the attempt budget.
  public record Retry(OwnerToken owner, UUID taskId, long expectedVersion) implements ControllerOperation {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "operation")
  @JsonSubTypes({@JsonSubTypes.Type(value = Claim.class, name = "CLAIM"),
      @JsonSubTypes.Type(value = Checkpoint.class, name = "CHECKPOINT"),
      @JsonSubTypes.Type(value = Complete.class, name = "COMPLETE"),
      @JsonSubTypes.Type(value = PublishOfflineOutputs.class, name = "PUBLISH_OFFLINE_OUTPUTS")})
  public sealed interface WorkerOperation permits Claim, Checkpoint, Complete, PublishOfflineOutputs {
    UUID taskId();

    long expectedVersion();
  }

  public record Claim(UUID taskId, long expectedVersion) implements WorkerOperation {
  }

  public record Checkpoint(UUID taskId, long expectedVersion, long attempt, Progress progress)
      implements WorkerOperation {
  }

  /// Only execution-finished, retryable-failed and fatal-failed reports are supported. No domain effects are accepted.
  public record Complete(UUID taskId, long expectedVersion, long attempt, AttemptStatus result, Progress progress)
      implements WorkerOperation {
  }

  public record PublishOfflineOutputs(UUID taskId, long expectedVersion, long attempt, List<Content> outputs,
                                      Progress progress) implements WorkerOperation {
    public PublishOfflineOutputs {
      outputs = outputs == null ? null : List.copyOf(outputs);
    }
  }

  public static ScheduleState decodeSchedule(JsonNode value) throws IOException {
    return MetadataProtocol.fromTree(value, ScheduleState.class);
  }

  public static PublicationState decodePublication(JsonNode value) throws IOException {
    return MetadataProtocol.fromTree(value, PublicationState.class);
  }

  public static UUID occurrenceTaskId(UUID historyId, UUID scheduleId, long occurrence) {
    if (historyId == null || scheduleId == null || occurrence <= 0 || occurrence > MAX_OCCURRENCES) {
      throw new IllegalArgumentException("Invalid scheduled occurrence identity");
    }
    return UUID.nameUUIDFromBytes((historyId + "\0TASK_OCCURRENCE\0" + scheduleId + "\0" + occurrence)
        .getBytes(StandardCharsets.UTF_8));
  }

  public static String outputSegmentName(UUID taskId, long attempt, int ordinal) {
    return NativeTaskOutputIdentity.outputSegmentName(taskId, attempt, ordinal);
  }

  public static UUID outputObjectId(UUID historyId, UUID taskId, long attempt, int ordinal) {
    return NativeTaskOutputIdentity.outputObjectId(historyId, taskId, attempt, ordinal);
  }

  public static String outputUri(OutputPolicy policy, UUID historyId, UUID taskId, long attempt, UUID incarnation,
      int ordinal, String sha256) {
    outputRoot(policy);
    return NativeTaskOutputIdentity.outputUri(policy.rootUri(), historyId, taskId, attempt, incarnation, ordinal,
        sha256);
  }

  public static URI outputRoot(OutputPolicy policy) {
    if (policy == null || policy.maxOutputs() <= 0 || policy.maxOutputs() > MAX_OUTPUTS) {
      throw new IllegalArgumentException("Invalid output publication policy");
    }
    return NativeTaskOutputIdentity.outputRoot(policy.rootUri());
  }

  public static QueueState decodeQueue(JsonNode value) throws IOException {
    return MetadataProtocol.fromTree(value, QueueState.class);
  }

  public static TaskState decodeTask(JsonNode value) throws IOException {
    return MetadataProtocol.fromTree(value, TaskState.class);
  }

  public static AttemptState decodeAttempt(JsonNode value) throws IOException {
    return MetadataProtocol.fromTree(value, AttemptState.class);
  }

  public static String queueName(QueueKey queue) {
    TaskCatalog.validateQueue(queue);
    return queue.tableObjectId() + "/" + queue.taskType();
  }

  public static String taskName(UUID taskId) {
    if (taskId == null) {
      throw new IllegalArgumentException("Task ID is required");
    }
    return taskId.toString();
  }

  public static String attemptName(UUID taskId, long attempt) {
    if (attempt <= 0) {
      throw new IllegalArgumentException("Attempt must be positive");
    }
    return taskName(taskId) + "/" + attempt;
  }

  public static String clientId(ParticipantToken minion) {
    if (minion == null) {
      throw new IllegalArgumentException("Minion session is required");
    }
    return "task/" + UUID.nameUUIDFromBytes((minion.historyId() + "\0" + minion.participantId() + "\0"
        + minion.incarnation() + "\0" + minion.registrationEpoch()).getBytes(StandardCharsets.UTF_8));
  }

  static UUID objectId(UUID historyId, RecordKind kind, String name) {
    return UUID.nameUUIDFromBytes((historyId + "\0" + kind + "\0" + name).getBytes(StandardCharsets.UTF_8));
  }
}
