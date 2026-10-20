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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.OwnerToken;


/// Internal durable task execution ledger. This protocol does not publish task outputs or enable native plugins.
/// Participant tokens identify sessions; a future authenticated transport must derive them from its caller identity.
public final class TaskProtocol {
  public static final int MAX_ATTEMPTS = 16;
  public static final int MAX_DEPENDENCIES = 32;
  public static final int MAX_CONFIGS = 128;
  public static final int MAX_DETAIL_LENGTH = 1024;

  private TaskProtocol() {
  }

  /// EXECUTION_FINISHED records worker execution only. It does not certify output publication or task success.
  public enum TaskStatus {
    READY, RUNNING, EXECUTION_FINISHED, RETRYABLE_FAILED, FATAL_FAILED, CANCELLED
  }

  /// FENCED revokes further reports. It does not assert that a cancelled or unavailable worker stopped executing.
  public enum AttemptStatus {
    RUNNING, EXECUTION_FINISHED, RETRYABLE_FAILED, FATAL_FAILED, FENCED
  }

  public record QueueKey(String tableName, UUID tableObjectId, String taskType) {
  }

  /// Explicit minion identity is a bounded initial assignment policy, not plugin capability discovery or scheduling.
  public record Input(QueueKey queue, String minionId, Map<String, String> configs, int maxAttempts) {
    public Input {
      configs = configs == null ? null : Map.copyOf(configs);
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
      @JsonSubTypes.Type(value = Retry.class, name = "RETRY")})
  public sealed interface ControllerOperation permits Submit, SetQueuePaused, Cancel, Retry {
    OwnerToken owner();
  }

  /// Captures current table and schema versions in addition to these explicit input dependencies.
  public record Submit(OwnerToken owner, UUID taskId, Input input, List<ObjectVersion> dependencies)
      implements ControllerOperation {
    public Submit {
      dependencies = dependencies == null ? null : List.copyOf(dependencies);
    }
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
      @JsonSubTypes.Type(value = Complete.class, name = "COMPLETE")})
  public sealed interface WorkerOperation permits Claim, Checkpoint, Complete {
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
