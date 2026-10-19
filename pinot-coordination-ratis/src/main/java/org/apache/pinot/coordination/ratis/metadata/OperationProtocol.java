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
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Mutation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.OwnerToken;


/// Durable operation intent and attempt fences. These immutable values describe controller orchestration metadata;
/// recording completion does not itself execute or prove a physical segment reload on a participant.
public final class OperationProtocol {
  public static final int MAX_TARGETS = 128;
  public static final int MAX_DEPENDENCIES = 128;
  /// One assignment dependency per target, plus immutable content, table and schema dependencies.
  public static final int MAX_ASSIGNED_TARGETS = MAX_DEPENDENCIES - 3;
  public static final int MAX_DETAIL_LENGTH = 1024;

  private OperationProtocol() {
  }

  public enum State {
    ACCEPTED, RUNNING, SUCCEEDED, FAILED, CANCELLED
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "specType")
  @JsonSubTypes({@JsonSubTypes.Type(value = OfflineSegmentReload.class, name = "OFFLINE_SEGMENT_RELOAD"),
      @JsonSubTypes.Type(value = AssignedOfflineSegmentReload.class, name = "ASSIGNED_OFFLINE_SEGMENT_RELOAD")})
  public sealed interface Spec permits OfflineSegmentReload, AssignedOfflineSegmentReload {
    String tableName();

    UUID tableObjectId();
  }

  /// The typed table name retains database qualification. The legacy name also supports explicit initial loading:
  /// `initialLoad=true` loads an absent segment; `reconcile=true` conditionally restores current assignment authority
  /// over an absent or previously installed segment. Ordinary reloads retain both defaults as `false`.
  /// `submittedAtMs` is informational client-supplied job metadata, never a lease or fencing clock.
  public record OfflineSegmentReload(String tableName, UUID tableObjectId, String segmentName, UUID segmentObjectId,
                                     String contentSha256, List<TargetMaterialization> targets,
                                     @JsonInclude(JsonInclude.Include.NON_DEFAULT) boolean forceDownload,
                                     @JsonInclude(JsonInclude.Include.NON_DEFAULT) boolean initialLoad,
                                     @JsonInclude(JsonInclude.Include.NON_DEFAULT) boolean reconcile,
                                     @JsonInclude(JsonInclude.Include.NON_DEFAULT) long submittedAtMs)
      implements Spec {
    public OfflineSegmentReload {
      targets = targets == null ? null : List.copyOf(targets);
    }

    /// Preserve the original intent encoding and request digest when force-download was not requested.
    public OfflineSegmentReload(String tableName, UUID tableObjectId, String segmentName, UUID segmentObjectId,
        String contentSha256, List<TargetMaterialization> targets) {
      this(tableName, tableObjectId, segmentName, segmentObjectId, contentSha256, targets, false, false, false, 0);
    }

    public OfflineSegmentReload(String tableName, UUID tableObjectId, String segmentName, UUID segmentObjectId,
        String contentSha256, List<TargetMaterialization> targets, boolean forceDownload) {
      this(tableName, tableObjectId, segmentName, segmentObjectId, contentSha256, targets, forceDownload, false,
          false, 0);
    }

    public OfflineSegmentReload(String tableName, UUID tableObjectId, String segmentName, UUID segmentObjectId,
        String contentSha256, List<TargetMaterialization> targets, boolean forceDownload, boolean initialLoad) {
      this(tableName, tableObjectId, segmentName, segmentObjectId, contentSha256, targets, forceDownload, initialLoad,
          false, 0);
    }

    public OfflineSegmentReload(String tableName, UUID tableObjectId, String segmentName, UUID segmentObjectId,
        String contentSha256, List<TargetMaterialization> targets, boolean forceDownload, boolean initialLoad,
        boolean reconcile) {
      this(tableName, tableObjectId, segmentName, segmentObjectId, contentSha256, targets, forceDownload, initialLoad,
          reconcile, 0);
    }

    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public static OfflineSegmentReload fromJson(JsonNode value)
        throws IOException {
      if (!(value instanceof ObjectNode)) {
        throw new IOException("Offline reload intent must be an object");
      }
      ObjectNode normalized = value.deepCopy();
      // Only these explicitly versioned defaults are permitted; all other missing/unknown fields remain invalid.
      if (!normalized.has("forceDownload")) {
        normalized.put("forceDownload", false);
      }
      if (!normalized.has("initialLoad")) {
        normalized.put("initialLoad", false);
      }
      if (!normalized.has("reconcile")) {
        normalized.put("reconcile", false);
      }
      if (!normalized.has("submittedAtMs")) {
        normalized.put("submittedAtMs", 0);
      }
      ReloadFields fields = MetadataProtocol.fromTree(normalized, ReloadFields.class);
      return new OfflineSegmentReload(fields.tableName(), fields.tableObjectId(), fields.segmentName(),
          fields.segmentObjectId(), fields.contentSha256(), fields.targets(), fields.forceDownload(),
          fields.initialLoad(), fields.reconcile(), fields.submittedAtMs());
    }
  }

  private record ReloadFields(String tableName, UUID tableObjectId, String segmentName, UUID segmentObjectId,
                              String contentSha256, List<TargetMaterialization> targets, boolean forceDownload,
                              boolean initialLoad, boolean reconcile, long submittedAtMs) {
  }

  /// Native intent captures the exact authoritative content, assignments and loader compatibility at acceptance.
  /// Only the state machine derives these dependencies; callers submit the bare reload intent.
  public record AssignedOfflineSegmentReload(OfflineSegmentReload reload, List<ObjectVersion> dependencies)
      implements Spec {
    public AssignedOfflineSegmentReload {
      dependencies = dependencies == null ? null : List.copyOf(dependencies);
    }

    @Override
    public String tableName() {
      return reload == null ? null : reload.tableName();
    }

    @Override
    public UUID tableObjectId() {
      return reload == null ? null : reload.tableObjectId();
    }
  }

  public record TargetMaterialization(ParticipantToken server, long assignmentEpoch, UUID materializationId) {
  }

  /// Counts are monotonic within one attempt. A new claim resets them and may rescan every immutable target;
  /// this ledger does not promise exactly-once external I/O.
  public record Progress(int completedTargets, int failedTargets, String detail) {
  }

  public record OperationState(UUID historyId, UUID operationId, Spec input, State status, long attempt,
                               @Nullable OwnerToken owner, List<ObjectVersion> dependencies, Progress progress,
                               @Nullable Progress result,
                               @Nullable @JsonInclude(JsonInclude.Include.NON_NULL) CompletionProof completionProof) {
    public OperationState {
      dependencies = dependencies == null ? null : List.copyOf(dependencies);
    }

    public OperationState(UUID historyId, UUID operationId, Spec input, State status, long attempt,
        @Nullable OwnerToken owner, List<ObjectVersion> dependencies, Progress progress, @Nullable Progress result) {
      this(historyId, operationId, input, status, attempt, owner, dependencies, progress, result, null);
    }

    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public static OperationState fromJson(JsonNode value)
        throws IOException {
      if (!(value instanceof ObjectNode)) {
        throw new IOException("Operation state must be an object");
      }
      ObjectNode normalized = value.deepCopy();
      if (!normalized.has("completionProof")) {
        normalized.putNull("completionProof");
      }
      OperationFields fields = MetadataProtocol.fromTree(normalized, OperationFields.class);
      return new OperationState(fields.historyId(), fields.operationId(), fields.input(), fields.status(),
          fields.attempt(), fields.owner(), fields.dependencies(), fields.progress(), fields.result(),
          fields.completionProof());
    }
  }

  private record OperationFields(UUID historyId, UUID operationId, Spec input, State status, long attempt,
                                 @Nullable OwnerToken owner, List<ObjectVersion> dependencies, Progress progress,
                                 @Nullable Progress result, @Nullable CompletionProof completionProof) {
  }

  public record CompletionProof(List<ObjectVersion> installations) {
    public CompletionProof {
      installations = installations == null ? null : List.copyOf(installations);
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "operation")
  @JsonSubTypes({@JsonSubTypes.Type(value = Submit.class, name = "SUBMIT"),
      @JsonSubTypes.Type(value = Claim.class, name = "CLAIM"),
      @JsonSubTypes.Type(value = Checkpoint.class, name = "CHECKPOINT"),
      @JsonSubTypes.Type(value = Complete.class, name = "COMPLETE"),
      @JsonSubTypes.Type(value = CompleteInstalled.class, name = "COMPLETE_INSTALLED"),
      @JsonSubTypes.Type(value = Cancel.class, name = "CANCEL")})
  public sealed interface Operation permits Submit, Claim, Checkpoint, Complete, CompleteInstalled, Cancel {
    UUID operationId();
  }

  public record Submit(UUID operationId, Spec input) implements Operation {
  }

  public record Claim(UUID operationId, long expectedVersion, OwnerToken owner, List<ObjectVersion> dependencies)
      implements Operation {
    public Claim {
      dependencies = dependencies == null ? null : List.copyOf(dependencies);
    }
  }

  public record Checkpoint(UUID operationId, long expectedVersion, OwnerToken owner, long attempt, Progress progress)
      implements Operation {
  }

  public record Complete(UUID operationId, long expectedVersion, OwnerToken owner, long attempt, State terminal,
                         Progress result, List<Mutation> effects) implements Operation {
    public Complete {
      effects = effects == null ? null : List.copyOf(effects);
    }
  }

  /// Success is derived from every committed target receipt; callers cannot supply counters or catalog effects.
  public record CompleteInstalled(UUID operationId, long expectedVersion, OwnerToken owner, long attempt)
      implements Operation {
  }

  /// Cancellation fences subsequent publication, including an in-flight former attempt. It does not roll back
  /// external work that a participant may already have performed.
  public record Cancel(UUID operationId, long expectedVersion, OwnerToken owner, String detail) implements Operation {
  }

  public static OperationState decodeOperation(JsonNode value)
      throws IOException {
    return MetadataProtocol.fromTree(value, OperationState.class);
  }

  public static String recordName(UUID operationId) {
    if (operationId == null) {
      throw new IllegalArgumentException("Operation ID is required");
    }
    return operationId.toString();
  }
}
