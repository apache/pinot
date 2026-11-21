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
  public static final int MAX_DETAIL_LENGTH = 1024;

  private OperationProtocol() {
  }

  public enum State {
    ACCEPTED, RUNNING, SUCCEEDED, FAILED, CANCELLED
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "specType")
  @JsonSubTypes(@JsonSubTypes.Type(value = OfflineSegmentReload.class, name = "OFFLINE_SEGMENT_RELOAD"))
  public sealed interface Spec permits OfflineSegmentReload {
    String tableName();

    UUID tableObjectId();
  }

  /// The typed table name retains database qualification. Segment and content identities are immutable input;
  /// future participant execution must additionally validate the assignment epoch and materialization identity.
  public record OfflineSegmentReload(String tableName, UUID tableObjectId, String segmentName, UUID segmentObjectId,
                                     String contentSha256, List<TargetMaterialization> targets) implements Spec {
    public OfflineSegmentReload {
      targets = targets == null ? null : List.copyOf(targets);
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
                               @Nullable Progress result) {
    public OperationState {
      dependencies = dependencies == null ? null : List.copyOf(dependencies);
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "operation")
  @JsonSubTypes({@JsonSubTypes.Type(value = Submit.class, name = "SUBMIT"),
      @JsonSubTypes.Type(value = Claim.class, name = "CLAIM"),
      @JsonSubTypes.Type(value = Checkpoint.class, name = "CHECKPOINT"),
      @JsonSubTypes.Type(value = Complete.class, name = "COMPLETE"),
      @JsonSubTypes.Type(value = Cancel.class, name = "CANCEL")})
  public sealed interface Operation permits Submit, Claim, Checkpoint, Complete, Cancel {
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
