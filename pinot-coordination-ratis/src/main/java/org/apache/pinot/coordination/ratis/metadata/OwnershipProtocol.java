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
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.hash.MurmurHashFunctions;


/// Durable controller ownership fences. Layout is explicitly configured and immutable until a future fenced
/// migration protocol is implemented. Tokens identify authority; the transport separately authenticates callers.
public final class OwnershipProtocol {
  public static final String LAYOUT_NAME = "layout";

  private OwnershipProtocol() {
  }

  public enum HashVersion {
    MURMUR2_UTF8_MASK_POSITIVE_V1
  }

  public record Layout(long generation, int tableShards, HashVersion hashVersion) {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes({@JsonSubTypes.Type(value = TableShard.class, name = "TABLE_SHARD"),
      @JsonSubTypes.Type(value = ClusterWork.class, name = "CLUSTER_WORK"),
      @JsonSubTypes.Type(value = TablelessTask.class, name = "TABLELESS_TASK")})
  public sealed interface Scope permits TableShard, ClusterWork, TablelessTask {
  }

  public record TableShard(int shard) implements Scope {
  }

  public record ClusterWork(String name) implements Scope {
  }

  public record TablelessTask(String taskType) implements Scope {
  }

  /// An owner epoch fences transfer and revocation. Eligibility additionally depends on the exact current
  /// participant token and its committed readiness/lease, independently of the current Raft leader's term.
  public record OwnerToken(UUID historyId, long layoutGeneration, Scope scope, long ownerEpoch,
                           ParticipantToken participant) {
  }

  /// Revocation retains this record with a null owner and a new epoch; assignment records are never deleted.
  public record Assignment(long layoutGeneration, Scope scope, long ownerEpoch, @Nullable ParticipantToken owner) {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "operation")
  @JsonSubTypes({@JsonSubTypes.Type(value = ConfigureLayout.class, name = "CONFIGURE_LAYOUT"),
      @JsonSubTypes.Type(value = AssignOwner.class, name = "ASSIGN_OWNER")})
  public sealed interface Operation permits ConfigureLayout, AssignOwner {
  }

  public record ConfigureLayout(ControlToken control, Layout layout) implements Operation {
  }

  public record AssignOwner(ControlToken control, long layoutGeneration, Scope scope, long expectedEpoch,
                            @Nullable ParticipantToken owner) implements Operation {
  }

  public static Layout decodeLayout(JsonNode value)
      throws IOException {
    return MetadataProtocol.fromTree(value, Layout.class);
  }

  public static Assignment decodeAssignment(JsonNode value)
      throws IOException {
    return MetadataProtocol.fromTree(value, Assignment.class);
  }

  /// Stable names distinguish all scope families without interpreting caller names as paths.
  public static String name(Scope scope) {
    OwnershipCatalog.validateScope(scope);
    return switch (scope) {
      case TableShard table -> "table-shard/" + table.shard();
      case ClusterWork work -> "cluster-work/" + work.name();
      case TablelessTask task -> "tableless-task/" + task.taskType();
    };
  }

  /// Matches the existing lead-controller UTF-8 Murmur2/positive-mask mapping. Only a terminal Pinot table-type
  /// suffix is removed; database qualification is part of the hashed raw identity.
  public static TableShard tableScope(Layout layout, String databaseQualifiedTypedOrRawName) {
    OwnershipCatalog.validateLayout(layout);
    OwnershipCatalog.identifier(databaseQualifiedTypedOrRawName, 1024);
    String rawName = TableNameBuilder.extractRawTableName(databaseQualifiedTypedOrRawName);
    OwnershipCatalog.identifier(rawName, 1024);
    return new TableShard((MurmurHashFunctions.murmurHash2(rawName.getBytes(StandardCharsets.UTF_8))
        & Integer.MAX_VALUE) % layout.tableShards());
  }
}
