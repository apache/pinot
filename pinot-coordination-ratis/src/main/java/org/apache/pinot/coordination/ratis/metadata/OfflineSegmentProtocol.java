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
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.OwnerToken;


/// Immutable OFFLINE content and fenced desired replica assignments. Publication records an authorized manifest;
/// apply does not fetch or verify external bytes. A desired ONLINE assignment is not an observation of installed data.
/// Content replacement, physical deletion, reclamation and serving publication require subsequent protocols.
public final class OfflineSegmentProtocol {
  public static final int MAX_CUSTOM_CONFIGS = 128;

  private OfflineSegmentProtocol() {
  }

  /// Loader inputs independent of ZK storage. Name and download URI are carried by [Content]. A published content
  /// record represents completed immutable content; it cannot be used to describe a consuming segment.
  public record LoaderDescriptor(long crc, long dataCrc, boolean useDataCrc, @Nullable String tier,
                                 @Nullable String crypterName, long creationTimeMs, long pushTimeMs,
                                 Map<String, String> customConfigs) {
    public LoaderDescriptor {
      customConfigs = customConfigs == null ? null : Map.copyOf(customConfigs);
    }
  }

  /// The segment object ID identifies this immutable content incarnation and is never reused. One table incarnation
  /// and logical segment name can publish once in this slice; a different URI/digest cannot overwrite that manifest.
  public record Content(String tableName, UUID tableObjectId, String segmentName, UUID segmentObjectId,
                        String contentSha256, String downloadUri, LoaderDescriptor loader) {
  }

  /// Versions of the exact table configuration and associated schema used to construct the materialization.
  public record Compatibility(ObjectVersion table, ObjectVersion schema) {
  }

  /// Assignment records are retained after release. Each change advances the epoch and derives a new materialization
  /// identity; old process registrations, compatibility versions and local materializations cannot authorize it.
  public record Assignment(String tableName, UUID tableObjectId, String segmentName, UUID segmentObjectId,
                           String contentSha256, long assignmentEpoch, ParticipantToken server, UUID materializationId,
                           Compatibility compatibility, boolean desiredOnline) {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "operation")
  @JsonSubTypes({@JsonSubTypes.Type(value = PublishContent.class, name = "PUBLISH_CONTENT"),
      @JsonSubTypes.Type(value = AssignReplica.class, name = "ASSIGN_REPLICA"),
      @JsonSubTypes.Type(value = ReconcileTaskOutput.class, name = "RECONCILE_TASK_OUTPUT")})
  public sealed interface Operation permits PublishContent, AssignReplica, ReconcileTaskOutput {
    OwnerToken owner();
  }

  public record PublishContent(OwnerToken owner, ObjectVersion expectedTable, Content content) implements Operation {
  }

  /// An OFFLINE release requires the exact previous server token and compatibility, even when that registration or
  /// table incarnation is no longer current. ONLINE assignment requires live current dependencies and a ready server.
  public record AssignReplica(OwnerToken owner, UUID segmentObjectId, long expectedEpoch, ParticipantToken server,
                              Compatibility compatibility, boolean desiredOnline) implements Operation {
  }

  /// Computes placement from current committed policy and membership, atomically pairing new assignments with
  /// durable installation intent. Subsequent calls claim or complete that intent; this is not serving authority.
  public record ReconcileTaskOutput(OwnerToken owner, UUID taskId, UUID segmentObjectId) implements Operation {
  }

  public static Content decodeContent(JsonNode value)
      throws IOException {
    return MetadataProtocol.fromTree(value, Content.class);
  }

  public static Assignment decodeAssignment(JsonNode value)
      throws IOException {
    return MetadataProtocol.fromTree(value, Assignment.class);
  }

  /// Hashing the variable component keeps record names bounded. Publication still checks the full payload identity,
  /// and the catalog rejects an occupied name or object ID rather than interpreting a collision as an update.
  public static String contentName(UUID tableObjectId, String segmentName) {
    requireIdentity(tableObjectId);
    OfflineSegmentCatalog.identifier(segmentName, 1024);
    return tableObjectId + "/" + UUID.nameUUIDFromBytes(segmentName.getBytes(StandardCharsets.UTF_8));
  }

  public static String assignmentName(UUID segmentObjectId, String serverId) {
    requireIdentity(segmentObjectId);
    OfflineSegmentCatalog.identifier(serverId, 1024);
    return segmentObjectId + "/" + UUID.nameUUIDFromBytes(serverId.getBytes(StandardCharsets.UTF_8));
  }

  public static UUID assignmentObjectId(UUID historyId, UUID segmentObjectId, String serverId) {
    requireIdentity(historyId);
    return UUID.nameUUIDFromBytes((historyId + "\0" + RecordKind.OFFLINE_REPLICA_ASSIGNMENT + "\0"
        + assignmentName(segmentObjectId, serverId)).getBytes(StandardCharsets.UTF_8));
  }

  public static UUID materializationId(UUID historyId, UUID assignmentObjectId, long assignmentEpoch) {
    requireIdentity(historyId);
    requireIdentity(assignmentObjectId);
    if (assignmentEpoch <= 0) {
      throw new CoordinationRejection(Status.INVALID_REQUEST, "Materialization requires a positive assignment epoch");
    }
    return UUID.nameUUIDFromBytes((historyId + "\0OFFLINE_MATERIALIZATION\0" + assignmentObjectId + "\0"
        + assignmentEpoch).getBytes(StandardCharsets.UTF_8));
  }

  private static void requireIdentity(UUID id) {
    if (id == null) {
      throw new CoordinationRejection(Status.INVALID_REQUEST, "Offline content identity is required");
    }
  }
}
