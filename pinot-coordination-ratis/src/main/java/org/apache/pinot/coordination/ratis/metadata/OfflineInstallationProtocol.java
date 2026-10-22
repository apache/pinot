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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Compatibility;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.OwnerToken;


/// Authenticated participant installation evidence for one immutable operation target. Receipts prove a completed
/// local installation, not continued query-serving authority. The state machine never inspects participant storage.
public final class OfflineInstallationProtocol {
  public static final String CLIENT_PREFIX = "server-installation/";
  public static final int MAX_SNAPSHOT_RECORDS = 4096;
  public static final int MIN_SNAPSHOT_BYTES = 1024;
  public static final int MAX_SNAPSHOT_BYTES = MetadataProtocol.MAX_CHANGE_BYTES;

  private OfflineInstallationProtocol() {
  }

  public record Materialization(String tableName, UUID tableObjectId, String segmentName, UUID segmentObjectId,
                                String contentSha256, ParticipantToken server, ObjectVersion assignment,
                                UUID materializationId, Compatibility compatibility) {
  }

  /// The installation ID identifies a physical install; the manifest digest binds the durable local receipt.
  /// Participant identity is supplied by authenticated transport, never taken from an untrusted wire identity.
  public record ReportInstallation(UUID operationId, long attempt, OwnerToken owner, Materialization installed,
                                   UUID installationId, String manifestSha256) {
  }

  public record Receipt(UUID historyId, ReportInstallation installation) {
  }

  /// Coherent bounded work for one server target. Other operation targets and participant metadata are omitted.
  public record Work(UUID operationId, long attempt, OwnerToken owner, Materialization target, Content content,
                     CatalogRecord table, CatalogRecord schema, boolean forceDownload, boolean initialLoad,
                     boolean reconcile) {
    public Work(UUID operationId, long attempt, OwnerToken owner, Materialization target, Content content,
        CatalogRecord table, CatalogRecord schema, boolean forceDownload, boolean initialLoad) {
      this(operationId, attempt, owner, target, content, table, schema, forceDownload, initialLoad, false);
    }
  }

  public record WorkResult(Status status, Revision revision, String detail, @Nullable Work work) {
  }

  /// Query peer authority for one exact BROKER session and the receiving server's assigned broker tenants. Public
  /// fingerprints identify authenticated certificates; they are not credentials or proof of possession.
  public record QueryBrokerAuthority(ParticipantState participant, List<String> certificateSha256,
                                     List<String> brokerTenants) {
    public QueryBrokerAuthority {
      certificateSha256 = List.copyOf(certificateSha256);
      brokerTenants = List.copyOf(brokerTenants);
    }
  }

  /// Complete metadata projection for one exact SERVER registration, captured atomically with membership/control.
  /// Records contain cluster configuration and this registration's assignments, content and current referenced
  /// table/schema records, including referenced tombstones that fence deleted incarnations. Only nonterminal
  /// assigned-operation IDs are disclosed, never other targets or receipts. Typed query broker grants are scoped
  /// to assigned current table tenants. Existing cluster configuration remains complete; its public policy settings
  /// do not themselves authorize query peers. Optional oversized authority sets reject the entire image and can
  /// prevent hydration; historical installation admission reservations do not cover this additional opt-in payload.
  /// A consumer may advertise this revision only after atomically installing the entire successful projection;
  /// a work lookup revision is not evidence of applying this metadata. Oversized projections fail without a prefix.
  public record InstallationSnapshot(Status status, Revision revision, String detail,
                                     @Nullable ParticipantState participant, List<CatalogRecord> records,
                                     List<UUID> operationIds, @Nullable ControlState control,
                                     List<QueryBrokerAuthority> queryBrokers) {
    public InstallationSnapshot {
      records = List.copyOf(records);
      operationIds = List.copyOf(operationIds);
      queryBrokers = List.copyOf(queryBrokers);
    }

    public InstallationSnapshot(Status status, Revision revision, String detail, @Nullable ParticipantState participant,
        List<CatalogRecord> records, List<UUID> operationIds, @Nullable ControlState control) {
      this(status, revision, detail, participant, records, operationIds, control, List.of());
    }
  }

  public static Receipt decodeReceipt(JsonNode value)
      throws IOException {
    return MetadataProtocol.fromTree(value, Receipt.class);
  }

  public static WorkResult decodeWorkResult(byte[] value)
      throws IOException {
    return MetadataProtocol.decodeValue(value, WorkResult.class);
  }

  /// A replaced server registration cannot acknowledge or reuse the previous registration's durable request stream.
  public static String clientId(ParticipantToken server) {
    if (server == null || server.historyId() == null || server.role() != Role.SERVER || server.incarnation() == null
        || server.registrationEpoch() <= 0) {
      throw new IllegalArgumentException("A complete SERVER registration is required");
    }
    OfflineSegmentCatalog.identifier(server.participantId(), 1024);
    String identity = server.historyId() + "\0" + server.participantId() + "\0" + server.role() + "\0"
        + server.incarnation() + "\0" + server.registrationEpoch();
    try {
      return CLIENT_PREFIX + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
          .digest(identity.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  public static String recordName(UUID operationId, long attempt, String participantId) {
    if (operationId == null || attempt <= 0 || participantId == null || participantId.isBlank()) {
      throw new IllegalArgumentException("Installation operation, attempt and participant are required");
    }
    return operationId + "/" + attempt + "/" + UUID.nameUUIDFromBytes(participantId.getBytes(StandardCharsets.UTF_8));
  }

  public static UUID objectId(UUID historyId, UUID operationId, long attempt, String participantId) {
    if (historyId == null) {
      throw new IllegalArgumentException("Installation history is required");
    }
    return UUID.nameUUIDFromBytes((historyId + "\0OFFLINE_INSTALLATION\0"
        + recordName(operationId, attempt, participantId)).getBytes(StandardCharsets.UTF_8));
  }
}
