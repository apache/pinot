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
package org.apache.pinot.controller.coordination;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogImage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.NativeReplicaPlacementPolicy;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Assignment;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.AssignedOfflineSegmentReload;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.OperationState;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Read-only observations from one bounded, coherent image. Desired assignments never count as installations:
/// installation counts require an exact current materialization receipt from the live SERVER registration.
/// Instances are confined to one controller scan or REST request.
final class NativeTaskPlacementStatus {
  enum State {
    BLOCKED, UNSUPPORTED, WAITING_FOR_SERVERS, UNDER_REPLICATED, INSTALLING, RECONCILING, INSTALLED, FAILED
  }

  record Output(@Nullable UUID taskId, String segmentName, UUID segmentObjectId, int requestedReplicas,
                int desiredReplicas, int installedReplicas, int eligibleServers, State state, String detail) {
    boolean needsReconciliation() {
      return state != State.BLOCKED && state != State.UNSUPPORTED && state != State.INSTALLED && state != State.FAILED;
    }
  }

  private final UUID _history;
  private final Map<RecordKind, Map<String, CatalogRecord>> _records = new EnumMap<>(RecordKind.class);
  private final Map<String, ParticipantState> _participants = new HashMap<>();
  private final Map<UUID, List<CatalogRecord>> _assignments = new HashMap<>();
  @Nullable
  private final ControlState _control;
  @Nullable
  private final NativeReplicaPlacementPolicy _policy;

  NativeTaskPlacementStatus(CatalogImage image) throws IOException {
    _history = image.cursor().historyId();
    ControlState control = null;
    for (CatalogRecord record : image.records()) {
      _records.computeIfAbsent(record.kind(), ignored -> new HashMap<>()).put(record.name(), record);
      if (record.kind() == RecordKind.PARTICIPANT) {
        _participants.put(record.name(), MembershipProtocol.decodeParticipant(record.value()));
      } else if (record.kind() == RecordKind.MEMBERSHIP_CONTROL) {
        control = MembershipProtocol.decodeControl(record.value());
      } else if (record.kind() == RecordKind.OFFLINE_REPLICA_ASSIGNMENT) {
        Assignment assignment = OfflineSegmentProtocol.decodeAssignment(record.value());
        _assignments.computeIfAbsent(assignment.segmentObjectId(), ignored -> new ArrayList<>()).add(record);
      }
    }
    _control = control;
    NativeReplicaPlacementPolicy policy = null;
    CatalogRecord cluster = record(RecordKind.CLUSTER_CONFIG, MetadataProtocol.CLUSTER_CONFIG_NAME);
    if (cluster != null) {
      var value = cluster.value().get(NativeReplicaPlacementPolicy.CONFIG_KEY);
      if (value != null && value.isTextual()) {
        try {
          policy = NativeReplicaPlacementPolicy.parse(value.textValue());
        } catch (IllegalArgumentException ignored) {
          // Invalid or missing administrative policy is an explicit blocked observation, not an empty placement.
        }
      }
    }
    _policy = policy;
  }

  Output output(@Nullable UUID taskId, Content content) throws IOException {
    if (content.loader().tier() != null) {
      return result(taskId, content, -1, -1, -1, -1, State.UNSUPPORTED,
          "Tiered output placement is not supported");
    }
    CatalogRecord table = record(RecordKind.TABLE_CONFIG, content.tableName());
    CatalogRecord schema = record(RecordKind.SCHEMA, TableNameBuilder.extractRawTableName(content.tableName()));
    if (table == null || schema == null || !table.objectId().equals(content.tableObjectId())) {
      return result(taskId, content, -1, -1, -1, -1, State.BLOCKED, "Output table or schema is no longer current");
    }
    TableConfig config = JsonUtils.jsonNodeToObject(table.value(), TableConfig.class);
    int requested;
    String tenant;
    try {
      requested = NativeReplicaPlacementPolicy.validateTable(config);
      tenant = NativeReplicaPlacementPolicy.tenant(config);
    } catch (IllegalArgumentException e) {
      return result(taskId, content, -1, -1, -1, -1, State.UNSUPPORTED,
          "Table placement constraints are not supported");
    }
    if (_policy == null || _control == null || _control.controlEpoch() == null || _control.policy() == null) {
      return result(taskId, content, requested, -1, -1, -1, State.BLOCKED,
          "Native placement policy or membership control is absent or invalid");
    }
    Map<String, String> eligible = new HashMap<>();
    _policy.servers().forEach((server, approval) -> {
      if (tenant.equals(approval.tenant()) && live(_participants.get(server))) {
        eligible.put(server, approval.failureDomain());
      }
    });
    int desired = 0;
    int installed = 0;
    int totalOnline = 0;
    boolean completed = true;
    boolean failed = false;
    Set<String> desiredDomains = new HashSet<>();
    for (CatalogRecord record : _assignments.getOrDefault(content.segmentObjectId(), List.of())) {
      Assignment assignment = OfflineSegmentProtocol.decodeAssignment(record.value());
      if (!assignment.desiredOnline()) {
        continue;
      }
      totalOnline++;
      String server = assignment.server().participantId();
      ParticipantState participant = _participants.get(server);
      if (!eligible.containsKey(server) || participant == null || !participant.participant().equals(assignment.server())
          || !matches(assignment, content, table, schema)) {
        continue;
      }
      desired++;
      desiredDomains.add(eligible.get(server));
      UUID operationId = NativeReplicaPlacementPolicy.operationId(_history, assignment.materializationId());
      CatalogRecord operationRecord = record(RecordKind.CONTROLLER_OPERATION, operationId.toString());
      OperationState operation = operationRecord == null ? null
          : OperationProtocol.decodeOperation(operationRecord.value());
      failed |= operation != null && (operation.status() == OperationProtocol.State.FAILED
          || operation.status() == OperationProtocol.State.CANCELLED);
      boolean receipt = installed(record, assignment, operation);
      if (receipt) {
        installed++;
      }
      completed &= receipt && operation.status() == OperationProtocol.State.SUCCEEDED;
    }
    State state;
    String detail;
    if (new HashSet<>(eligible.values()).size() < requested) {
      state = State.WAITING_FOR_SERVERS;
      detail = "Insufficient approved live SERVER failure domains";
    } else if (failed && desired == requested && totalOnline == desired && desiredDomains.size() == requested) {
      state = State.FAILED;
      detail = "A current installation operation is terminal; operator recovery is required";
    } else if (desired < requested) {
      state = State.UNDER_REPLICATED;
      detail = "Desired replicas have not reached the requested count";
    } else if (installed < requested) {
      state = State.INSTALLING;
      detail = "Waiting for exact current installation receipts";
    } else if (desired != requested || totalOnline != desired || desiredDomains.size() != requested || !completed) {
      state = State.RECONCILING;
      detail = "Placement or installation completion still requires reconciliation";
    } else {
      state = State.INSTALLED;
      detail = "Requested replicas have current installation receipts; query serving remains disabled";
    }
    return result(taskId, content, requested, desired, installed, eligible.size(), state, detail);
  }

  private boolean installed(CatalogRecord assignmentRecord, Assignment assignment, @Nullable OperationState operation)
      throws IOException {
    if (operation == null || !(operation.input() instanceof AssignedOfflineSegmentReload assigned)
        || (operation.status() != OperationProtocol.State.RUNNING
            && operation.status() != OperationProtocol.State.SUCCEEDED)
        || operation.owner() == null || operation.attempt() <= 0) {
      return false;
    }
    var reload = assigned.reload();
    var target = new OperationProtocol.TargetMaterialization(assignment.server(), assignment.assignmentEpoch(),
        assignment.materializationId());
    ObjectVersion version = new ObjectVersion(_history, assignmentRecord.objectId(), assignmentRecord.version());
    if (!reload.reconcile() || !reload.tableName().equals(assignment.tableName())
        || !reload.tableObjectId().equals(assignment.tableObjectId())
        || !reload.segmentName().equals(assignment.segmentName())
        || !reload.segmentObjectId().equals(assignment.segmentObjectId())
        || !reload.contentSha256().equals(assignment.contentSha256()) || !reload.targets().equals(List.of(target))
        || !assigned.dependencies().contains(version) || !operation.dependencies().contains(version)) {
      return false;
    }
    CatalogRecord receiptRecord = record(RecordKind.OFFLINE_INSTALLATION, OfflineInstallationProtocol.recordName(
        operation.operationId(), operation.attempt(), assignment.server().participantId()));
    if (receiptRecord == null) {
      return false;
    }
    var receipt = OfflineInstallationProtocol.decodeReceipt(receiptRecord.value());
    var report = receipt.installation();
    var expected = new OfflineInstallationProtocol.Materialization(assignment.tableName(), assignment.tableObjectId(),
        assignment.segmentName(), assignment.segmentObjectId(), assignment.contentSha256(), assignment.server(),
        version, assignment.materializationId(), assignment.compatibility());
    return _history.equals(receipt.historyId()) && report.operationId().equals(operation.operationId())
        && report.attempt() == operation.attempt() && report.owner().equals(operation.owner())
        && report.installed().equals(expected)
        && (operation.status() != OperationProtocol.State.SUCCEEDED || operation.completionProof() != null
            && operation.completionProof().installations().contains(
                new ObjectVersion(_history, receiptRecord.objectId(), receiptRecord.version())));
  }

  private static boolean matches(Assignment assignment, Content content, CatalogRecord table, CatalogRecord schema) {
    return assignment.tableObjectId().equals(content.tableObjectId())
        && assignment.tableName().equals(content.tableName()) && assignment.segmentName().equals(content.segmentName())
        && assignment.contentSha256().equals(content.contentSha256())
        && assignment.compatibility().table().objectId().equals(table.objectId())
        && assignment.compatibility().table().version() == table.version()
        && assignment.compatibility().schema().objectId().equals(schema.objectId())
        && assignment.compatibility().schema().version() == schema.version();
  }

  private boolean live(@Nullable ParticipantState participant) {
    return participant != null && participant.participant().role() == Role.SERVER && participant.ready()
        && !participant.retired() && !participant.draining() && _control != null
        && participant.expiryRound() > _control.round();
  }

  @Nullable
  private CatalogRecord record(RecordKind kind, String name) {
    return _records.getOrDefault(kind, Map.of()).get(name);
  }

  private static Output result(@Nullable UUID taskId, Content content, int requested, int desired, int installed,
      int eligible, State state, String detail) {
    return new Output(taskId, content.segmentName(), content.segmentObjectId(), requested, desired, installed, eligible,
        state, detail);
  }
}
