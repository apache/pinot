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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Mutation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.AssignReplica;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Assignment;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Compatibility;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.ReconcileContent;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.ReconcileTaskOutput;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.AssignedOfflineSegmentReload;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.OfflineSegmentReload;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.OperationState;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.State;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.TargetMaterialization;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.OwnerToken;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.PublicationState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.TaskState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.TaskStatus;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Reconciles one published immutable OFFLINE segment against one committed image. New assignments and their
/// installation intent commit together; subsequent commands claim and complete that intent. No external I/O or
/// clocks affect placement.
final class OfflineContentPlacementCatalog {
  private OfflineContentPlacementCatalog() {
  }

  static List<CatalogRecord> stage(ReconcileTaskOutput request, OperationCatalog.View view, long index)
      throws IOException {
    require(request.taskId() != null && request.segmentObjectId() != null, Status.INVALID_REQUEST,
        "Task output reconciliation requires task and segment identities");
    CatalogRecord taskRecord = view.record(RecordKind.TASK, TaskProtocol.taskName(request.taskId()));
    CatalogRecord publicationRecord = view.record(RecordKind.TASK_PUBLICATION, request.taskId().toString());
    require(taskRecord != null && !taskRecord.deleted() && publicationRecord != null && !publicationRecord.deleted(),
        Status.NOT_FOUND, "Published task output is absent");
    TaskState task = TaskProtocol.decodeTask(taskRecord.value());
    PublicationState publication = TaskProtocol.decodePublication(publicationRecord.value());
    require(task.status() == TaskStatus.SUCCEEDED && task.attempt() == publication.attempt(), Status.CONFLICT,
        "Task output requires a successful atomically published attempt");
    Content content = OfflineSegmentCatalog.currentContent(request.segmentObjectId(), view);
    require(publication.outputs().contains(content), Status.INVALID_REQUEST,
        "Segment is not an immutable output of this task publication");
    require(content.loader().tier() == null, Status.INVALID_REQUEST,
        "Native task output placement does not support tiered content");
    return reconcile(request.owner(), content, view, index);
  }

  static List<CatalogRecord> stage(ReconcileContent request, OperationCatalog.View view, long index)
      throws IOException {
    Content content = OfflineSegmentCatalog.currentContent(request.segmentObjectId(), view);
    require(content.loader().tier() == null, Status.INVALID_REQUEST,
        "Native content placement does not support tiered content");
    return reconcile(request.owner(), content, view, index);
  }

  private static List<CatalogRecord> reconcile(OwnerToken owner, Content content, OperationCatalog.View view,
      long index) throws IOException {
    OwnershipCatalog.validateTableOwner(owner, content.tableName(), view);
    CatalogRecord tableRecord = view.record(content.tableObjectId());
    CatalogRecord schemaRecord = view.record(RecordKind.SCHEMA,
        TableNameBuilder.extractRawTableName(content.tableName()));
    require(schemaRecord != null && !schemaRecord.deleted(), Status.NOT_READY, "Placement schema is absent");
    TableConfig table = JsonUtils.jsonNodeToObject(tableRecord.value(), TableConfig.class);
    int replication = NativeReplicaPlacementPolicy.validateTable(table);
    String tenant = NativeReplicaPlacementPolicy.tenant(table);
    CatalogRecord cluster = view.record(RecordKind.CLUSTER_CONFIG, MetadataProtocol.CLUSTER_CONFIG_NAME);
    require(cluster != null && !cluster.deleted() && cluster.value().path(NativeReplicaPlacementPolicy.CONFIG_KEY)
        .isTextual(), Status.NOT_READY, "Native OFFLINE placement policy is not configured");
    NativeReplicaPlacementPolicy policy =
        NativeReplicaPlacementPolicy.parse(cluster.value().path(NativeReplicaPlacementPolicy.CONFIG_KEY).textValue());
    Compatibility compatibility = new Compatibility(version(view, tableRecord), version(view, schemaRecord));
    Map<String, ParticipantState> eligible = new HashMap<>();
    for (var entry : policy.servers().entrySet()) {
      ParticipantState server = view.participant(entry.getKey());
      if (entry.getValue().tenant().equals(tenant) && eligible(server, view.control())) {
        eligible.put(entry.getKey(), server);
      }
    }
    Map<String, Assignment> previous = new TreeMap<>();
    for (CatalogRecord record : view.records(RecordKind.OFFLINE_REPLICA_ASSIGNMENT)) {
      Assignment assignment = OfflineSegmentProtocol.decodeAssignment(record.value());
      if (assignment.segmentObjectId().equals(content.segmentObjectId())) {
        previous.put(assignment.server().participantId(), assignment);
      }
    }
    List<String> ranked = new ArrayList<>(eligible.keySet());
    ranked.sort(Comparator.comparing((String id) -> rank(content.segmentObjectId(), id)).thenComparing(id -> id));
    Set<String> selected = new HashSet<>();
    Set<String> domains = new HashSet<>();
    // Existing eligible copies win over placement ranking, preventing churn when another server joins.
    for (String id : ranked) {
      Assignment assignment = previous.get(id);
      if (assignment != null && assignment.desiredOnline()
          && assignment.server().equals(eligible.get(id).participant()) && selected.size() < replication
          && domains.add(policy.servers().get(id).failureDomain())) {
        selected.add(id);
      }
    }
    for (String id : ranked) {
      if (selected.size() < replication && domains.add(policy.servers().get(id).failureDomain())) {
        selected.add(id);
      }
    }
    Overlay staged = new Overlay(view);
    Set<String> increased = new HashSet<>();
    // Replacing the same stable ID advances its epoch once; it never stages release and assign for one object.
    for (var entry : previous.entrySet()) {
      Assignment assignment = entry.getValue();
      if (assignment.desiredOnline() && !selected.contains(entry.getKey())) {
        staged.add(OfflineSegmentCatalog.stage(new AssignReplica(owner, content.segmentObjectId(),
            assignment.assignmentEpoch(), assignment.server(), assignment.compatibility(), false), staged, index));
      }
    }
    for (String id : selected.stream().sorted().toList()) {
      Assignment assignment = previous.get(id);
      ParticipantState server = eligible.get(id);
      if (assignment == null || !assignment.desiredOnline() || !assignment.server().equals(server.participant())
          || !assignment.compatibility().equals(compatibility)) {
        staged.add(OfflineSegmentCatalog.stage(new AssignReplica(owner, content.segmentObjectId(),
            assignment == null ? 0 : assignment.assignmentEpoch(), server.participant(), compatibility, true),
            staged, index));
        increased.add(id);
      }
    }
    // Superseded operation work must be retired even if a server cannot deliver its previous result.
    for (CatalogRecord record : view.records(RecordKind.CONTROLLER_OPERATION).stream()
        .sorted(Comparator.comparing(CatalogRecord::objectId)).toList()) {
      OperationState operation = OperationProtocol.decodeOperation(record.value());
      if (!(operation.input() instanceof AssignedOfflineSegmentReload assigned)
          || !assigned.reload().segmentObjectId().equals(content.segmentObjectId()) || !active(operation)) {
        continue;
      }
      try {
        OperationCatalog.currentAssigned(assigned, staged);
      } catch (CoordinationRejection e) {
        staged.add(OperationCatalog.stageNative(new OperationProtocol.Cancel(operation.operationId(),
            record.version(), owner, "Replica assignment superseded"), staged, index));
      }
    }
    for (String id : selected.stream().sorted().toList()) {
      Assignment assignment = OfflineSegmentCatalog.currentAssignment(content.segmentObjectId(),
          eligible.get(id).participant(), staged);
      if (reconcileOperation(owner, content, assignment, staged, index)) {
        increased.add(id);
      }
    }
    for (String id : increased.stream().sorted().toList()) {
      OfflineInstallationCatalog.admitProjection(eligible.get(id).participant(), staged);
    }
    return List.copyOf(staged._staged.values());
  }

  private static boolean reconcileOperation(OwnerToken owner, Content content, Assignment assignment,
      Overlay view, long index) throws IOException {
    UUID operationId = NativeReplicaPlacementPolicy.operationId(view.historyId(), assignment.materializationId());
    CatalogRecord record = view.record(RecordKind.CONTROLLER_OPERATION, OperationProtocol.recordName(operationId));
    OfflineSegmentReload reload = new OfflineSegmentReload(content.tableName(), content.tableObjectId(),
        content.segmentName(), content.segmentObjectId(), content.contentSha256(),
        List.of(new TargetMaterialization(assignment.server(), assignment.assignmentEpoch(),
            assignment.materializationId())), false, false, true);
    if (record == null) {
      view.add(OperationCatalog.stageNative(new OperationProtocol.Submit(operationId, reload), view, index));
      return true;
    }
    OperationState operation = OperationProtocol.decodeOperation(record.value());
    require(operation.input() instanceof AssignedOfflineSegmentReload assigned && assigned.reload().equals(reload),
        Status.CONFLICT, "Placement operation identity is occupied by another intent");
    if (!active(operation)) {
      // A terminal failure is visible for operator intervention; never manufacture endless new epochs or retries.
      return false;
    }
    if (operation.status() == State.ACCEPTED || !operation.owner().equals(owner)) {
      AssignedOfflineSegmentReload assigned = (AssignedOfflineSegmentReload) operation.input();
      view.add(OperationCatalog.stageNative(new OperationProtocol.Claim(operationId, record.version(), owner,
          assigned.dependencies()), view, index));
      return false;
    }
    try {
      view.add(OperationCatalog.stageNative(new OperationProtocol.CompleteInstalled(operationId, record.version(),
          owner, operation.attempt()), view, index));
    } catch (CoordinationRejection e) {
      if (e.status() != Status.NOT_READY) {
        throw e;
      }
    }
    return false;
  }

  private static boolean eligible(ParticipantState server, ControlState control) {
    return server != null && server.participant().role() == Role.SERVER && server.ready() && !server.draining()
        && !server.retired() && control != null && control.controlEpoch() != null && control.policy() != null
        && server.expiryRound() > control.round();
  }

  private static boolean active(OperationState operation) {
    return operation.status() == State.ACCEPTED || operation.status() == State.RUNNING;
  }

  private static UUID rank(UUID content, String server) {
    return UUID.nameUUIDFromBytes((content + "\0" + server).getBytes(StandardCharsets.UTF_8));
  }

  private static ObjectVersion version(OperationCatalog.View view, CatalogRecord record) {
    return new ObjectVersion(view.historyId(), record.objectId(), record.version());
  }

  private static void require(boolean condition, Status status, String message) {
    if (!condition) {
      throw new CoordinationRejection(status, message);
    }
  }

  /// An uncommitted overlay lets operation validation capture assignments created by this same atomic command.
  private static final class Overlay implements OperationCatalog.View {
    private final OperationCatalog.View _base;
    private final Map<UUID, CatalogRecord> _staged = new LinkedHashMap<>();

    private Overlay(OperationCatalog.View base) {
      _base = base;
    }

    private void add(List<CatalogRecord> records) {
      for (CatalogRecord record : records) {
        require(_staged.putIfAbsent(record.objectId(), record) == null, Status.CONFLICT,
            "Placement cannot change the same record twice");
        require(_staged.size() <= MetadataProtocol.MAX_MUTATIONS, Status.LIMIT_EXCEEDED,
            "Placement exceeds its atomic record bound");
      }
    }

    @Override
    public CatalogRecord record(RecordKind kind, String name) {
      for (CatalogRecord record : _staged.values()) {
        if (record.kind() == kind && record.name().equals(name)) {
          return record;
        }
      }
      return _base.record(kind, name);
    }

    @Override
    public CatalogRecord record(UUID objectId) {
      return _staged.containsKey(objectId) ? _staged.get(objectId) : _base.record(objectId);
    }

    @Override
    public List<CatalogRecord> records(RecordKind kind) {
      Map<UUID, CatalogRecord> records = new LinkedHashMap<>();
      for (CatalogRecord record : _base.records(kind)) {
        records.put(record.objectId(), record);
      }
      for (CatalogRecord record : _staged.values()) {
        if (record.kind() == kind) {
          records.put(record.objectId(), record);
        }
      }
      return List.copyOf(records.values());
    }

    @Override
    public long recordBytes(CatalogRecord record) throws IOException {
      return _base.recordBytes(record);
    }

    @Override
    public ParticipantState participant(String id) {
      return _base.participant(id);
    }

    @Override
    public ControlState control() {
      return _base.control();
    }

    @Override
    public UUID historyId() {
      return _base.historyId();
    }

    @Override
    public CatalogRecord stage(Mutation mutation, long index) {
      return _base.stage(mutation, index);
    }
  }
}
