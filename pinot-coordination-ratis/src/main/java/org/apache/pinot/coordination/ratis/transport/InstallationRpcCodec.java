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
package org.apache.pinot.coordination.ratis.transport;

import java.io.IOException;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.InstallationSnapshot;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.Materialization;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.Receipt;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.ReportInstallation;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.Work;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.WorkResult;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Compatibility;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.OwnerToken;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.transport.ParticipantAccessPolicy.ParticipantIdentity;


/// Closed server installation protocol. All command and outcome identities come from the authenticated SERVER
/// session; full server identities are read-only response values. No generic metadata queries or writes are accepted.
final class InstallationRpcCodec {
  private InstallationRpcCodec() {
  }

  static MetadataProtocol.InstallationSessionCommand command(MetadataRpc.InstallationCommandRequest request,
      ParticipantIdentity identity)
      throws IOException {
    MetadataRpcCodec.rejectUnknown(request);
    require(request.hasSession(), "Server session required");
    UUID history = UUID.fromString(request.getHistoryId());
    ParticipantToken server = server(history, request.getSession(), identity);
    String clientId = OfflineInstallationProtocol.clientId(server);
    MetadataProtocol.Command command = switch (request.getCommandCase()) {
      case REGISTER -> new MetadataProtocol.RegisterClient(history, clientId,
          UUID.fromString(request.getRegister().getIncarnation()), request.getRegister().getExpectedEpoch());
      case ACKNOWLEDGE -> new MetadataProtocol.Acknowledge(history, clientId,
          request.getAcknowledge().getClientEpoch(), request.getAcknowledge().getStreamId(),
          request.getAcknowledge().getThroughSequence());
      case REPORT -> {
        MetadataRpc.SequencedInstallationReport report = request.getReport();
        require(report.hasReport(), "Installation report required");
        yield new MetadataProtocol.NativeInstallationCommand(history, clientId, report.getClientEpoch(),
            report.getStreamId(), report.getSequence(), report(report.getReport(), server));
      }
      default -> throw new IllegalArgumentException("Typed installation command required");
    };
    MetadataProtocol.InstallationSessionCommand guarded = new MetadataProtocol.InstallationSessionCommand(server,
        command);
    MetadataProtocol.encode(guarded);
    return guarded;
  }

  static MetadataProtocol.GetInstallationWork lookup(MetadataRpc.InstallationLookupRequest request,
      ParticipantIdentity identity) {
    MetadataRpcCodec.rejectUnknown(request);
    require(request.hasSession(), "Server session required");
    UUID history = UUID.fromString(request.getHistoryId());
    return new MetadataProtocol.GetInstallationWork(history, server(history, request.getSession(), identity),
        UUID.fromString(request.getOperationId()));
  }

  static MetadataProtocol.InstallationSessionQuery outcome(MetadataRpc.InstallationOutcomeRequest request,
      ParticipantIdentity identity) {
    MetadataRpcCodec.rejectUnknown(request);
    require(request.hasSession() && request.hasOutcome(), "Server session and outcome identity required");
    UUID history = UUID.fromString(request.getHistoryId());
    ParticipantToken server = server(history, request.getSession(), identity);
    return new MetadataProtocol.InstallationSessionQuery(server, new MetadataProtocol.GetOutcome(history,
        OfflineInstallationProtocol.clientId(server), request.getOutcome().getClientEpoch(),
        request.getOutcome().getStreamId(), request.getOutcome().getSequence()));
  }

  static ParticipantToken snapshot(MetadataRpc.InstallationSnapshotRequest request, ParticipantIdentity identity) {
    MetadataRpcCodec.rejectUnknown(request);
    require(request.hasSession(), "Server session required");
    require(request.getMaxRecords() > 0
        && request.getMaxRecords() <= OfflineInstallationProtocol.MAX_SNAPSHOT_RECORDS
        && request.getMaxBytes() >= OfflineInstallationProtocol.MIN_SNAPSHOT_BYTES
        && request.getMaxBytes() <= OfflineInstallationProtocol.MAX_SNAPSHOT_BYTES,
        "Installation snapshot bounds required");
    return server(UUID.fromString(request.getHistoryId()), request.getSession(), identity);
  }

  static MetadataRpc.InstallationSnapshot snapshot(InstallationSnapshot snapshot)
      throws IOException {
    MetadataRpc.InstallationSnapshot.Builder result = MetadataRpc.InstallationSnapshot.newBuilder()
        .setStatus(MetadataRpc.ResultStatus.valueOf(snapshot.status().name())).setDetail(snapshot.detail())
        .setRevision(MetadataRpc.Revision.newBuilder().setHistoryId(snapshot.revision().historyId().toString())
            .setIndex(snapshot.revision().index()));
    if (snapshot.participant() != null) {
      result.setParticipant(MembershipRpcCodec.participant(snapshot.participant()));
    }
    if (snapshot.control() != null) {
      result.setControl(MembershipRpcCodec.control(snapshot.control()));
    }
    for (CatalogRecord record : snapshot.records()) {
      result.addRecords(MetadataRpcCodec.record(record));
    }
    snapshot.operationIds().forEach(operation -> result.addOperationIds(operation.toString()));
    for (var authority : snapshot.queryBrokers()) {
      result.addQueryBrokers(MetadataRpc.QueryBrokerAuthority.newBuilder()
          .setParticipant(MembershipRpcCodec.participant(authority.participant()))
          .addAllCertificateSha256(authority.certificateSha256())
          .addAllBrokerTenants(authority.brokerTenants()));
    }
    return result.build();
  }

  private static ParticipantToken server(UUID history, MetadataRpc.ParticipantSession session,
      ParticipantIdentity identity) {
    require(identity.role() == Role.SERVER && session.getRegistrationEpoch() > 0, "Current SERVER session required");
    return new ParticipantToken(history, identity.participantId(), Role.SERVER,
        UUID.fromString(session.getIncarnation()), session.getRegistrationEpoch());
  }

  static MetadataRpc.InstallationReport report(ReportInstallation report) {
    return MetadataRpc.InstallationReport.newBuilder().setOperationId(report.operationId().toString())
        .setAttempt(report.attempt()).setOwner(ControllerRpcCodec.owner(report.owner()))
        .setInstalled(target(report.installed())).setInstallationId(report.installationId().toString())
        .setManifestSha256(report.manifestSha256()).build();
  }

  private static ReportInstallation report(MetadataRpc.InstallationReport report, ParticipantToken server) {
    require(report.hasOwner() && report.hasInstalled() && report.getAttempt() > 0
        && report.getManifestSha256().matches("[0-9a-f]{64}"), "Complete installation evidence required");
    return new ReportInstallation(UUID.fromString(report.getOperationId()), report.getAttempt(),
        ControllerRpcCodec.owner(report.getOwner()), target(report.getInstalled(), server),
        UUID.fromString(report.getInstallationId()), report.getManifestSha256());
  }

  private static MetadataRpc.InstallationTarget target(Materialization target) {
    return MetadataRpc.InstallationTarget.newBuilder().setTableName(target.tableName())
        .setTableObjectId(target.tableObjectId().toString()).setSegmentName(target.segmentName())
        .setSegmentObjectId(target.segmentObjectId().toString()).setContentSha256(target.contentSha256())
        .setAssignment(OfflineSegmentRpcCodec.version(target.assignment()))
        .setMaterializationId(target.materializationId().toString())
        .setCompatibility(MetadataRpc.MaterializationCompatibility.newBuilder()
            .setTable(OfflineSegmentRpcCodec.version(target.compatibility().table()))
            .setSchema(OfflineSegmentRpcCodec.version(target.compatibility().schema()))).build();
  }

  private static Materialization target(MetadataRpc.InstallationTarget target, ParticipantToken server) {
    require(target.hasAssignment() && target.hasCompatibility() && target.getCompatibility().hasTable()
        && target.getCompatibility().hasSchema() && target.getContentSha256().matches("[0-9a-f]{64}"),
        "Complete installation target required");
    identifier(target.getTableName());
    identifier(target.getSegmentName());
    ObjectVersion assignment = OfflineSegmentRpcCodec.version(target.getAssignment());
    ObjectVersion table = OfflineSegmentRpcCodec.version(target.getCompatibility().getTable());
    ObjectVersion schema = OfflineSegmentRpcCodec.version(target.getCompatibility().getSchema());
    UUID tableId = UUID.fromString(target.getTableObjectId());
    require(assignment.historyId().equals(server.historyId()) && table.historyId().equals(server.historyId())
        && schema.historyId().equals(server.historyId()) && table.objectId().equals(tableId)
        && !table.objectId().equals(schema.objectId()), "Installation target histories or compatibility differ");
    return new Materialization(target.getTableName(), tableId, target.getSegmentName(),
        UUID.fromString(target.getSegmentObjectId()), target.getContentSha256(), server, assignment,
        UUID.fromString(target.getMaterializationId()), new Compatibility(table, schema));
  }

  static MetadataRpc.InstallationReceipt receipt(Receipt receipt) {
    return MetadataRpc.InstallationReceipt.newBuilder().setHistoryId(receipt.historyId().toString())
        .setInstallation(report(receipt.installation()))
        .setServer(MembershipRpcCodec.participant(receipt.installation().installed().server())).build();
  }

  static Receipt receipt(MetadataRpc.InstallationReceipt receipt) {
    MetadataRpcCodec.rejectUnknown(receipt);
    require(receipt.hasInstallation() && receipt.hasServer(), "Installation receipt identity required");
    ParticipantToken server = responseServer(receipt.getServer());
    UUID history = UUID.fromString(receipt.getHistoryId());
    require(history.equals(server.historyId()), "Receipt server history differs");
    return new Receipt(history, report(receipt.getInstallation(), server));
  }

  static MetadataRpc.InstallationWorkResult work(WorkResult result)
      throws IOException {
    MetadataRpc.InstallationWorkResult.Builder value = MetadataRpc.InstallationWorkResult.newBuilder()
        .setStatus(MetadataRpc.ResultStatus.valueOf(result.status().name()))
        .setRevision(MetadataRpc.Revision.newBuilder().setHistoryId(result.revision().historyId().toString())
            .setIndex(result.revision().index())).setDetail(result.detail());
    Work work = result.work();
    if (work != null) {
      value.setWork(MetadataRpc.InstallationWork.newBuilder().setOperationId(work.operationId().toString())
          .setAttempt(work.attempt()).setOwner(ControllerRpcCodec.owner(work.owner()))
          .setTarget(MetadataRpc.InstalledMaterialization.newBuilder().setTarget(target(work.target()))
              .setServer(MembershipRpcCodec.participant(work.target().server())))
          .setContent(OfflineSegmentRpcCodec.content(work.content())).setTable(MetadataRpcCodec.record(work.table()))
          .setSchema(MetadataRpcCodec.record(work.schema())).setForceDownload(work.forceDownload())
          .setInitialLoad(work.initialLoad()).setReconcile(work.reconcile()));
    }
    return value.build();
  }

  static WorkResult work(MetadataRpc.InstallationWorkResult result)
      throws IOException {
    MetadataRpcCodec.rejectUnknown(result);
    require(result.hasRevision() && result.getRevision().getIndex() >= 0, "Installation work revision required");
    Status status = Status.valueOf(result.getStatus().name());
    require(result.hasWork() == (status == Status.OK), "Work presence differs from lookup status");
    UUID history = UUID.fromString(result.getRevision().getHistoryId());
    Work work = null;
    if (result.hasWork()) {
      MetadataRpc.InstallationWork value = result.getWork();
      require(value.getAttempt() > 0 && value.hasOwner() && value.hasTarget() && value.getTarget().hasTarget()
          && value.getTarget().hasServer() && value.hasContent() && value.hasTable() && value.hasSchema()
          && !(value.getInitialLoad() && value.getReconcile()),
          "Complete installation work required");
      ParticipantToken server = responseServer(value.getTarget().getServer());
      require(history.equals(server.historyId()), "Installation work history differs");
      Materialization target = target(value.getTarget().getTarget(), server);
      CatalogRecord table = MetadataRpcCodec.record(value.getTable());
      CatalogRecord schema = MetadataRpcCodec.record(value.getSchema());
      require(table.kind() == RecordKind.TABLE_CONFIG && schema.kind() == RecordKind.SCHEMA
          && !table.deleted() && !schema.deleted() && table.objectId().equals(target.compatibility().table().objectId())
          && table.version() == target.compatibility().table().version()
          && schema.objectId().equals(target.compatibility().schema().objectId())
          && schema.version() == target.compatibility().schema().version(), "Work configuration versions differ");
      Content content = OfflineSegmentRpcCodec.content(value.getContent());
      OwnerToken owner = ControllerRpcCodec.owner(value.getOwner());
      require(owner.historyId().equals(history) && content.tableName().equals(target.tableName())
          && content.tableObjectId().equals(target.tableObjectId()) && table.name().equals(target.tableName())
          && content.segmentName().equals(target.segmentName())
          && content.segmentObjectId().equals(target.segmentObjectId())
          && content.contentSha256().equals(target.contentSha256()), "Work owner, content or table identity differs");
      work = new Work(UUID.fromString(value.getOperationId()), value.getAttempt(), owner, target, content,
          table, schema, value.getForceDownload(), value.getInitialLoad(), value.getReconcile());
    }
    return new WorkResult(status, new Revision(history, result.getRevision().getIndex()), result.getDetail(), work);
  }

  private static ParticipantToken responseServer(MetadataRpc.ParticipantToken value) {
    ParticipantToken server = MembershipRpcCodec.participant(value);
    identifier(server.participantId());
    require(server.role() == Role.SERVER && server.registrationEpoch() > 0,
        "Complete SERVER response identity required");
    return server;
  }

  private static void identifier(String value) {
    require(!value.isBlank() && value.length() <= 1024 && value.chars().noneMatch(Character::isISOControl),
        "Invalid installation identifier");
  }

  private static void require(boolean valid, String message) {
    if (!valid) {
      throw new IllegalArgumentException(message);
    }
  }
}
