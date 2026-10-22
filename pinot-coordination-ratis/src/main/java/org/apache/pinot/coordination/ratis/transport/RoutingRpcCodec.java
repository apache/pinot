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
import org.apache.pinot.coordination.ratis.metadata.OfflineRoutingProtocol;
import org.apache.pinot.coordination.ratis.metadata.OfflineRoutingProtocol.RoutingSnapshot;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.transport.ParticipantAccessPolicy.ParticipantIdentity;

import static com.google.common.base.Preconditions.checkArgument;


/// Closed, table-scoped broker planning vocabulary. The authenticated certificate supplies the stable broker ID;
/// the request can select only its session and one table or authorized discovery.
final class RoutingRpcCodec {
  private RoutingRpcCodec() {
  }

  static ParticipantToken broker(MetadataRpc.RoutingSnapshotRequest request, ParticipantIdentity identity) {
    MetadataRpcCodec.rejectUnknown(request);
    checkArgument(identity.role() == Role.BROKER && request.hasSession()
        && request.getSession().getRegistrationEpoch() > 0, "Current BROKER session required");
    checkArgument(request.getMaxRecords() > 0
        && request.getMaxRecords() <= OfflineRoutingProtocol.MAX_SNAPSHOT_RECORDS
        && request.getMaxBytes() >= OfflineRoutingProtocol.MIN_SNAPSHOT_BYTES
        && request.getMaxBytes() <= OfflineRoutingProtocol.MAX_SNAPSHOT_BYTES
        && request.getTableName().length() <= 1024, "Invalid routing projection bounds");
    return new ParticipantToken(UUID.fromString(request.getHistoryId()), identity.participantId(), Role.BROKER,
        UUID.fromString(request.getSession().getIncarnation()), request.getSession().getRegistrationEpoch());
  }

  static MetadataRpc.RoutingSnapshot snapshot(RoutingSnapshot snapshot) throws IOException {
    var response = MetadataRpc.RoutingSnapshot.newBuilder()
        .setStatus(MetadataRpc.ResultStatus.valueOf(snapshot.status().name())).setDetail(snapshot.detail())
        .setRevision(MetadataRpc.Revision.newBuilder().setHistoryId(snapshot.revision().historyId().toString())
            .setIndex(snapshot.revision().index())).addAllTables(snapshot.tables());
    if (snapshot.participant() != null) {
      response.setParticipant(MembershipRpcCodec.participant(snapshot.participant()));
    }
    if (snapshot.control() != null) {
      response.setControl(MembershipRpcCodec.control(snapshot.control()));
    }
    if (snapshot.table() != null) {
      var table = snapshot.table();
      var encodedTable = MetadataRpc.TableRouting.newBuilder().setTableName(table.tableName())
          .setTable(OfflineSegmentRpcCodec.version(table.table())).setSchemaName(table.schemaName())
          .setSchema(OfflineSegmentRpcCodec.version(table.schema()));
      if (table.tableConfig() != null) {
        encodedTable.setTableConfig(MetadataRpcCodec.record(table.tableConfig()));
      }
      if (table.schemaRecord() != null) {
        encodedTable.setSchemaRecord(MetadataRpcCodec.record(table.schemaRecord()));
      }
      for (var server : table.serverAuthorities()) {
        encodedTable.addServerAuthorities(MembershipRpcCodec.participant(server));
      }
      for (var segment : table.segments()) {
        var encodedSegment = MetadataRpc.SegmentRouting.newBuilder().setName(segment.name())
            .setContent(OfflineSegmentRpcCodec.version(segment.content())).setContentSha256(segment.contentSha256());
        for (var replica : segment.replicas()) {
          var encodedReplica = MetadataRpc.ReplicaRouting.newBuilder()
              .setServer(MembershipRpcCodec.participant(replica.server()))
              .setAssignment(OfflineSegmentRpcCodec.version(replica.assignment()))
              .setAssignmentEpoch(replica.assignmentEpoch())
              .setMaterializationId(replica.materializationId().toString())
              .setInstallationId(replica.installationId().toString());
          if (replica.endpoint() != null) {
            encodedReplica.setEndpoint(replica.endpoint());
          }
          encodedSegment.addReplicas(encodedReplica);
        }
        encodedTable.addSegments(encodedSegment);
      }
      response.setTable(encodedTable);
    }
    return response.build();
  }
}
