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

import java.net.URI;
import java.util.Map;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Assignment;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Compatibility;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.LoaderDescriptor;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;


/// Typed read/feed projections of immutable segment manifests and desired replica assignments. This codec exposes
/// no command vocabulary and cannot publish content, assignments or observed readiness.
final class OfflineSegmentRpcCodec {
  private OfflineSegmentRpcCodec() {
  }

  static MetadataRpc.OfflineSegmentContent content(Content content) {
    LoaderDescriptor loader = content.loader();
    MetadataRpc.SegmentLoaderDescriptor.Builder descriptor = MetadataRpc.SegmentLoaderDescriptor.newBuilder()
        .setCrc(loader.crc()).setDataCrc(loader.dataCrc()).setUseDataCrc(loader.useDataCrc())
        .setCreationTimeMs(loader.creationTimeMs()).setPushTimeMs(loader.pushTimeMs())
        .putAllCustomConfigs(loader.customConfigs());
    if (loader.tier() != null) {
      descriptor.setTier(loader.tier());
    }
    if (loader.crypterName() != null) {
      descriptor.setCrypterName(loader.crypterName());
    }
    return MetadataRpc.OfflineSegmentContent.newBuilder().setTableName(content.tableName())
        .setTableObjectId(content.tableObjectId().toString()).setSegmentName(content.segmentName())
        .setSegmentObjectId(content.segmentObjectId().toString()).setContentSha256(content.contentSha256())
        .setDownloadUri(content.downloadUri()).setLoader(descriptor).build();
  }

  static Content content(MetadataRpc.OfflineSegmentContent content) {
    MetadataRpcCodec.rejectUnknown(content);
    require(content.hasLoader(), "Segment loader descriptor required");
    identifier(content.getTableName(), 1024);
    identifier(content.getSegmentName(), 1024);
    identifier(content.getDownloadUri(), 8192);
    require(content.getContentSha256().matches("[0-9a-f]{64}"), "Invalid content digest");
    URI uri = URI.create(content.getDownloadUri());
    require(uri.isAbsolute() && uri.getFragment() == null, "Invalid immutable content URI");
    MetadataRpc.SegmentLoaderDescriptor loader = content.getLoader();
    require(loader.getCrc() >= 0 && loader.getDataCrc() >= -1
        && (!loader.getUseDataCrc() || loader.getDataCrc() >= 0) && loader.getCreationTimeMs() >= -1
        && loader.getPushTimeMs() >= -1 && loader.getCustomConfigsCount() <= OfflineSegmentProtocol.MAX_CUSTOM_CONFIGS,
        "Invalid loader descriptor bounds");
    if (loader.hasTier()) {
      identifier(loader.getTier(), 1024);
    }
    if (loader.hasCrypterName()) {
      identifier(loader.getCrypterName(), 1024);
    }
    for (Map.Entry<String, String> entry : loader.getCustomConfigsMap().entrySet()) {
      identifier(entry.getKey(), 1024);
      require(entry.getValue().length() <= 4096, "Loader custom value exceeds supported bound");
    }
    return new Content(content.getTableName(), UUID.fromString(content.getTableObjectId()), content.getSegmentName(),
        UUID.fromString(content.getSegmentObjectId()), content.getContentSha256(), content.getDownloadUri(),
        new LoaderDescriptor(loader.getCrc(), loader.getDataCrc(), loader.getUseDataCrc(),
            loader.hasTier() ? loader.getTier() : null, loader.hasCrypterName() ? loader.getCrypterName() : null,
            loader.getCreationTimeMs(), loader.getPushTimeMs(), loader.getCustomConfigsMap()));
  }

  static MetadataRpc.OfflineReplicaAssignment assignment(Assignment assignment) {
    return MetadataRpc.OfflineReplicaAssignment.newBuilder().setTableName(assignment.tableName())
        .setTableObjectId(assignment.tableObjectId().toString()).setSegmentName(assignment.segmentName())
        .setSegmentObjectId(assignment.segmentObjectId().toString()).setContentSha256(assignment.contentSha256())
        .setAssignmentEpoch(assignment.assignmentEpoch()).setServer(MembershipRpcCodec.participant(assignment.server()))
        .setMaterializationId(assignment.materializationId().toString()).setDesiredOnline(assignment.desiredOnline())
        .setCompatibility(MetadataRpc.MaterializationCompatibility.newBuilder()
            .setTable(version(assignment.compatibility().table()))
            .setSchema(version(assignment.compatibility().schema())))
        .build();
  }

  static Assignment assignment(MetadataRpc.OfflineReplicaAssignment assignment) {
    MetadataRpcCodec.rejectUnknown(assignment);
    require(assignment.hasServer() && assignment.hasCompatibility() && assignment.getCompatibility().hasTable()
        && assignment.getCompatibility().hasSchema() && assignment.getAssignmentEpoch() > 0,
        "Complete replica assignment required");
    identifier(assignment.getTableName(), 1024);
    identifier(assignment.getSegmentName(), 1024);
    require(assignment.getContentSha256().matches("[0-9a-f]{64}"), "Invalid content digest");
    ParticipantToken server = MembershipRpcCodec.participant(assignment.getServer());
    identifier(server.participantId(), 1024);
    require(server.role() == Role.SERVER && server.registrationEpoch() > 0, "Invalid server registration");
    ObjectVersion table = version(assignment.getCompatibility().getTable());
    ObjectVersion schema = version(assignment.getCompatibility().getSchema());
    UUID tableId = UUID.fromString(assignment.getTableObjectId());
    UUID segmentId = UUID.fromString(assignment.getSegmentObjectId());
    UUID materializationId = UUID.fromString(assignment.getMaterializationId());
    require(server.historyId().equals(table.historyId()) && server.historyId().equals(schema.historyId())
        && table.objectId().equals(tableId) && !table.objectId().equals(schema.objectId()),
        "Replica compatibility identity differs");
    UUID objectId = OfflineSegmentProtocol.assignmentObjectId(server.historyId(), segmentId, server.participantId());
    require(OfflineSegmentProtocol.materializationId(server.historyId(), objectId, assignment.getAssignmentEpoch())
        .equals(materializationId), "Replica materialization identity differs");
    return new Assignment(assignment.getTableName(), tableId, assignment.getSegmentName(), segmentId,
        assignment.getContentSha256(), assignment.getAssignmentEpoch(), server, materializationId,
        new Compatibility(table, schema), assignment.getDesiredOnline());
  }

  static MetadataRpc.ObjectVersion version(ObjectVersion version) {
    return MetadataRpc.ObjectVersion.newBuilder().setHistoryId(version.historyId().toString())
        .setObjectId(version.objectId().toString()).setVersion(version.version()).build();
  }

  static ObjectVersion version(MetadataRpc.ObjectVersion version) {
    require(version.getVersion() > 0, "Positive compatibility version required");
    return new ObjectVersion(UUID.fromString(version.getHistoryId()), UUID.fromString(version.getObjectId()),
        version.getVersion());
  }

  private static void identifier(String value, int maximum) {
    require(!value.isBlank() && value.length() <= maximum && value.chars().noneMatch(Character::isISOControl),
        "Invalid offline segment identifier");
  }

  private static void require(boolean valid, String message) {
    if (!valid) {
      throw new IllegalArgumentException(message);
    }
  }
}
