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
package org.apache.pinot.coordination.ratis.client;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.TableDataManagerMetadata;
import org.apache.pinot.common.metadata.segment.SegmentLoadMetadata;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Assignment;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.LoaderDescriptor;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Supplies native OFFLINE loader inputs from one immutable committed image per call. The caller owns authenticated
/// image distribution and binds this adapter to one server process and table incarnation. Values are decoded into
/// detached Pinot configs; independent config-cache family reads cannot mix revisions here.
///
/// This read adapter checks desired state, not local installation or query-serving authority. A participant action
/// must still revalidate its captured identities under installation locks and publish a fenced execution observation.
public final class RatisOfflineTableMetadata implements TableDataManagerMetadata {
  private final Supplier<MetadataImage> _images;
  private final ParticipantToken _server;
  private final UUID _tableObjectId;

  public RatisOfflineTableMetadata(Supplier<MetadataImage> images, ParticipantToken server, UUID tableObjectId) {
    _images = Objects.requireNonNull(images, "images");
    _server = Objects.requireNonNull(server, "server");
    _tableObjectId = Objects.requireNonNull(tableObjectId, "tableObjectId");
    if (server.role() != Role.SERVER || server.historyId() == null || server.incarnation() == null
        || server.registrationEpoch() <= 0 || server.participantId() == null || server.participantId().isBlank()) {
      throw new IllegalArgumentException("A complete SERVER participant token is required");
    }
  }

  @Override
  public TableSnapshot fetchTable(String tableNameWithType) {
    try {
      MetadataImage image = capture(false);
      return table(image, tableNameWithType);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot decode the committed offline table image", e);
    }
  }

  @Nullable
  @Override
  public SegmentSnapshot fetchSegment(String tableNameWithType, String segmentName) {
    try {
      MetadataImage image = capture(true);
      TableSnapshot table = table(image, tableNameWithType);
      CatalogRecord contentRecord = image.records(RecordKind.OFFLINE_SEGMENT_CONTENT)
          .get(OfflineSegmentProtocol.contentName(_tableObjectId, segmentName));
      if (contentRecord == null) {
        return null;
      }
      Content content = OfflineSegmentProtocol.decodeContent(contentRecord.value());
      require(content.tableObjectId().equals(_tableObjectId) && content.tableName().equals(tableNameWithType)
          && content.segmentName().equals(segmentName) && content.segmentObjectId().equals(contentRecord.objectId()),
          "Offline content identity differs from the requested table/segment");
      CatalogRecord assignmentRecord = image.records(RecordKind.OFFLINE_REPLICA_ASSIGNMENT)
          .get(OfflineSegmentProtocol.assignmentName(content.segmentObjectId(), _server.participantId()));
      if (assignmentRecord == null) {
        return null;
      }
      Assignment assignment = OfflineSegmentProtocol.decodeAssignment(assignmentRecord.value());
      require(assignment.server().equals(_server), "Desired replica belongs to another server registration");
      if (!assignment.desiredOnline()) {
        return null;
      }
      require(assignment.tableObjectId().equals(_tableObjectId) && assignment.tableName().equals(tableNameWithType)
          && assignment.segmentObjectId().equals(content.segmentObjectId())
          && assignment.segmentName().equals(segmentName)
          && assignment.contentSha256().equals(content.contentSha256())
          && assignment.assignmentEpoch() == assignmentRecord.version()
          && assignment.materializationId().equals(OfflineSegmentProtocol.materializationId(_server.historyId(),
              assignmentRecord.objectId(), assignment.assignmentEpoch())), "Desired replica content or epoch differs");
      currentVersion(image, RecordKind.TABLE_CONFIG, tableNameWithType, assignment.compatibility().table());
      currentVersion(image, RecordKind.SCHEMA, table.schema().getSchemaName(), assignment.compatibility().schema());
      LoaderDescriptor loader = content.loader();
      SegmentLoadMetadata segment = new SegmentLoadMetadata(segmentName, loader.crc(), loader.dataCrc(),
          loader.useDataCrc(), content.downloadUri(), loader.tier(), loader.crypterName(), loader.creationTimeMs(),
          loader.pushTimeMs(), loader.customConfigs(), true);
      return new SegmentSnapshot(table.tableConfig(), table.schema(), segment);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot decode the committed offline segment image", e);
    }
  }

  private MetadataImage capture(boolean requireReady)
      throws IOException {
    MetadataImage image = _images.get();
    require(image != null && _server.historyId().equals(image.cursor().historyId()),
        "Offline metadata image is unavailable or belongs to another history");
    CatalogRecord memberRecord = image.records(RecordKind.PARTICIPANT).get(_server.participantId());
    CatalogRecord controlRecord = image.records(RecordKind.MEMBERSHIP_CONTROL).get(MembershipProtocol.CONTROL_NAME);
    require(memberRecord != null && controlRecord != null, "Offline metadata image omits membership authority");
    ParticipantState member = MembershipProtocol.decodeParticipant(memberRecord.value());
    ControlState control = MembershipProtocol.decodeControl(controlRecord.value());
    require(member.participant().equals(_server) && !member.retired() && !member.draining()
        && (!requireReady || member.ready()) && control.controlEpoch() != null && control.policy() != null
        && member.expiryRound() > control.round(), "Server registration is fenced or its committed lease is invalid");
    return image;
  }

  private TableSnapshot table(MetadataImage image, String tableNameWithType)
      throws IOException {
    require(TableNameBuilder.getTableTypeFromTableName(tableNameWithType) == TableType.OFFLINE,
        "Native offline metadata requires an OFFLINE table");
    CatalogRecord tableRecord = image.records(RecordKind.TABLE_CONFIG).get(tableNameWithType);
    require(tableRecord != null && tableRecord.objectId().equals(_tableObjectId),
        "Offline table was deleted or its incarnation changed");
    String schemaName = TableNameBuilder.extractRawTableName(tableNameWithType);
    CatalogRecord schemaRecord = image.records(RecordKind.SCHEMA).get(schemaName);
    require(schemaRecord != null, "Offline table schema is absent");
    TableConfig table = JsonUtils.jsonNodeToObject(tableRecord.value(), TableConfig.class);
    Schema schema = JsonUtils.jsonNodeToObject(schemaRecord.value(), Schema.class);
    require(tableNameWithType.equals(table.getTableName()) && table.getTableType() == TableType.OFFLINE
        && schemaName.equals(schema.getSchemaName()), "Offline table/schema payload identity differs");
    return new TableSnapshot(table, schema);
  }

  private void currentVersion(MetadataImage image, RecordKind kind, String name, ObjectVersion expected) {
    CatalogRecord record = image.records(kind).get(name);
    require(expected != null && _server.historyId().equals(expected.historyId()) && record != null
        && expected.objectId().equals(record.objectId()) && expected.version() == record.version(),
        "Offline assignment compatibility changed; reconcile the desired materialization before loading");
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalStateException(message);
    }
  }
}
