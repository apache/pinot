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

import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;


/// Complete bounded OFFLINE routing observations, never serving grants. Every current segment is represented,
/// including segments without an installed replica. A null endpoint means installed evidence is not selectable.
/// Planning includes only the authorized table's exact config/schema and the membership of its installed candidates.
/// Archive locations, cluster configuration and unrelated participants are deliberately not exposed.
public final class OfflineRoutingProtocol {
  public static final int MAX_SNAPSHOT_RECORDS = 4096;
  public static final int MIN_SNAPSHOT_BYTES = 1024;
  public static final int MAX_SNAPSHOT_BYTES = MetadataProtocol.MAX_CHANGE_BYTES;

  private OfflineRoutingProtocol() {
  }

  public record ReplicaRouting(ParticipantToken server, ObjectVersion assignment, long assignmentEpoch,
                               UUID materializationId, UUID installationId, @Nullable String endpoint) {
  }

  public record SegmentRouting(String name, ObjectVersion content, String contentSha256,
                               List<ReplicaRouting> replicas) {
    public SegmentRouting {
      replicas = List.copyOf(replicas);
    }
  }

  public record TableRouting(String tableName, ObjectVersion table, String schemaName, ObjectVersion schema,
                             List<SegmentRouting> segments, @Nullable CatalogRecord tableConfig,
                             @Nullable CatalogRecord schemaRecord, List<ParticipantState> serverAuthorities) {
    /// Legacy observations lack planning inputs; consumers must not use them for native query compilation.
    public TableRouting(String tableName, ObjectVersion table, String schemaName, ObjectVersion schema,
        List<SegmentRouting> segments) {
      this(tableName, table, schemaName, schema, segments, null, null, List.of());
    }

    public TableRouting {
      segments = List.copyOf(segments);
      serverAuthorities = List.copyOf(serverAuthorities);
    }
  }

  /// Empty requested table name discovers authorized tables. A table image is complete at this revision; consumers
  /// must atomically replace it, reject incomplete coverage and separately enforce query lifetime and serving fences.
  public record RoutingSnapshot(Status status, Revision revision, String detail, @Nullable ParticipantState participant,
                                @Nullable ControlState control, List<String> tables, @Nullable TableRouting table) {
    public RoutingSnapshot {
      tables = List.copyOf(tables);
    }
  }
}
