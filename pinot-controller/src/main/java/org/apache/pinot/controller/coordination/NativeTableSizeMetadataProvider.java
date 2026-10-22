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

import com.google.common.collect.HashBiMap;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.pinot.common.restlet.resources.NativeTableSizeIdentity;
import org.apache.pinot.common.restlet.resources.NativeTableSizeIdentity.SegmentIdentity;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.controller.util.TableSizeMetadataProvider;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Assignment;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Captures one native table's expected replicas and validates measured server reports against those exact identities.
/// Invalid/missing reports remain missing sizes. Desired assignments never count as observations of installed bytes.
/// Participant-advertised URLs receive controller credentials only after an exact endpoint approval in cluster config;
/// HTTPS advertisements never fall back to HTTP. The approval is rechecked after collection.
final class NativeTableSizeMetadataProvider implements TableSizeMetadataProvider {
  private static final int MAX_SERVERS = 128;
  static final String ENDPOINT_APPROVAL_PREFIX = "pinot.coordination.ratis.server.admin.endpoint.";
  private final NativeControllerMetadata.Snapshot _snapshot;
  private final String _tableName;
  @Nullable
  private final CatalogRecord _table;
  @Nullable
  private final CatalogRecord _schema;
  private final Map<String, CatalogRecord> _contents = new HashMap<>();
  private final Map<String, Map<String, CatalogRecord>> _assignments = new HashMap<>();
  private final Map<String, ParticipantState> _participants = new HashMap<>();
  private final Replicas _replicas;
  private final int _replication;
  private final AtomicLong _maxReportedRevision = new AtomicLong();

  NativeTableSizeMetadataProvider(NativeControllerMetadata.Snapshot snapshot, String tableName,
      @Nullable AuthProvider auth) throws IOException {
    _snapshot = snapshot;
    _tableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    _table = snapshot.tables().get(_tableName);
    Map<String, List<String>> serverToSegments = new HashMap<>();
    var endpoints = HashBiMap.<String, String>create();
    if (_table == null) {
      _schema = null;
      _replication = 1;
      _replicas = new Replicas(serverToSegments, endpoints, auth);
      return;
    }
    TableConfig config = JsonUtils.jsonNodeToObject(_table.value(), TableConfig.class);
    _replication = config.getReplication();
    _schema = snapshot.schemas().get(TableNameBuilder.extractRawTableName(_tableName));
    if (_replication <= 0 || _schema == null || !_tableName.equals(config.getTableName())) {
      throw new IOException("Native table configuration is incomplete");
    }
    for (CatalogRecord record : snapshot.contents().values()) {
      Content content = OfflineSegmentProtocol.decodeContent(record.value());
      if (content.tableObjectId().equals(_table.objectId())) {
        if (!_tableName.equals(content.tableName()) || !record.objectId().equals(content.segmentObjectId())
            || _contents.putIfAbsent(content.segmentName(), record) != null) {
          throw new IOException("Native segment content identity is ambiguous");
        }
      }
    }
    Map<String, Integer> assigned = new HashMap<>();
    for (CatalogRecord record : snapshot.assignments().values()) {
      Assignment assignment = OfflineSegmentProtocol.decodeAssignment(record.value());
      if (!assignment.tableObjectId().equals(_table.objectId()) || !assignment.desiredOnline()) {
        continue;
      }
      CatalogRecord contentRecord = _contents.get(assignment.segmentName());
      if (contentRecord == null || !_tableName.equals(assignment.tableName())
          || !contentRecord.objectId().equals(assignment.segmentObjectId())
          || !OfflineSegmentProtocol.decodeContent(contentRecord.value()).contentSha256()
              .equals(assignment.contentSha256())
          || !matches(assignment.compatibility().table(), _table)
          || !matches(assignment.compatibility().schema(), _schema)
          || assignment.server().role() != Role.SERVER
          || !snapshot.revision().historyId().equals(assignment.server().historyId())) {
        throw new IOException("Native segment assignment requires current content and configuration");
      }
      String server = assignment.server().participantId();
      if (_assignments.computeIfAbsent(server, ignored -> new HashMap<>())
          .putIfAbsent(assignment.segmentName(), record) != null) {
        throw new IOException("Native segment assignment identity is ambiguous");
      }
      serverToSegments.computeIfAbsent(server, ignored -> new ArrayList<>()).add(assignment.segmentName());
      if (serverToSegments.size() > MAX_SERVERS) {
        throw new IOException("Native table size exceeds the server fan-out limit");
      }
      assigned.merge(assignment.segmentName(), 1, Integer::sum);
      ParticipantState participant = snapshot.participants().get(server);
      if (live(participant, snapshot) && participant.participant().equals(assignment.server())) {
        _participants.put(server, participant);
        boolean https = participant.endpoints().containsKey("adminHttps");
        String endpoint = participant.endpoints().get(https ? "adminHttps" : "adminHttp");
        String approved = snapshot.clusterConfig().get(ENDPOINT_APPROVAL_PREFIX + server);
        if (endpoint != null && endpoint.equals(approved)) {
          URI uri;
          try {
            uri = URI.create(endpoint);
          } catch (IllegalArgumentException e) {
            throw new IOException("Invalid native server admin endpoint", e);
          }
          if (!(https ? "https" : "http").equals(uri.getScheme()) || uri.getHost() == null
              || uri.getPort() <= 0 || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null
              || (uri.getPath() != null && !uri.getPath().isEmpty())) {
            throw new IOException("Invalid native server admin endpoint");
          }
          if (endpoints.containsValue(endpoint) && !endpoint.equals(endpoints.get(server))) {
            throw new IOException("Native server registrations share an admin endpoint");
          }
          endpoints.put(server, endpoint);
        }
      }
    }
    if (_contents.keySet().stream().anyMatch(segment -> assigned.getOrDefault(segment, 0) < _replication)) {
      throw new IOException("Native segment placement is incomplete; table size cannot be estimated");
    }
    _replicas = new Replicas(serverToSegments, endpoints, auth);
  }

  boolean empty() {
    return _table != null && _contents.isEmpty();
  }

  @Override
  public TableSizeMetadataProvider.Snapshot readTableSizes(String tableName, boolean includeReplacedSegments) {
    return new TableSizeMetadataProvider.Snapshot(_table == null ? null
        : new Table(_tableName, _replication, false, _replicas), null);
  }

  @Override
  public Replicas readReplicaAssignments(String tableNameWithType, boolean includeReplacedSegments) {
    return _replicas;
  }

  @Override
  public boolean canPublishMetrics(String tableNameWithType) {
    // REST reads do not own the table shard or run the authoritative periodic metrics publisher.
    return false;
  }

  @Override
  public Map<String, TableSizeInfo> validateServerReports(Replicas replicas, Map<String, TableSizeInfo> reports) {
    Map<String, TableSizeInfo> accepted = new HashMap<>();
    reports.forEach((server, report) -> {
      try {
        if (validReport(server, report)) {
          accepted.put(server, report);
        }
      } catch (IOException | RuntimeException ignored) {
        // A malformed or stale response is unavailable, never a successful zero-byte measurement.
      }
    });
    // Validate the arithmetic used by the shared estimator before publishing any contribution. This also covers
    // missing replicas, whose estimates multiply the largest surviving report for that segment.
    try {
      Map<String, Long> maximums = new HashMap<>();
      long reported = 0;
      for (TableSizeInfo report : accepted.values()) {
        for (SegmentSizeInfo segment : report.getSegments()) {
          reported = Math.addExact(reported, segment.getDiskSizeInBytes());
          maximums.merge(segment.getSegmentName(), segment.getDiskSizeInBytes(), Math::max);
        }
      }
      long estimated = reported;
      for (Map.Entry<String, List<String>> server : _replicas.serverToSegments().entrySet()) {
        Set<String> observed = new HashSet<>();
        TableSizeInfo report = accepted.get(server.getKey());
        if (report != null) {
          report.getSegments().forEach(segment -> observed.add(segment.getSegmentName()));
        }
        for (String segment : server.getValue()) {
          if (!observed.contains(segment)) {
            estimated = Math.addExact(estimated, maximums.getOrDefault(segment, 0L));
          }
        }
      }
    } catch (ArithmeticException e) {
      return Map.of();
    }
    return accepted;
  }

  private boolean validReport(String server, TableSizeInfo report) throws IOException {
    ParticipantState participant = _participants.get(server);
    NativeTableSizeIdentity nativeMetadata = report.getNativeMetadata();
    if (participant == null || !_replicas.serverEndpoints().containsKey(server)
        || nativeMetadata == null || !_tableName.equals(report.getTableName())
        || report.getMetadataVersion() != TableSizeInfo.CURRENT_METADATA_VERSION
        || report.getSegments() == null || nativeMetadata.getSegments() == null) {
      return false;
    }
    ParticipantToken token = participant.participant();
    if (!token.historyId().equals(nativeMetadata.getHistoryId()) || !server.equals(nativeMetadata.getParticipantId())
        || !token.incarnation().equals(nativeMetadata.getIncarnation())
        || token.registrationEpoch() != nativeMetadata.getRegistrationEpoch() || nativeMetadata.getRevision() <= 0) {
      return false;
    }
    Set<String> seen = new HashSet<>();
    long reported = 0;
    for (SegmentSizeInfo size : report.getSegments()) {
      if (size == null || !seen.add(size.getSegmentName()) || size.getDiskSizeInBytes() < 0) {
        return false;
      }
      reported = Math.addExact(reported, size.getDiskSizeInBytes());
      String name = size.getSegmentName();
      CatalogRecord contentRecord = _contents.get(name);
      CatalogRecord assignmentRecord = _assignments.getOrDefault(server, Map.of()).get(name);
      SegmentIdentity identity = nativeMetadata.getSegments().get(name);
      if (contentRecord == null || assignmentRecord == null || identity == null) {
        return false;
      }
      Content content = OfflineSegmentProtocol.decodeContent(contentRecord.value());
      Assignment assignment = OfflineSegmentProtocol.decodeAssignment(assignmentRecord.value());
      if (!content.tableObjectId().equals(identity.getTableObjectId())
          || _table.version() != identity.getTableVersion()
          || !assignment.compatibility().schema().objectId().equals(identity.getSchemaObjectId())
          || assignment.compatibility().schema().version() != identity.getSchemaVersion()
          || !content.segmentObjectId().equals(identity.getSegmentObjectId())
          || contentRecord.version() != identity.getContentVersion()
          || !content.contentSha256().equals(identity.getContentSha256())
          || !assignmentRecord.objectId().equals(identity.getAssignmentObjectId())
          || assignment.assignmentEpoch() != identity.getAssignmentEpoch()
          || !assignment.materializationId().equals(identity.getMaterializationId())
          || !assignment.server().equals(token)
          || nativeMetadata.getRevision() < Math.max(Math.max(_table.lastModifiedIndex(), _schema.lastModifiedIndex()),
              Math.max(contentRecord.lastModifiedIndex(), assignmentRecord.lastModifiedIndex()))) {
        return false;
      }
    }
    if (!seen.equals(nativeMetadata.getSegments().keySet()) || report.getDiskSizeInBytes() != reported) {
      return false;
    }
    _maxReportedRevision.accumulateAndGet(nativeMetadata.getRevision(), Math::max);
    return true;
  }

  /// A request may span a lease expiry or reassignment. Refuse attribution if any captured dependency changed.
  void validateCurrent(NativeControllerMetadata.Snapshot current) throws IOException {
    if (!current.revision().historyId().equals(_snapshot.revision().historyId())
        || current.revision().index() < Math.max(_snapshot.revision().index(), _maxReportedRevision.get())
        || !Objects.equals(_table, current.tables().get(_tableName))
        || _schema != null && !Objects.equals(_schema, current.schemas().get(_schema.name()))) {
      throw new IOException("Native table changed while measuring size");
    }
    for (CatalogRecord content : _contents.values()) {
      if (!content.equals(current.contents().get(content.name()))) {
        throw new IOException("Native content changed while measuring size");
      }
    }
    for (Map<String, CatalogRecord> assignments : _assignments.values()) {
      for (CatalogRecord assignment : assignments.values()) {
        if (!assignment.equals(current.assignments().get(assignment.name()))) {
          throw new IOException("Native assignment changed while measuring size");
        }
      }
    }
    for (Map.Entry<String, ParticipantState> entry : _participants.entrySet()) {
      ParticipantState latest = current.participants().get(entry.getKey());
      if (!live(latest, current) || !latest.participant().equals(entry.getValue().participant())
          || !latest.endpoints().equals(entry.getValue().endpoints())
          || !Objects.equals(current.clusterConfig().get(ENDPOINT_APPROVAL_PREFIX + entry.getKey()),
              _snapshot.clusterConfig().get(ENDPOINT_APPROVAL_PREFIX + entry.getKey()))) {
        throw new IOException("Native server registration changed while measuring size");
      }
    }
    // New content or assignment also changes the scope of the requested measurement.
    NativeTableSizeMetadataProvider latest = new NativeTableSizeMetadataProvider(current, _tableName, null);
    if (!latest._contents.equals(_contents) || !latest._assignments.equals(_assignments)) {
      throw new IOException("Native table placement changed while measuring size");
    }
  }

  private boolean matches(ObjectVersion version, CatalogRecord record) {
    return version != null && _snapshot.revision().historyId().equals(version.historyId())
        && record.objectId().equals(version.objectId()) && record.version() == version.version();
  }

  private static boolean live(ParticipantState participant, NativeControllerMetadata.Snapshot snapshot) {
    return participant != null && participant.participant().role() == Role.SERVER && participant.ready()
        && !participant.draining() && !participant.retired() && participant.expiryRound() > snapshot.control().round();
  }
}
