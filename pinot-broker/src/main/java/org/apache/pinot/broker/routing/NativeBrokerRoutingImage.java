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
package org.apache.pinot.broker.routing;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ObjectVersion;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Participant;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Role;
import org.apache.pinot.common.coordination.NativeOfflineSegmentWorker;
import org.apache.pinot.common.request.context.NativeQueryContext;
import org.apache.pinot.common.request.context.NativeQueryContext.ProcessIdentity;
import org.apache.pinot.common.request.context.NativeQueryContext.Segment;
import org.apache.pinot.common.request.context.NativeQueryDeadline;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ParticipantRole;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ParticipantToken;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ReplicaRouting;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.RoutingSnapshot;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;


/// Immutable complete metadata for one OFFLINE table at one committed revision. Installed replicas without an
/// approved query endpoint remain visible but cannot be selected. Neither this image nor a produced request grants
/// serving authority: authenticated transport, admission, qualified deadlines and delivery fences remain mandatory.
public final class NativeBrokerRoutingImage {
  public static final int MAX_RECORDS = 4096;
  public static final int MAX_BYTES = 2 * 1024 * 1024;
  private final RoutingSnapshot _snapshot;

  public NativeBrokerRoutingImage(RoutingSnapshot snapshot, ParticipantToken broker, String tableName)
      throws IOException {
    validateHeader(snapshot, broker);
    checkArgument(snapshot.hasTable() && snapshot.getTablesCount() == 0
        && snapshot.getTable().getTableName().equals(tableName), "Routing snapshot table differs from request");
    canonicalTable(tableName);
    var table = snapshot.getTable();
    checkArgument(table.hasTable() && table.hasSchema()
        && table.getSchemaName().equals(TableNameBuilder.extractRawTableName(tableName)),
        "Routing table/schema identity is incomplete");
    Set<UUID> identities = new HashSet<>();
    checkArgument(identities.add(object(table.getTable(), broker.getHistoryId()).objectId())
        && identities.add(object(table.getSchema(), broker.getHistoryId()).objectId()),
        "Routing table/schema identities overlap");
    Set<String> names = new HashSet<>();
    Set<UUID> materializations = new HashSet<>();
    Set<UUID> installations = new HashSet<>();
    Map<String, ParticipantToken> servers = new HashMap<>();
    Map<String, String> endpoints = new HashMap<>();
    Map<String, String> endpointOwners = new HashMap<>();
    int records = 4;
    for (var segment : table.getSegmentsList()) {
      checkArgument(++records <= MAX_RECORDS && names.add(identifier(segment.getName())) && segment.hasContent()
          && identities.add(object(segment.getContent(), broker.getHistoryId()).objectId())
          && segment.getContentSha256().matches("[0-9a-f]{64}"), "Invalid or repeated routing segment");
      Set<String> replicas = new HashSet<>();
      for (ReplicaRouting replica : segment.getReplicasList()) {
        checkArgument(++records <= MAX_RECORDS && replica.hasServer() && replica.hasAssignment(),
            "Routing replica is incomplete or exceeds the record bound");
        ParticipantToken server = replica.getServer();
        participant(server, broker.getHistoryId(), ParticipantRole.SERVER);
        checkArgument(replicas.add(server.getParticipantId())
            && identities.add(object(replica.getAssignment(), broker.getHistoryId()).objectId())
            && replica.getAssignmentEpoch() == replica.getAssignment().getVersion()
            && materializations.add(uuid(replica.getMaterializationId()))
            && installations.add(uuid(replica.getInstallationId())), "Invalid or repeated routing replica");
        ParticipantToken previous = servers.putIfAbsent(server.getParticipantId(), server);
        checkArgument(previous == null || previous.equals(server), "Routing image mixes server incarnations");
        String endpoint = replica.hasEndpoint() ? replica.getEndpoint() : "";
        if (replica.hasEndpoint()) {
          endpoint(endpoint);
          String owner = endpointOwners.putIfAbsent(endpoint, server.getParticipantId());
          checkArgument(owner == null || owner.equals(server.getParticipantId()),
              "Query endpoint identifies multiple servers");
        }
        String priorEndpoint = endpoints.putIfAbsent(server.getParticipantId(), endpoint);
        checkArgument(priorEndpoint == null || priorEndpoint.equals(endpoint),
            "Routing image mixes query endpoint approvals");
      }
    }
    _snapshot = snapshot;
  }

  /// Validates the own-session discovery result. It is not a table image or evidence that any table is routable.
  public static List<String> discover(RoutingSnapshot snapshot, ParticipantToken broker) throws IOException {
    validateHeader(snapshot, broker);
    checkArgument(!snapshot.hasTable() && snapshot.getTablesCount() <= MAX_RECORDS - 2,
        "Invalid routing discovery response");
    Set<String> tables = new HashSet<>();
    for (String table : snapshot.getTablesList()) {
      canonicalTable(table);
      checkArgument(tables.add(table), "Repeated routing discovery table");
    }
    return tables.stream().sorted().toList();
  }

  public MetadataRpc.Revision revision() {
    return _snapshot.getRevision();
  }

  public int serializedBytes() {
    return _snapshot.getSerializedSize();
  }

  public String tableName() {
    return _snapshot.getTable().getTableName();
  }

  public int segmentCount() {
    return _snapshot.getTable().getSegmentsCount();
  }

  /// Selects exactly one approved replica for every segment, in deterministic order. The same originating deadline
  /// is attached to every fanout request; callers must preserve it across retries and independently qualify its clock.
  public List<ServerRequest> requests(UUID queryId, UUID attemptId, NativeQueryDeadline deadline) {
    requireNonNull(queryId, "queryId");
    requireNonNull(attemptId, "attemptId");
    requireNonNull(deadline, "deadline");
    var table = _snapshot.getTable();
    checkState(table.getSegmentsCount() > 0, "Native empty-table query execution is not supported");
    Map<String, List<Segment>> selected = new TreeMap<>();
    Map<String, ReplicaRouting> targets = new HashMap<>();
    for (var segment : table.getSegmentsList().stream()
        .sorted(Comparator.comparing(MetadataRpc.SegmentRouting::getName)).toList()) {
      ReplicaRouting replica = segment.getReplicasList().stream().filter(ReplicaRouting::hasEndpoint)
          .min(Comparator.comparing(value -> value.getServer().getParticipantId())).orElseThrow(
              () -> new IllegalStateException("No approved query replica for required segment: " + segment.getName()));
      String server = replica.getServer().getParticipantId();
      targets.put(server, replica);
      selected.computeIfAbsent(server, ignored -> new ArrayList<>()).add(new Segment(segment.getName(),
          object(segment.getContent(), _snapshot.getRevision().getHistoryId()), segment.getContentSha256(),
          object(replica.getAssignment(), _snapshot.getRevision().getHistoryId()),
          uuid(replica.getMaterializationId())));
    }
    ParticipantToken broker = _snapshot.getParticipant().getParticipant();
    ProcessIdentity process = new ProcessIdentity(broker.getParticipantId(), uuid(broker.getIncarnation()),
        broker.getRegistrationEpoch());
    List<ServerRequest> requests = new ArrayList<>();
    selected.forEach((id, segments) -> {
      ReplicaRouting replica = targets.get(id);
      ParticipantToken server = replica.getServer();
      NativeQueryContext context = new NativeQueryContext(NativeQueryContext.VERSION, uuid(broker.getHistoryId()),
          process, new Participant(id, Role.SERVER, uuid(server.getIncarnation()), server.getRegistrationEpoch()),
          queryId, attemptId, table.getTableName(), object(table.getTable(), broker.getHistoryId()),
          table.getSchemaName(), object(table.getSchema(), broker.getHistoryId()), segments, List.of())
          .withDeadline(deadline);
      requests.add(new ServerRequest(endpoint(replica.getEndpoint()), context));
    });
    return List.copyOf(requests);
  }

  /// An exact destination and identity envelope, not a dispatched or admitted query.
  public record ServerRequest(URI endpoint, NativeQueryContext context) {
  }

  private static void validateHeader(RoutingSnapshot snapshot, ParticipantToken broker) throws IOException {
    NativeOfflineSegmentWorker.validateMessage(snapshot);
    participant(broker, broker.getHistoryId(), ParticipantRole.BROKER);
    checkArgument(snapshot.getSerializedSize() <= MAX_BYTES && snapshot.getStatus() == MetadataRpc.ResultStatus.OK
        && snapshot.hasRevision() && snapshot.getRevision().getIndex() > 0
        && snapshot.getRevision().getHistoryId().equals(broker.getHistoryId())
        && snapshot.hasParticipant() && snapshot.hasControl(), "Incomplete routing snapshot");
    var state = snapshot.getParticipant();
    var control = snapshot.getControl();
    long index = snapshot.getRevision().getIndex();
    checkArgument(state.getParticipant().equals(broker) && !state.getRetired() && !state.getDraining()
        && state.getExpiryRound() > control.getRound() && state.getLastModifiedIndex() > 0
        && state.getLastModifiedIndex() <= index && state.getLastRenewalSequence() >= 0
        && state.hasAppliedRevision() && state.getAppliedRevision().getHistoryId().equals(broker.getHistoryId())
        && state.getAppliedRevision().getIndex() >= 0 && state.getAppliedRevision().getIndex() <= index,
        "Routing snapshot broker session is not live");
    checkArgument(control.hasControlEpoch() && control.hasPolicy(), "Routing control state is incomplete");
    var epoch = control.getControlEpoch();
    checkArgument(epoch.getHistoryId().equals(broker.getHistoryId()) && epoch.getRaftTerm() > 0
        && epoch.getBarrierIndex() > 0 && epoch.getBarrierIndex() <= index && !epoch.getLeaderId().isBlank()
        && control.getRound() >= 0 && control.getClockBarrierIndex() >= epoch.getBarrierIndex()
        && control.getClockBarrierIndex() <= index, "Invalid routing control barrier");
    uuid(epoch.getActivationId());
    var policy = control.getPolicy();
    checkArgument(policy.getVersion() > 0 && policy.getMinTickIntervalMillis() > 0 && policy.getLeaseRounds() >= 2
        && policy.getPermitLifetimeMillis() > 0 && policy.getMaxQueryLifetimeMillis() > 0
        && policy.getMaxRevisionLag() > 0, "Invalid routing membership policy");
  }

  private static ObjectVersion object(MetadataRpc.ObjectVersion value, String history) {
    checkArgument(value.getHistoryId().equals(history), "Routing object belongs to a different history");
    return new ObjectVersion(uuid(value.getObjectId()), value.getVersion());
  }

  private static void participant(ParticipantToken value, String history, ParticipantRole role) {
    checkArgument(value.getHistoryId().equals(history) && value.getRole() == role
        && value.getRegistrationEpoch() > 0 && value.getParticipantId().length() <= 128,
        "Invalid routing participant");
    identifier(value.getParticipantId());
    uuid(history);
    uuid(value.getIncarnation());
  }

  private static void canonicalTable(String table) {
    identifier(table);
    String raw = TableNameBuilder.extractRawTableName(table);
    int separator = raw.indexOf('.');
    checkArgument(TableNameBuilder.getTableTypeFromTableName(table) == TableType.OFFLINE && !raw.isBlank()
        && TableNameBuilder.getTableTypeFromTableName(raw) == null
        && (separator < 0 || separator > 0 && separator < raw.length() - 1 && separator == raw.lastIndexOf('.'))
        && table.chars().noneMatch(Character::isWhitespace), "Invalid canonical OFFLINE routing table");
  }

  private static String identifier(String value) {
    checkArgument(value != null && !value.isBlank() && value.length() <= 1024
        && value.chars().noneMatch(Character::isISOControl) && StandardCharsets.UTF_8.newEncoder().canEncode(value),
        "Invalid routing identifier");
    return value;
  }

  private static UUID uuid(String value) {
    UUID parsed = UUID.fromString(value);
    checkArgument(parsed.toString().equals(value), "Invalid canonical routing UUID");
    return parsed;
  }

  private static URI endpoint(String value) {
    URI uri = URI.create(value);
    checkArgument(value.length() <= 2048 && "grpcs".equals(uri.getScheme()) && uri.getHost() != null
        && uri.getHost().equals(uri.getHost().toLowerCase(Locale.ROOT)) && uri.getPort() > 0
        && uri.getPort() <= 65535 && uri.getRawUserInfo() == null
        && (uri.getRawPath() == null || uri.getRawPath().isEmpty())
        && uri.getRawQuery() == null && uri.getRawFragment() == null, "Unapproved native query endpoint");
    try {
      String canonical = new URI("grpcs", null, uri.getHost(), uri.getPort(), null, null, null).toASCIIString();
      checkArgument(value.equals(canonical), "Noncanonical native query endpoint");
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid native query endpoint", e);
    }
    return uri;
  }
}
