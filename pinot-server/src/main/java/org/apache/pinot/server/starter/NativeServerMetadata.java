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
package org.apache.pinot.server.starter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Identity;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ObjectVersion;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Role;
import org.apache.pinot.common.config.provider.RecoverableClusterConfigCache;
import org.apache.pinot.common.config.provider.TableDataManagerMetadata;
import org.apache.pinot.common.coordination.NativeOfflineSegmentWorker;
import org.apache.pinot.common.coordination.ServingPermitGuard.MembershipView;
import org.apache.pinot.common.coordination.ServingPermitGuard.ReadOnlyCapability;
import org.apache.pinot.common.metadata.segment.SegmentLoadMetadata;
import org.apache.pinot.common.request.context.NativeQueryContext;
import org.apache.pinot.common.request.context.NativeQueryContext.Segment;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.CatalogRecord;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

import static com.google.common.base.Preconditions.checkState;


/// Complete own-session metadata images, including a coherent local membership view for serving-permit validation.
/// The default remains installation-only. Loading inputs and membership metadata do not themselves grant publication
/// or query-serving authority; a provider remains bound to its table incarnation.
public final class NativeServerMetadata {
  private final MetadataRpc.ParticipantToken _server;
  private final RecoverableClusterConfigCache _clusterConfig;
  private final Set<ReadOnlyCapability> _allowedCapabilities;
  @Nullable
  private MetadataRpc.InstallationSnapshot _lastSnapshot;
  @Nullable
  private volatile Image _image;
  @Nullable
  private QueryAuthorities _lastAuthorities;
  private long _authorizationGeneration;

  private record Image(MetadataRpc.Revision revision, Map<String, CatalogRecord> tables,
                       Map<String, CatalogRecord> schemas, Map<String, CatalogRecord> contents,
                       Map<String, CatalogRecord> assignments, List<UUID> operations, MembershipView membership,
                       Map<String, String> tableTenants, Map<String, QueryBroker> queryBrokers,
                       Map<String, String> queryPrincipals, long authorizationGeneration) {
  }

  private record QueryBroker(MetadataRpc.ParticipantToken participant, Set<String> tenants) {
  }

  private record TableTenant(String objectId, String tenant) {
  }

  private record QueryAuthorities(Map<String, QueryBroker> brokers, Map<String, String> principals,
                                  Map<String, TableTenant> tables) {
  }

  public NativeServerMetadata(MetadataRpc.ParticipantToken server, RecoverableClusterConfigCache clusterConfig)
      throws IOException {
    this(server, clusterConfig, Set.of());
  }

  /// Explicitly allows the server's read-only capability in hydrated images. Admission still requires a separately
  /// qualified serving-permit guard; this constructor neither enables query listeners nor grants serving permission.
  public NativeServerMetadata(MetadataRpc.ParticipantToken server, RecoverableClusterConfigCache clusterConfig,
      Set<ReadOnlyCapability> allowedCapabilities) throws IOException {
    InstallationOutbox.validateServer(server);
    require(allowedCapabilities != null && Set.of(ReadOnlyCapability.SERVER_QUERY).containsAll(allowedCapabilities),
        "Server metadata accepts only the SERVER_QUERY capability");
    _server = server;
    _clusterConfig = clusterConfig;
    _allowedCapabilities = Set.copyOf(allowedCapabilities);
  }

  /// Validates the whole image before publishing. Listener failure invalidates the usable image; callers must fence
  /// readiness and retry hydration rather than advertise this revision or fall back to older metadata.
  public synchronized void apply(MetadataRpc.InstallationSnapshot snapshot) throws IOException {
    NativeOfflineSegmentWorker.validateMessage(snapshot);
    require(snapshot.getStatus() == MetadataRpc.ResultStatus.OK && snapshot.hasRevision()
        && snapshot.hasParticipant() && snapshot.hasControl(), "Incomplete server metadata snapshot");
    MetadataRpc.Revision revision = snapshot.getRevision();
    require(revision.getHistoryId().equals(_server.getHistoryId()) && revision.getIndex() > 0,
        "Server metadata history or revision differs");
    MetadataRpc.ParticipantState participant = snapshot.getParticipant();
    require(participant.getParticipant().equals(_server) && !participant.getRetired()
        && participant.getExpiryRound() > snapshot.getControl().getRound()
        && participant.getLastModifiedIndex() > 0 && participant.getLastModifiedIndex() <= revision.getIndex()
        && participant.getLastRenewalSequence() >= 0 && participant.hasAppliedRevision()
        && participant.getAppliedRevision().getHistoryId().equals(_server.getHistoryId())
        && participant.getAppliedRevision().getIndex() >= 0
        && participant.getAppliedRevision().getIndex() <= revision.getIndex(), "Server registration is not current");
    Set<ReadOnlyCapability> capabilities = new HashSet<>();
    for (MetadataRpc.ServingCapability capability : participant.getCapabilitiesList()) {
      ReadOnlyCapability decoded;
      try {
        decoded = ReadOnlyCapability.fromWire(capability);
      } catch (IllegalArgumentException e) {
        throw new IOException("Invalid server read-only capability", e);
      }
      require(_allowedCapabilities.contains(decoded) && capabilities.add(decoded),
          "Server metadata capability is repeated or not explicitly allowed");
    }
    require(snapshot.getControl().hasControlEpoch() && snapshot.getControl().hasPolicy()
        && snapshot.getControl().getControlEpoch().getHistoryId().equals(_server.getHistoryId()),
        "Server metadata lacks a current membership control epoch");
    MetadataRpc.MembershipControl control = snapshot.getControl();
    MetadataRpc.ControlToken epoch = control.getControlEpoch();
    require(epoch.getRaftTerm() > 0 && epoch.getBarrierIndex() > 0 && epoch.getBarrierIndex() <= revision.getIndex()
        && !epoch.getLeaderId().isBlank() && control.getRound() >= 0
        && control.getClockBarrierIndex() >= epoch.getBarrierIndex()
        && control.getClockBarrierIndex() <= revision.getIndex(), "Invalid membership control barriers");
    uuid(epoch.getActivationId());
    MetadataRpc.MembershipPolicy policy = control.getPolicy();
    require(policy.getVersion() > 0 && policy.getMinTickIntervalMillis() > 0 && policy.getLeaseRounds() >= 2
        && policy.getPermitLifetimeMillis() > 0 && policy.getMaxQueryLifetimeMillis() > 0
        && policy.getMaxRevisionLag() > 0, "Invalid membership policy");
    require((policy.getPermitLifetimeMillis() - 1) / policy.getMinTickIntervalMillis() < policy.getLeaseRounds() - 1
        && control.getMinimumTickDelayMillis() >= policy.getMinTickIntervalMillis()
        && control.getMaxConfiguredPermitLifetimeMillis() >= policy.getPermitLifetimeMillis()
        && (control.getMaxConfiguredPermitLifetimeMillis() == policy.getPermitLifetimeMillis()
            || control.getMinimumTickDelayMillis() >= control.getMaxConfiguredPermitLifetimeMillis()),
        "Membership timing barrier does not protect serving permits");
    require(_lastSnapshot == null || revision.getIndex() > _lastSnapshot.getRevision().getIndex()
        || snapshot.equals(_lastSnapshot), "Server metadata revision regressed or changed at the same index");
    require(snapshot.getRecordsCount() + snapshot.getOperationIdsCount() + snapshot.getQueryBrokersCount() <= 4096,
        "Server metadata snapshot exceeds record bound");
    Map<String, CatalogRecord> tables = new HashMap<>();
    Map<String, String> tableTenants = new HashMap<>();
    Map<String, CatalogRecord> schemas = new HashMap<>();
    Map<String, CatalogRecord> configsById = new HashMap<>();
    Map<String, CatalogRecord> contents = new HashMap<>();
    Map<String, CatalogRecord> assignments = new HashMap<>();
    Map<String, String> cluster = Map.of();
    Set<String> identities = new HashSet<>();
    boolean hasCluster = false;
    for (CatalogRecord record : snapshot.getRecordsList()) {
      require(record.getSchemaVersion() == 1 && record.getVersion() > 0 && !record.getName().isBlank()
          && record.getLastModifiedIndex() > 0 && record.getLastModifiedIndex() <= revision.getIndex()
          && identities.add(uuid(record.getObjectId()).toString()), "Invalid or repeated metadata record");
      switch (record.getKind()) {
        case CLUSTER_CONFIG -> {
          require(!hasCluster && record.getName().equals("cluster"), "Invalid cluster configuration record");
          hasCluster = true;
          require(record.getDeleted() ? !record.hasValue() : record.getValue().hasClusterConfig(),
              "Missing or inconsistent cluster configuration");
          cluster = record.getDeleted() ? Map.of() : record.getValue().getClusterConfig().getPropertiesMap();
        }
        case TABLE_CONFIG -> {
          if (!record.getDeleted()) {
            TableConfig table = NativeOfflineSegmentWorker.decodeTable(record);
            require(tables.put(record.getName(), record) == null, "Repeated live table configuration");
            // Unsupported tenant configurations cannot grant query access but still permit installation hydration.
            if (table.getTenantConfig() != null) {
              String tenant = table.getTenantConfig().getBroker();
              tenant = tenant == null ? "DefaultTenant" : tenant;
              if (validRoutingIdentifier(tenant)) {
                tableTenants.put(record.getName(), tenant);
              }
            }
          } else {
            require(!record.hasValue(), "Deleted table retains a value");
          }
          configsById.put(record.getObjectId(), record);
        }
        case SCHEMA -> {
          if (!record.getDeleted()) {
            NativeOfflineSegmentWorker.decodeSchema(record);
            require(schemas.put(record.getName(), record) == null, "Repeated live schema");
          } else {
            require(!record.hasValue(), "Deleted schema retains a value");
          }
          configsById.put(record.getObjectId(), record);
        }
        case OFFLINE_SEGMENT_CONTENT -> {
          require(!record.getDeleted() && record.getValue().hasOfflineSegmentContent(), "Missing immutable content");
          MetadataRpc.OfflineSegmentContent content = record.getValue().getOfflineSegmentContent();
          NativeOfflineSegmentWorker.validateContent(content);
          require(record.getVersion() == 1 && content.getSegmentObjectId().equals(record.getObjectId()),
              "Immutable content identity or version differs");
          require(contents.put(content.getSegmentObjectId(), record) == null, "Repeated content");
        }
        case OFFLINE_REPLICA_ASSIGNMENT -> {
          require(!record.getDeleted() && record.getValue().hasOfflineReplicaAssignment(), "Missing assignment");
          MetadataRpc.OfflineReplicaAssignment assignment = record.getValue().getOfflineReplicaAssignment();
          require(assignment.getServer().equals(_server), "Another server's assignment was exposed");
          require(assignment.getAssignmentEpoch() > 0 && assignment.getAssignmentEpoch() == record.getVersion()
              && assignment.hasCompatibility() && assignment.getCompatibility().hasTable()
              && assignment.getCompatibility().hasSchema(), "Incomplete assignment");
          uuid(assignment.getMaterializationId());
          require(assignments.put(assignment.getSegmentObjectId(), record) == null, "Repeated assignment");
        }
        default -> throw new IOException("Unexpected record in server metadata projection: " + record.getKind());
      }
    }
    for (CatalogRecord record : assignments.values()) {
      MetadataRpc.OfflineReplicaAssignment assignment = record.getValue().getOfflineReplicaAssignment();
      CatalogRecord contentRecord = contents.get(assignment.getSegmentObjectId());
      require(contentRecord != null, "Assignment content is missing from snapshot");
      MetadataRpc.OfflineSegmentContent content = contentRecord.getValue().getOfflineSegmentContent();
      require(content.getTableName().equals(assignment.getTableName())
          && content.getTableObjectId().equals(assignment.getTableObjectId())
          && content.getSegmentName().equals(assignment.getSegmentName())
          && content.getContentSha256().equals(assignment.getContentSha256()), "Assignment content differs");
      MetadataRpc.MaterializationCompatibility compatibility = assignment.getCompatibility();
      require(compatibility.getTable().getObjectId().equals(assignment.getTableObjectId()),
          "Assignment table compatibility differs");
      validateReference(configsById, compatibility.getTable(), MetadataRpc.RecordKind.TABLE_CONFIG,
          assignment.getTableName());
      validateReference(configsById, compatibility.getSchema(), MetadataRpc.RecordKind.SCHEMA,
          TableNameBuilder.extractRawTableName(assignment.getTableName()));
    }
    Set<UUID> operations = new HashSet<>();
    for (String operation : snapshot.getOperationIdsList()) {
      require(operations.add(uuid(operation)), "Repeated operation discovery hint");
    }
    Map<String, QueryBroker> queryBrokers = new HashMap<>();
    Map<String, String> queryPrincipals = new HashMap<>();
    Set<String> serverTenants = new HashSet<>(tableTenants.values());
    require(snapshot.getQueryBrokersCount() <= 64, "Too many query broker authorities");
    for (MetadataRpc.QueryBrokerAuthority authority : snapshot.getQueryBrokersList()) {
      MetadataRpc.ParticipantState broker = authority.getParticipant();
      MetadataRpc.ParticipantToken token = broker.getParticipant();
      require(authority.hasParticipant() && broker.hasParticipant()
          && token.getHistoryId().equals(_server.getHistoryId())
          && token.getRole() == MetadataRpc.ParticipantRole.BROKER
          && token.getRegistrationEpoch() > 0 && validRoutingIdentifier(token.getParticipantId())
          && !token.getParticipantId().equals(_server.getParticipantId()), "Invalid query broker identity");
      uuid(token.getIncarnation());
      require(broker.getReady() && !broker.getRetired() && !broker.getDraining()
          && broker.getExpiryRound() > control.getRound() && broker.getLastRenewalSequence() >= 0
          && broker.getLastModifiedIndex() > 0 && broker.getLastModifiedIndex() <= revision.getIndex()
          && broker.hasAppliedRevision() && broker.getAppliedRevision().getHistoryId().equals(_server.getHistoryId())
          && broker.getAppliedRevision().getIndex() >= 0
          && broker.getAppliedRevision().getIndex() <= revision.getIndex()
          && broker.getCapabilitiesList().equals(List.of(MetadataRpc.ServingCapability.BROKER_QUERY)),
          "Query broker registration is not current and eligible");
      require(authority.getCertificateSha256Count() > 0 && authority.getCertificateSha256Count() <= 64
          && authority.getBrokerTenantsCount() > 0 && authority.getBrokerTenantsCount() <= 64,
          "Query broker authority is empty or oversized");
      Set<String> tenants = new HashSet<>();
      for (String tenant : authority.getBrokerTenantsList()) {
        require(validRoutingIdentifier(tenant) && tenants.add(tenant) && serverTenants.contains(tenant),
            "Query broker tenant is invalid, repeated or outside this server's tables");
      }
      require(queryBrokers.put(token.getParticipantId(), new QueryBroker(token, Set.copyOf(tenants))) == null,
          "Repeated query broker identity");
      for (String fingerprint : authority.getCertificateSha256List()) {
        require(fingerprint.matches("[0-9a-f]{64}")
            && queryPrincipals.put(fingerprint, token.getParticipantId()) == null,
            "Query certificate fingerprint is invalid or repeated");
      }
      require(queryPrincipals.size() <= 64, "Too many query certificate principals");
    }
    MembershipView membership = new MembershipView(revision, epoch, participant.getParticipant(),
        !participant.getRetired() && participant.getExpiryRound() > control.getRound(), participant.getReady(),
        participant.getDraining(), capabilities, policy.getVersion());
    Map<String, TableTenant> authorizedTables = new HashMap<>();
    tableTenants.forEach((name, tenant) -> authorizedTables.put(name,
        new TableTenant(tables.get(name).getObjectId(), tenant)));
    QueryAuthorities authorities = new QueryAuthorities(Map.copyOf(queryBrokers), Map.copyOf(queryPrincipals),
        Map.copyOf(authorizedTables));
    if (!authorities.equals(_lastAuthorities)) {
      require(_authorizationGeneration < Long.MAX_VALUE, "Query authorization generation exhausted");
      _authorizationGeneration++;
      _lastAuthorities = authorities;
    }
    Image next = new Image(revision, Map.copyOf(tables), Map.copyOf(schemas), Map.copyOf(contents),
        Map.copyOf(assignments), operations.stream().sorted().toList(), membership, Map.copyOf(tableTenants),
        authorities.brokers(), authorities.principals(), _authorizationGeneration);
    // Retain the validated high watermark even if a callback partially applies the update and then fails.
    _lastSnapshot = snapshot;
    _image = null;
    _clusterConfig.updateClusterConfigs(cluster);
    _image = next;
  }

  public List<UUID> operations() {
    return image().operations();
  }

  public MetadataRpc.Revision revision() {
    return image().revision();
  }

  /// Returns one completely applied image's membership and revision; unavailable during failed hydration or fencing.
  public MembershipView membershipView() {
    return image().membership();
  }

  public synchronized void invalidate() {
    _image = null;
  }

  /// Intersects a captured work identity with this complete local image, without decoding values or doing I/O.
  /// The participant must hold its admission read guard through publication to serialize invalidation with this check.
  /// Operation hints establish continued discovery, while the separate fresh-work guard checks attempt and owner.
  boolean isCurrent(Identity identity) {
    return isCurrent(_image, identity, true);
  }

  /// Physical observations are intersected with one complete assignment image. Completed operation hints may
  /// disappear; only the current materialization and configuration identify bytes that still belong to the table.
  public record Observation(MetadataRpc.Revision revision, Set<Identity> identities) {
    public Observation {
      identities = Set.copyOf(identities);
    }
  }

  Observation observeInstalled(List<Identity> identities) {
    Image current = image();
    Set<Identity> selected = new HashSet<>();
    for (Identity identity : identities) {
      if (isCurrent(current, identity, false)) {
        selected.add(identity);
      }
    }
    return new Observation(current.revision(), selected);
  }

  private boolean isCurrent(@Nullable Image current, Identity identity, boolean requireOperation) {
    if (current == null || !current.membership().live() || !current.membership().ready()
        || current.membership().draining() || !_server.getHistoryId().equals(identity.historyId().toString())
        || identity.server().role() != Role.SERVER
        || !_server.getParticipantId().equals(identity.server().participantId())
        || !_server.getIncarnation().equals(identity.server().incarnation().toString())
        || _server.getRegistrationEpoch() != identity.server().registrationEpoch()
        || requireOperation && !current.operations().contains(identity.operation().operationId())
        || !matches(current.tables().get(identity.tableName()), identity.table())
        || !matches(current.schemas().get(identity.schemaName()), identity.schema())) {
      return false;
    }
    String contentId = identity.content().objectId().toString();
    CatalogRecord contentRecord = current.contents().get(contentId);
    CatalogRecord assignmentRecord = current.assignments().get(contentId);
    if (!matches(contentRecord, identity.content()) || !matches(assignmentRecord, identity.assignment())) {
      return false;
    }
    MetadataRpc.OfflineSegmentContent content = contentRecord.getValue().getOfflineSegmentContent();
    MetadataRpc.OfflineReplicaAssignment assignment = assignmentRecord.getValue().getOfflineReplicaAssignment();
    MetadataRpc.MaterializationCompatibility compatibility = assignment.getCompatibility();
    return content.getTableName().equals(identity.tableName())
        && content.getTableObjectId().equals(identity.table().objectId().toString())
        && content.getSegmentName().equals(identity.segmentName()) && content.getSegmentObjectId().equals(contentId)
        && content.getContentSha256().equals(identity.contentSha256())
        && assignment.getDesiredOnline() && assignment.getServer().equals(_server)
        && assignment.getTableName().equals(identity.tableName())
        && assignment.getTableObjectId().equals(identity.table().objectId().toString())
        && assignment.getSegmentName().equals(identity.segmentName())
        && assignment.getSegmentObjectId().equals(contentId)
        && assignment.getContentSha256().equals(identity.contentSha256())
        && assignment.getAssignmentEpoch() == identity.assignment().version()
        && assignment.getMaterializationId().equals(identity.materializationId().toString())
        && matches(compatibility.getTable(), identity.table(), identity.historyId())
        && matches(compatibility.getSchema(), identity.schema(), identity.historyId());
  }

  /// Checks all requested read identities against one coherent local image, without I/O or operation discovery.
  /// Completed installations remain eligible. A caller must independently enforce its serving ticket and pin exact
  /// installed segment identities before execution; this predicate does not grant serving authority.
  boolean isCurrentQuery(NativeQueryContext query) {
    Image current = _image;
    if (current == null || !current.membership().live() || !current.membership().ready()
        || current.membership().draining()
        || !current.membership().capabilities().contains(ReadOnlyCapability.SERVER_QUERY)
        || !_server.getHistoryId().equals(query.historyId().toString()) || query.server().role() != Role.SERVER
        || !_server.getParticipantId().equals(query.server().participantId())
        || !_server.getIncarnation().equals(query.server().incarnation().toString())
        || _server.getRegistrationEpoch() != query.server().registrationEpoch()
        || !matches(current.tables().get(query.tableName()), query.table())
        || !matches(current.schemas().get(query.schemaName()), query.schema())) {
      return false;
    }
    for (List<Segment> group : List.of(query.requiredSegments(), query.optionalSegments())) {
      for (Segment segment : group) {
        String contentId = segment.content().objectId().toString();
        CatalogRecord contentRecord = current.contents().get(contentId);
        CatalogRecord assignmentRecord = current.assignments().get(contentId);
        if (!matches(contentRecord, segment.content()) || !matches(assignmentRecord, segment.assignment())) {
          return false;
        }
        MetadataRpc.OfflineSegmentContent content = contentRecord.getValue().getOfflineSegmentContent();
        MetadataRpc.OfflineReplicaAssignment assignment = assignmentRecord.getValue().getOfflineReplicaAssignment();
        MetadataRpc.MaterializationCompatibility compatibility = assignment.getCompatibility();
        if (!content.getTableName().equals(query.tableName())
            || !content.getTableObjectId().equals(query.table().objectId().toString())
            || !content.getSegmentName().equals(segment.name()) || !content.getSegmentObjectId().equals(contentId)
            || !content.getContentSha256().equals(segment.contentSha256())
            || !assignment.getDesiredOnline() || !assignment.getServer().equals(_server)
            || !assignment.getTableName().equals(query.tableName())
            || !assignment.getTableObjectId().equals(query.table().objectId().toString())
            || !assignment.getSegmentName().equals(segment.name()) || !assignment.getSegmentObjectId().equals(contentId)
            || !assignment.getContentSha256().equals(segment.contentSha256())
            || assignment.getAssignmentEpoch() != segment.assignment().version()
            || !assignment.getMaterializationId().equals(segment.materializationId().toString())
            || !matches(compatibility.getTable(), query.table(), query.historyId())
            || !matches(compatibility.getSchema(), query.schema(), query.historyId())) {
          return false;
        }
      }
    }
    return true;
  }

  /// Authenticates the claimed broker against one committed image and the current table incarnation's tenant.
  /// Delivery intentionally does not recheck desired segment identities: an admitted attempt retains pinned readers.
  boolean isAuthorizedQuery(NativeQueryContext query, @Nullable String certificateSha256) {
    Image current = _image;
    if (current == null || certificateSha256 == null
        || !_server.getHistoryId().equals(query.historyId().toString())
        || !query.broker().participantId().equals(current.queryPrincipals().get(certificateSha256))) {
      return false;
    }
    QueryBroker broker = current.queryBrokers().get(query.broker().participantId());
    CatalogRecord table = current.tables().get(query.tableName());
    String tenant = current.tableTenants().get(query.tableName());
    return broker != null && table != null && !table.getDeleted()
        && tenant != null
        && table.getObjectId().equals(query.table().objectId().toString())
        && broker.participant().getIncarnation().equals(query.broker().incarnation().toString())
        && broker.participant().getRegistrationEpoch() == query.broker().registrationEpoch()
        && broker.tenants().contains(tenant);
  }

  /// Semantic authorization changes conservatively fence all admitted attempts. Renewal counters and compatible
  /// table version changes do not advance this generation; a removed then re-added grant cannot revive an old ticket.
  long authorizationGeneration() {
    return image().authorizationGeneration();
  }

  private static boolean validRoutingIdentifier(String value) {
    return value != null && !value.isBlank() && value.length() <= 1024 && !value.contains("*")
        && value.chars().noneMatch(Character::isISOControl) && StandardCharsets.UTF_8.newEncoder().canEncode(value);
  }

  private static boolean matches(@Nullable CatalogRecord record, ObjectVersion expected) {
    return record != null && !record.getDeleted() && record.getObjectId().equals(expected.objectId().toString())
        && record.getVersion() == expected.version();
  }

  private static boolean matches(MetadataRpc.ObjectVersion actual, ObjectVersion expected, UUID history) {
    return actual.getHistoryId().equals(history.toString())
        && actual.getObjectId().equals(expected.objectId().toString()) && actual.getVersion() == expected.version();
  }

  public TableDataManagerMetadata provider(String tableName) {
    CatalogRecord captured = image().tables().get(tableName);
    checkState(captured != null && !captured.getDeleted(), "Assigned table metadata is absent: %s", tableName);
    String objectId = captured.getObjectId();
    return new TableDataManagerMetadata() {
      @Override
      public TableSnapshot fetchTable(String requested) {
        checkState(tableName.equals(requested), "Table provider identity differs");
        return table(image(), tableName, objectId);
      }

      @Nullable
      @Override
      public SegmentSnapshot fetchSegment(String requested, String segmentName) {
        checkState(tableName.equals(requested), "Table provider identity differs");
        Image current = image();
        TableSnapshot table = table(current, tableName, objectId);
        for (CatalogRecord record : current.assignments().values()) {
          MetadataRpc.OfflineReplicaAssignment assignment = record.getValue().getOfflineReplicaAssignment();
          if (!assignment.getTableName().equals(tableName) || !assignment.getSegmentName().equals(segmentName)
              || !assignment.getTableObjectId().equals(objectId) || !assignment.getDesiredOnline()) {
            continue;
          }
          checkVersion(current.tables().get(tableName), assignment.getCompatibility().getTable());
          checkVersion(current.schemas().get(table.schema().getSchemaName()),
              assignment.getCompatibility().getSchema());
          MetadataRpc.OfflineSegmentContent content = current.contents().get(assignment.getSegmentObjectId())
              .getValue().getOfflineSegmentContent();
          MetadataRpc.SegmentLoaderDescriptor loader = content.getLoader();
          SegmentLoadMetadata segment = new SegmentLoadMetadata(segmentName, loader.getCrc(), loader.getDataCrc(),
              loader.getUseDataCrc(), content.getDownloadUri(), loader.hasTier() ? loader.getTier() : null,
              loader.hasCrypterName() ? loader.getCrypterName() : null, loader.getCreationTimeMs(),
              loader.getPushTimeMs(), loader.getCustomConfigsMap(), true);
          return new SegmentSnapshot(table.tableConfig(), table.schema(), segment);
        }
        return null;
      }
    };
  }

  private TableDataManagerMetadata.TableSnapshot table(Image image, String tableName, String objectId) {
    CatalogRecord table = image.tables().get(tableName);
    CatalogRecord schema = image.schemas().get(TableNameBuilder.extractRawTableName(tableName));
    checkState(table != null && !table.getDeleted() && table.getObjectId().equals(objectId)
        && schema != null && !schema.getDeleted(), "Table or schema was deleted or replaced");
    try {
      return new TableDataManagerMetadata.TableSnapshot(NativeOfflineSegmentWorker.decodeTable(table),
          NativeOfflineSegmentWorker.decodeSchema(schema));
    } catch (IOException e) {
      throw new IllegalStateException("Invalid server metadata", e);
    }
  }

  private void validateReference(Map<String, CatalogRecord> configs, MetadataRpc.ObjectVersion reference,
      MetadataRpc.RecordKind kind, String name) throws IOException {
    CatalogRecord record = configs.get(reference.getObjectId());
    require(reference.getHistoryId().equals(_server.getHistoryId()) && reference.getVersion() > 0
        && record != null && record.getKind() == kind && record.getName().equals(name)
        && reference.getVersion() <= record.getVersion(), "Assignment configuration reference is absent or invalid");
  }

  private void checkVersion(CatalogRecord record, MetadataRpc.ObjectVersion expected) {
    checkState(record != null && !record.getDeleted() && expected.getHistoryId().equals(_server.getHistoryId())
        && expected.getObjectId().equals(record.getObjectId()) && expected.getVersion() == record.getVersion(),
        "Assignment compatibility changed; controller reconciliation is required");
  }

  private Image image() {
    Image image = _image;
    checkState(image != null, "Server metadata is not hydrated");
    return image;
  }

  private static UUID uuid(String value) throws IOException {
    try {
      UUID parsed = UUID.fromString(value);
      require(parsed.toString().equals(value), "Noncanonical metadata UUID");
      return parsed;
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid metadata UUID", e);
    }
  }

  private static void require(boolean condition, String message) throws IOException {
    if (!condition) {
      throw new IOException(message);
    }
  }
}
