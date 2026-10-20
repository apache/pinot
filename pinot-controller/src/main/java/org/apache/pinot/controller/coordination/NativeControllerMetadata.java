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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.RecoverableClusterConfigCache;
import org.apache.pinot.common.coordination.NativeMetadataConfig;
import org.apache.pinot.common.evaluator.FunctionEvaluatorFactory;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Acknowledge;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogImage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ConfigTransaction;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedScope;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetStreamTip;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Mutation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RegisterClient;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Result;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.StreamTipResult;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol;
import org.apache.pinot.coordination.ratis.transport.MetadataAccess;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.config.provider.PinotClusterConfigProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableConfigValidatorRegistry;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;


/// Bounded, quorum-backed controller reads. Each REST read captures a new coherent image; the separately refreshed
/// cluster config provider exists only for local listeners such as audit configuration, never as a REST fallback.
/// Conditional writes serialize one durable controller stream and validate one coherent image. Native configurations
/// are literal: environment/system-property templates are unsupported. Lifecycle and provider refreshes are
/// serialized. The owner closes this facade before closing its MetadataAccess.
public final class NativeControllerMetadata implements PinotClusterConfigProvider, AutoCloseable {
  private static final FeedScope CONFIG_SCOPE =
      new FeedScope(Set.of(RecordKind.CLUSTER_CONFIG), "controller-http-config");
  private static final FeedScope BROWSE_SCOPE = new FeedScope(EnumSet.of(RecordKind.CLUSTER_CONFIG, RecordKind.SCHEMA,
      RecordKind.TABLE_CONFIG, RecordKind.MEMBERSHIP_CONTROL, RecordKind.PARTICIPANT,
      RecordKind.OFFLINE_SEGMENT_CONTENT, RecordKind.OFFLINE_REPLICA_ASSIGNMENT, RecordKind.CONTROLLER_OPERATION),
      "controller-http-browse");
  private final MetadataAccess _access;
  private final UUID _history;
  private final String _clusterName;
  private final int _maxRecords;
  private final int _maxBytes;
  private final long _timeoutMillis;
  private final long _refreshMillis;
  private final RecoverableClusterConfigCache _clusterConfig = new RecoverableClusterConfigCache();
  private final Object _writeLock = new Object();
  private final String _clientId;
  private final UUID _clientIncarnation;
  private final boolean _disableGroovy;
  private final boolean _enforcePoolBasedAssignment;
  private final int _minReplicas;
  @Nullable
  private ScheduledExecutorService _refresher;
  private volatile boolean _started;
  private volatile boolean _closed;
  @Nullable
  private volatile String _clusterConfigFailure;
  private long _configRevision = -1;

  public NativeControllerMetadata(MetadataAccess access, UUID history, ControllerConf conf) {
    _access = requireNonNull(access, "access");
    _history = requireNonNull(history, "history");
    _clientId = "controller-http/" + NativeMetadataConfig.required(conf, "id");
    _clientIncarnation = UUID.nameUUIDFromBytes((_history + "/" + _clientId).getBytes(StandardCharsets.UTF_8));
    _disableGroovy = conf.isDisableIngestionGroovy();
    _enforcePoolBasedAssignment = conf.isEnforcePoolBasedAssignmentEnabled();
    _minReplicas = conf.getDefaultTableMinReplicas();
    String name = conf.getHelixClusterName();
    _clusterName = name == null || name.isBlank() ? NativeMetadataConfig.required(conf, "clusterId") : name;
    String prefix = NativeMetadataConfig.PREFIX + "controller.http.";
    _maxRecords = conf.getProperty(prefix + "maxRecords", 4096);
    _maxBytes = conf.getProperty(prefix + "maxBytes", 2 * MetadataProtocol.MAX_COMMAND_BYTES);
    _timeoutMillis = conf.getProperty(prefix + "timeoutMs", 5000L);
    _refreshMillis = conf.getProperty(prefix + "configRefreshMs", 1000L);
    checkArgument(_maxRecords > 0 && _maxRecords <= MetadataProtocol.MAX_IMAGE_RECORDS,
        "Native HTTP record limit is outside supported bounds");
    checkArgument(_maxBytes >= 1024 && _maxBytes <= MetadataProtocol.MAX_IMAGE_BYTES,
        "Native HTTP byte limit is outside supported bounds");
    checkArgument(_timeoutMillis > 0 && _timeoutMillis <= 60_000 && _refreshMillis > 0 && _refreshMillis <= 60_000,
        "Native HTTP read and config refresh intervals must be between 1 and 60000 milliseconds");
  }

  public synchronized void start() throws IOException {
    checkState(!_started && !_closed, "Native controller metadata has already started or closed");
    refreshClusterConfig();
    _refresher = Executors.newSingleThreadScheduledExecutor(runnable -> {
      Thread thread = new Thread(runnable, "native-controller-cluster-config");
      thread.setDaemon(true);
      return thread;
    });
    _started = true;
    _refresher.scheduleWithFixedDelay(this::refreshSafely, _refreshMillis, _refreshMillis, TimeUnit.MILLISECONDS);
  }

  private void refreshSafely() {
    try {
      refreshClusterConfig();
    } catch (Exception e) {
      _clusterConfigFailure = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    }
  }

  /// Serialized complete refreshes preserve changed/unset listener retries after a partial callback failure.
  synchronized void refreshClusterConfig() throws IOException {
    checkState(!_closed, "Native controller metadata is closed");
    CatalogImage image = image(CONFIG_SCOPE);
    if (image.cursor().index() < _configRevision) {
      throw new IOException("Native cluster config image regressed");
    }
    Map<String, String> values = clusterConfig(image.records());
    try {
      if (_configRevision < 0 || _clusterConfigFailure != null || !values.equals(_clusterConfig.getClusterConfigs())) {
        _clusterConfig.updateClusterConfigs(values);
      }
      _configRevision = image.cursor().index();
      _clusterConfigFailure = null;
    } catch (RuntimeException e) {
      _clusterConfigFailure = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
      throw e;
    }
  }

  @Nullable
  public String clusterConfigFailure() {
    return _clusterConfigFailure;
  }

  public String clusterName() {
    return _clusterName;
  }

  public Snapshot snapshot() throws IOException {
    if (!_started || _closed) {
      throw new IOException("Native controller metadata is not running");
    }
    CatalogImage image = image(BROWSE_SCOPE);
    Map<String, CatalogRecord> schemas = new HashMap<>();
    Map<String, CatalogRecord> tables = new HashMap<>();
    Map<String, ParticipantState> participants = new HashMap<>();
    ControlState control = null;
    Set<String> nativeTables = new HashSet<>();
    Map<String, CatalogRecord> contents = new HashMap<>();
    Map<String, CatalogRecord> assignments = new HashMap<>();
    CatalogRecord clusterRecord = null;
    try {
      for (CatalogRecord record : image.records()) {
        switch (record.kind()) {
          case SCHEMA -> schemas.put(record.name(), record);
          case TABLE_CONFIG -> tables.put(record.name(), record);
          case MEMBERSHIP_CONTROL -> {
            if (!MembershipProtocol.CONTROL_NAME.equals(record.name()) || control != null) {
              throw new IOException("Invalid native membership control record");
            }
            control = MembershipProtocol.decodeControl(record.value());
          }
          case PARTICIPANT -> {
            ParticipantState participant = MembershipProtocol.decodeParticipant(record.value());
            if (participant.participant() == null || !_history.equals(participant.participant().historyId())
                || !record.name().equals(participant.participant().participantId())) {
              throw new IOException("Invalid native participant record");
            }
            participants.put(record.name(), participant);
          }
          case CLUSTER_CONFIG -> clusterRecord = record;
          case OFFLINE_SEGMENT_CONTENT -> {
            nativeTables.add(OfflineSegmentProtocol.decodeContent(record.value()).tableName());
            contents.put(record.name(), record);
          }
          case OFFLINE_REPLICA_ASSIGNMENT -> {
            nativeTables.add(OfflineSegmentProtocol.decodeAssignment(record.value()).tableName());
            assignments.put(record.name(), record);
          }
          case CONTROLLER_OPERATION -> nativeTables.add(
              OperationProtocol.decodeOperation(record.value()).input().tableName());
          default -> throw new IOException("Unexpected native HTTP record family");
        }
      }
      if (control == null || control.controlEpoch() == null || control.policy() == null
          || !_history.equals(control.controlEpoch().historyId())) {
        throw new IOException("Native membership control is not initialized");
      }
      return new Snapshot(new Revision(_history, image.cursor().index()), clusterConfig(image.records()), schemas,
          tables, control, participants, clusterRecord, nativeTables, contents, assignments);
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new IOException("Malformed native controller metadata image", e);
    }
  }

  /// Resolves the bounded durable controller stream before issuing one conditional transaction. An IOException from
  /// execute remains an unknown outcome: this method never changes its precondition or automatically resubmits it.
  public Result commit(Snapshot snapshot, Mutation mutation) throws IOException {
    synchronized (_writeLock) {
      if (!_started || _closed) {
        throw new IOException("Native controller metadata is not running");
      }
      Result registration = _access.execute(new RegisterClient(_history, _clientId, _clientIncarnation, 0),
          _timeoutMillis);
      if (registration.status() != Status.OK || registration.clientEpoch() == null || registration.clientEpoch() != 1) {
        throw new IOException("Native HTTP writer registration is unavailable or fenced");
      }
      StreamTipResult tip = _access.query(new GetStreamTip(_history, _clientId, 1, "config"), _timeoutMillis);
      if (tip.status() != Status.OK || tip.clientEpoch() != 1 || tip.sequence() < 0 || tip.floor() < 0
          || tip.floor() > tip.sequence() || tip.sequence() == Long.MAX_VALUE) {
        throw new IOException("Native HTTP writer stream is unavailable or exhausted");
      }
      if (tip.sequence() > tip.floor()) {
        if (tip.outcome() == null) {
          throw new IOException("Native HTTP writer has an unresolved outcome");
        }
        Result acknowledged = _access.execute(new Acknowledge(_history, _clientId, 1, "config", tip.sequence()),
            _timeoutMillis);
        if (acknowledged.status() != Status.OK) {
          throw new IOException("Native HTTP writer could not acknowledge its resolved outcome");
        }
      }
      return _access.execute(new ConfigTransaction(_history, _clientId, 1, "config", tip.sequence() + 1,
          snapshot.revision(), List.of(mutation)), _timeoutMillis);
    }
  }

  /// Validates only metadata changes whose native segment lifecycle has not started. Existing assignments retain
  /// table/schema versions, so changing those versions requires reconciliation that this HTTP surface does not run.
  public void validateSchema(Snapshot snapshot, Schema schema, @Nullable Schema previous) throws IOException {
    requireLiteralConfig(schema);
    String name = schema.getSchemaName();
    checkArgument(name != null && !name.isBlank() && TableNameBuilder.getTableTypeFromTableName(name) == null
            && !name.contains(TableConfig.TABLE_NAME_FORBIDDEN_SUBSTRING),
        "Schema requires a valid name");
    List<TableConfig> tables = new ArrayList<>();
    for (TableType type : TableType.values()) {
      String typed = TableNameBuilder.forType(type).tableNameWithType(name);
      requireNoNativeState(snapshot, typed);
      CatalogRecord table = snapshot.tables().get(typed);
      if (table != null) {
        if (type != TableType.OFFLINE) {
          throw new UnsupportedOperationException("Schema changes for realtime tables are not supported");
        }
        tables.add(JsonUtils.jsonNodeToObject(table.value(), TableConfig.class));
      }
    }
    validateSchemaTransforms(schema);
    validateNameCollision(snapshot.schemas(), name, ignoreCase(snapshot.clusterConfig()));
    // The SchemaUtils table-compatibility overload reads legacy process-global validation switches. Validate the
    // schema separately, then each affected table using the explicit native component policy instead.
    SchemaUtils.validate(schema, List.of(), ignoreCase(snapshot.clusterConfig()), previous);
    for (TableConfig table : tables) {
      TableConfigUtils.validate(table, schema, null, table, _disableGroovy, _enforcePoolBasedAssignment);
    }
    checkArgument(previous == null || schema.isBackwardCompatibleWith(previous),
        "Schema update is not backward compatible");
  }

  public void validateTable(Snapshot snapshot, TableConfig table, @Nullable TableConfig previous) throws IOException {
    requireLiteralConfig(table);
    String name = table.getTableName();
    checkArgument(name != null && !name.isBlank()
            && TableNameBuilder.getTableTypeFromTableName(TableNameBuilder.extractRawTableName(name)) == null,
        "Table requires an unambiguous raw name");
    checkArgument(table.getValidationConfig() != null, "Table validation configuration is required");
    requireNoNativeState(snapshot, name);
    if (table.getTableType() != TableType.OFFLINE || table.isDimTable() || table.isUpsertEnabled()
        || table.getDedupConfig() != null || table.getTaskConfig() != null
        || table.getTierConfigsList() != null && !table.getTierConfigsList().isEmpty()
        || table.getTunerConfigsList() != null && !table.getTunerConfigsList().isEmpty()
        || table.getInstanceAssignmentConfigMap() != null && !table.getInstanceAssignmentConfigMap().isEmpty()
        || table.getInstancePartitionsMap() != null && !table.getInstancePartitionsMap().isEmpty()
        || table.getSegmentAssignmentConfigMap() != null && !table.getSegmentAssignmentConfigMap().isEmpty()
        || table.getMaterializedViewTaskConfigs() != null && !table.getMaterializedViewTaskConfigs().isEmpty()
        || table.getValidationConfig().getPeerSegmentDownloadScheme() != null) {
      throw new UnsupportedOperationException("Native HTTP supports ordinary OFFLINE table configurations without "
          + "tasks, tiers, upsert, dedup, tuning, peer download or Helix assignment options");
    }
    String raw = TableNameBuilder.extractRawTableName(name);
    if (snapshot.tables().containsKey(TableNameBuilder.REALTIME.tableNameWithType(raw))) {
      throw new UnsupportedOperationException("Native HTTP does not manage hybrid tables");
    }
    CatalogRecord schemaRecord = snapshot.schemas().get(raw);
    checkArgument(schemaRecord != null, "Schema not found for table: %s", name);
    Schema schema = JsonUtils.jsonNodeToObject(schemaRecord.value(), Schema.class);
    validateSchema(snapshot, schema, schema);
    validateNameCollision(snapshot.tables(), name, ignoreCase(snapshot.clusterConfig()));
    TableConfigUtils.validateTableName(table);
    TableConfigUtils.ensureMinReplicas(table, _minReplicas);
    TableConfigUtils.validate(table, schema, null, previous, _disableGroovy, _enforcePoolBasedAssignment);
    TableConfigValidatorRegistry.validate(table, schema);
    checkArgument(previous == null || TableConfigUtils.validateBackwardCompatibility(table, previous).isEmpty(),
        "Table configuration update is not backward compatible");
  }

  public void validateSchemaDeletion(Snapshot snapshot, String name) {
    for (TableType type : TableType.values()) {
      String typed = TableNameBuilder.forType(type).tableNameWithType(name);
      requireNoNativeState(snapshot, typed);
      checkState(!snapshot.tables().containsKey(typed), "Schema is still referenced by table: %s", typed);
    }
  }

  public void validateClusterConfig(Snapshot snapshot, Map<String, String> values) throws IOException {
    requireLiteralConfig(values);
    if (!snapshot.nativeTables().isEmpty()) {
      Set<String> changed = new HashSet<>(snapshot.clusterConfig().keySet());
      changed.addAll(values.keySet());
      changed.removeIf(key -> Objects.equals(snapshot.clusterConfig().get(key), values.get(key)));
      // Endpoint approvals govern read-only controller fan-out and require no segment reconciliation. Their
      // removal/change is rechecked before returning an observation; all other changes retain the lifecycle gate.
      if (changed.isEmpty() || changed.stream().anyMatch(
          key -> !key.startsWith(NativeTableSizeMetadataProvider.ENDPOINT_APPROVAL_PREFIX))) {
        throw new UnsupportedOperationException("Cluster config changes require native table reconciliation");
      }
    }
    String ignoreCase = values.get(CommonConstants.Helix.ENABLE_CASE_INSENSITIVE_KEY);
    checkArgument(ignoreCase == null || "true".equalsIgnoreCase(ignoreCase) || "false".equalsIgnoreCase(ignoreCase),
        "Case-insensitive configuration must be true or false");
    if (ignoreCase(values)) {
      for (CatalogRecord record : snapshot.schemas().values()) {
        validateNameCollision(snapshot.schemas(), record.name(), true);
        Schema schema = JsonUtils.jsonNodeToObject(record.value(), Schema.class);
        validateSchemaTransforms(schema);
        SchemaUtils.validate(schema, List.of(), true, schema);
      }
      for (CatalogRecord record : snapshot.tables().values()) {
        validateNameCollision(snapshot.tables(), record.name(), true);
      }
    }
  }

  private static void requireLiteralConfig(Object value) throws IOException {
    // Unlike the Helix provider, native participants do not resolve host-local templates on config reads.
    // Even a template with a default can differ across hosts and cannot be validated as committed configuration.
    if (JsonUtils.objectToString(value).contains("${")) {
      throw new UnsupportedOperationException("Native metadata requires literal configuration; environment and "
          + "system-property templates are not supported");
    }
  }

  // Inspect legacy schema transforms before SchemaUtils can compile them, even though the field is deprecated.
  @SuppressWarnings("deprecation")
  private void validateSchemaTransforms(Schema schema) {
    if (_disableGroovy) {
      schema.getAllFieldSpecs().forEach(field -> {
        String expression = field.getTransformFunction();
        checkArgument(expression == null || !FunctionEvaluatorFactory.isGroovyExpression(expression),
            "Groovy schema transforms are disabled");
      });
    }
  }

  private static boolean ignoreCase(Map<String, String> values) {
    return Boolean.parseBoolean(values.getOrDefault(CommonConstants.Helix.ENABLE_CASE_INSENSITIVE_KEY,
        Boolean.toString(CommonConstants.Helix.DEFAULT_ENABLE_CASE_INSENSITIVE)));
  }

  private static void validateNameCollision(Map<String, CatalogRecord> records, String name, boolean ignoreCase) {
    if (ignoreCase) {
      checkArgument(records.keySet().stream().noneMatch(existing -> !existing.equals(name)
          && existing.equalsIgnoreCase(name)), "Case-insensitive metadata name already exists: %s", name);
    }
  }

  private static void requireNoNativeState(Snapshot snapshot, String name) {
    if (snapshot.nativeTables().contains(name)) {
      throw new UnsupportedOperationException(
          "Table has native segment or operation state; reconciliation is required");
    }
  }

  private CatalogImage image(FeedScope scope) throws IOException {
    CatalogImage image = _access.image(scope, _maxRecords, _maxBytes, _timeoutMillis);
    if (image == null || image.cursor() == null || !_history.equals(image.cursor().historyId())
        || image.cursor().index() < 0 || !scope.scopeId().equals(image.cursor().scopeId())
        || image.records() == null || image.records().size() > _maxRecords) {
      throw new IOException("Invalid native controller metadata image");
    }
    Set<UUID> ids = new HashSet<>();
    Set<String> names = new HashSet<>();
    for (CatalogRecord record : image.records()) {
      if (record == null || record.kind() == null || !scope.kinds().contains(record.kind())
          || record.name() == null || record.name().isBlank() || record.objectId() == null || record.version() <= 0
          || record.schemaVersion() != MetadataProtocol.FORMAT_VERSION || record.deleted() || record.value() == null
          || !record.value().isObject() || record.lastModifiedIndex() <= 0
          || record.lastModifiedIndex() > image.cursor().index() || !ids.add(record.objectId())
          || !names.add(record.kind() + "/" + record.name())) {
        throw new IOException("Invalid or duplicate native controller metadata record");
      }
    }
    if (JsonUtils.objectToBytes(image).length > _maxBytes) {
      throw new IOException("Native controller metadata image exceeds its reserved byte limit");
    }
    return image;
  }

  private static Map<String, String> clusterConfig(List<CatalogRecord> records) throws IOException {
    Map<String, String> values = new HashMap<>();
    boolean found = false;
    for (CatalogRecord record : records) {
      if (record.kind() == RecordKind.CLUSTER_CONFIG) {
        if (found || !MetadataProtocol.CLUSTER_CONFIG_NAME.equals(record.name())) {
          throw new IOException("Invalid native cluster config record");
        }
        found = true;
        for (var entry : record.value().properties()) {
          if (!entry.getValue().isTextual()) {
            throw new IOException("Native cluster config values must be strings");
          }
          values.put(entry.getKey(), entry.getValue().textValue());
        }
      }
    }
    return Map.copyOf(values);
  }

  @Override
  public Map<String, String> getClusterConfigs() {
    checkState(_started && !_closed, "Native cluster config provider is not running");
    return _clusterConfig.getClusterConfigs();
  }

  @Override
  public boolean registerClusterConfigChangeListener(PinotClusterConfigChangeListener listener) {
    checkState(_started && !_closed, "Native cluster config provider is not running");
    try {
      return _clusterConfig.registerClusterConfigChangeListener(listener);
    } catch (RuntimeException e) {
      _clusterConfigFailure = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
      throw e;
    }
  }

  @Override
  public void close() throws InterruptedException {
    ScheduledExecutorService refresher;
    synchronized (this) {
      if (_closed) {
        return;
      }
      _closed = true;
      refresher = _refresher;
      if (refresher != null) {
        refresher.shutdown();
      }
    }
    if (refresher != null && !refresher.awaitTermination(_timeoutMillis + 1000, TimeUnit.MILLISECONDS)) {
      throw new IllegalStateException("Native cluster config refresher did not stop within its read deadline");
    }
  }

  /// All fields come from one image. Catalog records and membership records are detached immutable protocol values.
  public record Snapshot(Revision revision, Map<String, String> clusterConfig, Map<String, CatalogRecord> schemas,
                         Map<String, CatalogRecord> tables, ControlState control,
                         Map<String, ParticipantState> participants, @Nullable CatalogRecord clusterRecord,
                         Set<String> nativeTables, Map<String, CatalogRecord> contents,
                         Map<String, CatalogRecord> assignments) {
    public Snapshot {
      clusterConfig = Map.copyOf(clusterConfig);
      schemas = Map.copyOf(schemas);
      tables = Map.copyOf(tables);
      participants = Map.copyOf(participants);
      nativeTables = Set.copyOf(nativeTables);
      contents = Map.copyOf(contents);
      assignments = Map.copyOf(assignments);
    }
  }
}
