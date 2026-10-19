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
package org.apache.pinot.common.coordination;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Message;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Identity;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Installation;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Mode;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ObjectVersion;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Operation;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Owner;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Participant;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ReadGuard;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Request;
import org.apache.pinot.common.config.provider.TableDataManagerMetadata.SegmentSnapshot;
import org.apache.pinot.common.metadata.segment.SegmentLoadMetadata;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Converts authenticated own-target wire captures to native installation actions without a consensus dependency.
/// Local installation is not serving authority or controller success. The publisher must durably retain the exact
/// report and request sequence across unknown outcomes; this adapter never acknowledges or advances that stream.
public final class NativeOfflineSegmentWorker {
  private static final ObjectMapper JSON = JsonMapper.builder(JsonFactory.builder()
          .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
          .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(100)
              .maxStringLength(MetadataRpcClient.MAX_COMMAND_BYTES).build()).build())
      .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).build();

  private final MetadataRpc.ParticipantToken _server;
  private final UUID _history;
  private final WorkSource _source;
  private final ReceiptPublisher _publisher;

  @FunctionalInterface
  public interface WorkSource {
    /// Return a fresh committed capture authorized for this worker's SERVER session.
    MetadataRpc.InstallationWorkResult lookup(UUID operationId) throws Exception;
  }

  @FunctionalInterface
  public interface ReceiptPublisher {
    /// Publish through the durable authenticated SERVER outbox, preserving identity on unknown outcomes.
    MetadataRpc.CommandResult publish(MetadataRpc.InstallationReport report) throws Exception;
  }

  /// A physical outcome and, only for an installation, its conditional consensus receipt outcome.
  public static final class Execution {
    private final NativeOfflineSegmentAction.Result _local;
    @Nullable
    private final MetadataRpc.CommandResult _receipt;

    private Execution(NativeOfflineSegmentAction.Result local, @Nullable MetadataRpc.CommandResult receipt) {
      _local = local;
      _receipt = receipt;
    }

    public NativeOfflineSegmentAction.Result local() {
      return _local;
    }

    @Nullable
    public MetadataRpc.CommandResult receipt() {
      return _receipt;
    }
  }

  public NativeOfflineSegmentWorker(MetadataRpc.ParticipantToken server, WorkSource source, ReceiptPublisher publisher)
      throws IOException {
    validateMessage(server);
    _history = uuid(server.getHistoryId());
    participant(server, _history, MetadataRpc.ParticipantRole.SERVER);
    _server = server;
    _source = Objects.requireNonNull(source, "source");
    _publisher = Objects.requireNonNull(publisher, "publisher");
  }

  public Execution execute(UUID operationId, NativeOfflineSegmentAction.Executor installer)
      throws Exception {
    Objects.requireNonNull(installer, "installer");
    return execute(operationId, table -> installer);
  }

  /// Resolve a table installer only after validating the complete authorized capture, including loading values.
  /// The factory may create a table manager and its directories, so malformed or foreign work must not reach it.
  public Execution execute(UUID operationId, Function<String, NativeOfflineSegmentAction.Executor> executorFactory)
      throws Exception {
    Objects.requireNonNull(executorFactory, "executorFactory");
    MetadataRpc.InstallationWork work = lookup(operationId);
    if (work == null) {
      return new Execution(new NativeOfflineSegmentAction.Result(NativeOfflineSegmentAction.Status.FENCED, null), null);
    }
    Identity identity = identity(work);
    SegmentSnapshot snapshot = snapshot(work);
    NativeOfflineSegmentAction.Executor installer = Objects.requireNonNull(executorFactory.apply(identity.tableName()),
        "installer");
    Installation previous = installer.installed(identity.segmentName());
    Mode mode = work.getReconcile() ? Mode.RECONCILE : work.getInitialLoad() ? Mode.LOAD : Mode.RELOAD;
    Request request = new Request(mode, identity, snapshot, work.getForceDownload(),
        previous == null ? null : previous.installationId());
    NativeOfflineSegmentAction.Result local = installer.execute(request, new ExecutionAuthority(operationId, work));
    require(local != null, "Installer returned no result");
    if (local.status() != NativeOfflineSegmentAction.Status.INSTALLED) {
      return new Execution(local, null);
    }
    Installation installed = local.installation();
    require(installed != null && identity.equals(installed.identity()), "Installer returned another identity");
    MetadataRpc.InstallationReport report = MetadataRpc.InstallationReport.newBuilder()
        .setOperationId(work.getOperationId()).setAttempt(work.getAttempt()).setOwner(work.getOwner())
        .setInstalled(work.getTarget().getTarget()).setInstallationId(installed.installationId().toString())
        .setManifestSha256(installed.manifestSha256()).build();
    MetadataRpc.CommandResult receipt = _publisher.publish(report);
    validateMessage(receipt);
    require(receipt.hasRevision() && receipt.getRevision().getIndex() >= 0
        && uuid(receipt.getRevision().getHistoryId()).equals(_history)
        && receipt.getStatus() != MetadataRpc.ResultStatus.RESULT_STATUS_UNSPECIFIED
        && !receipt.hasClientEpoch() && !receipt.hasRecord() && receipt.getVersionsCount() <= 128
        && (receipt.getStatus() == MetadataRpc.ResultStatus.OK || receipt.getVersionsCount() == 0),
        "Invalid installation receipt result");
    Set<UUID> objects = new HashSet<>();
    for (MetadataRpc.ObjectVersion version : receipt.getVersionsList()) {
      require(objects.add(version(version, _history).objectId()), "Repeated installation receipt object");
    }
    return new Execution(local, receipt);
  }

  @Nullable
  private MetadataRpc.InstallationWork lookup(UUID operationId)
      throws Exception {
    Objects.requireNonNull(operationId, "operationId");
    MetadataRpc.InstallationWorkResult response = _source.lookup(operationId);
    validateMessage(response);
    require(response.hasRevision() && response.getRevision().getIndex() >= 0
        && response.getStatus() != MetadataRpc.ResultStatus.RESULT_STATUS_UNSPECIFIED
        && response.hasWork() == (response.getStatus() == MetadataRpc.ResultStatus.OK),
        "Invalid installation work envelope");
    UUID history = uuid(response.getRevision().getHistoryId());
    switch (response.getStatus()) {
      case FENCED:
      case NOT_FOUND:
      case CONFLICT:
      case NOT_READY:
      case HISTORY_MISMATCH:
        return null;
      default:
        require(response.getStatus() == MetadataRpc.ResultStatus.OK && _history.equals(history),
            "Current installation work is unavailable: " + response.getStatus());
        break;
    }
    MetadataRpc.InstallationWork work = response.getWork();
    require(uuid(work.getOperationId()).equals(operationId) && work.hasTarget()
        && work.getTarget().hasServer() && _server.equals(work.getTarget().getServer()),
        "Installation work belongs to another operation or SERVER session");
    identity(work);
    require(work.getTable().getLastModifiedIndex() <= response.getRevision().getIndex()
        && work.getSchema().getLastModifiedIndex() <= response.getRevision().getIndex(),
        "Installation dependencies are newer than the captured revision");
    return work;
  }

  private static Identity identity(MetadataRpc.InstallationWork work)
      throws IOException {
    validateMessage(work);
    require(work.getAttempt() > 0 && work.hasOwner() && work.hasTarget() && work.getTarget().hasTarget()
        && work.getTarget().hasServer() && work.hasContent() && work.hasTable() && work.hasSchema()
        && !(work.getInitialLoad() && work.getReconcile()), "Complete installation work required");
    MetadataRpc.ParticipantToken server = work.getTarget().getServer();
    UUID history = uuid(server.getHistoryId());
    MetadataRpc.InstallationTarget target = work.getTarget().getTarget();
    require(target.hasAssignment() && target.hasCompatibility() && target.getCompatibility().hasTable()
        && target.getCompatibility().hasSchema(), "Complete installation target required");
    tableName(target.getTableName());
    identifier(target.getSegmentName(), 1024);
    String rawTable = TableNameBuilder.extractRawTableName(target.getTableName());
    ObjectVersion table = version(target.getCompatibility().getTable(), history);
    ObjectVersion schema = version(target.getCompatibility().getSchema(), history);
    require(table.objectId().equals(uuid(target.getTableObjectId())) && !table.objectId().equals(schema.objectId()),
        "Installation compatibility identities differ");
    validateRecord(work.getTable(), MetadataRpc.RecordKind.TABLE_CONFIG);
    validateRecord(work.getSchema(), MetadataRpc.RecordKind.SCHEMA);
    require(work.getTable().getName().equals(target.getTableName()) && work.getSchema().getName().equals(rawTable)
        && recordVersion(work.getTable()).equals(table) && recordVersion(work.getSchema()).equals(schema),
        "Installation table/schema compatibility differs");
    MetadataRpc.OfflineSegmentContent content = work.getContent();
    validateContent(content);
    require(content.getTableName().equals(target.getTableName())
        && content.getTableObjectId().equals(target.getTableObjectId())
        && content.getSegmentName().equals(target.getSegmentName())
        && content.getSegmentObjectId().equals(target.getSegmentObjectId())
        && content.getContentSha256().equals(target.getContentSha256()), "Installation content identity differs");
    MetadataRpc.OwnerToken owner = work.getOwner();
    require(uuid(owner.getHistoryId()).equals(history) && owner.getLayoutGeneration() == 1 && owner.getOwnerEpoch() > 0
        && owner.hasScope() && owner.getScope().getScopeCase() == MetadataRpc.OwnershipScope.ScopeCase.TABLE_SHARD
        && owner.getScope().getTableShard() >= 0 && owner.getScope().getTableShard() < 24 && owner.hasParticipant(),
        "Installation owner is invalid");
    return new Identity(history, target.getTableName(), table, work.getSchema().getName(), schema,
        target.getSegmentName(), new ObjectVersion(uuid(target.getSegmentObjectId()), 1), target.getContentSha256(),
        participant(server, history, MetadataRpc.ParticipantRole.SERVER), version(target.getAssignment(), history),
        uuid(target.getMaterializationId()), new Operation(uuid(work.getOperationId()), work.getAttempt(),
            new Owner(owner.getLayoutGeneration(), owner.getScope().getTableShard(), owner.getOwnerEpoch(),
                participant(owner.getParticipant(), history, MetadataRpc.ParticipantRole.CONTROLLER))));
  }

  /// Decode a complete detached loading snapshot after checking all captured identities and compatibility versions.
  public static SegmentSnapshot snapshot(MetadataRpc.InstallationWork work)
      throws IOException {
    identity(work);
    TableConfig table = decodeTable(work.getTable());
    require(table.getTableType() == TableType.OFFLINE, "Native installation requires an OFFLINE table");
    Schema schema = decodeSchema(work.getSchema());
    MetadataRpc.OfflineSegmentContent content = work.getContent();
    MetadataRpc.SegmentLoaderDescriptor loader = content.getLoader();
    SegmentLoadMetadata segment = new SegmentLoadMetadata(content.getSegmentName(), loader.getCrc(),
        loader.getDataCrc(), loader.getUseDataCrc(), content.getDownloadUri(),
        loader.hasTier() ? loader.getTier() : null,
        loader.hasCrypterName() ? loader.getCrypterName() : null, loader.getCreationTimeMs(), loader.getPushTimeMs(),
        loader.getCustomConfigsMap(), true);
    return new SegmentSnapshot(table, schema, segment);
  }

  /// Validate the closed wire vocabulary before a runtime consumes a typed response or snapshot projection.
  public static void validateMessage(Message message)
      throws IOException {
    require(message != null && message.getSerializedSize() <= MetadataRpcClient.MAX_RESPONSE_BYTES,
        "Metadata response exceeds the supported bound or is absent");
    rejectUnknown(message);
  }

  private static void rejectUnknown(Message message)
      throws IOException {
    require(message.getUnknownFields().asMap().isEmpty(), "Unsupported metadata protocol field");
    for (Object value : message.getAllFields().values()) {
      if (value instanceof List<?>) {
        for (Object element : (List<?>) value) {
          validateField(element);
        }
      } else {
        validateField(value);
      }
    }
  }

  private static void validateField(Object value)
      throws IOException {
    if (value instanceof Message) {
      rejectUnknown((Message) value);
    } else if (value instanceof EnumValueDescriptor) {
      require(((EnumValueDescriptor) value).getIndex() >= 0, "Unsupported metadata protocol enum value");
    }
  }

  public static TableConfig decodeTable(MetadataRpc.CatalogRecord record)
      throws IOException {
    validateRecord(record, MetadataRpc.RecordKind.TABLE_CONFIG);
    require(record.getValue().getValueCase() == MetadataRpc.CatalogValue.ValueCase.TABLE_CONFIG_JSON,
        "Table record has another value kind");
    TableConfig table = JsonUtils.jsonNodeToObject(object(record.getValue().getTableConfigJson()), TableConfig.class);
    require(record.getName().equals(table.getTableName()), "Table configuration name differs from its record");
    return table;
  }

  public static Schema decodeSchema(MetadataRpc.CatalogRecord record)
      throws IOException {
    validateRecord(record, MetadataRpc.RecordKind.SCHEMA);
    require(record.getValue().getValueCase() == MetadataRpc.CatalogValue.ValueCase.SCHEMA_JSON,
        "Schema record has another value kind");
    Schema schema = JsonUtils.jsonNodeToObject(object(record.getValue().getSchemaJson()), Schema.class);
    require(record.getName().equals(schema.getSchemaName()), "Schema name differs from its record");
    return schema;
  }

  private static void validateRecord(MetadataRpc.CatalogRecord record, MetadataRpc.RecordKind kind)
      throws IOException {
    validateMessage(record);
    identifier(record.getName(), 1024);
    uuid(record.getObjectId());
    require(record.getKind() == kind && record.getVersion() > 0 && record.getSchemaVersion() == 1
        && record.getLastModifiedIndex() > 0 && !record.getDeleted() && record.hasValue(),
        "Invalid live configuration record");
  }

  private static JsonNode object(ByteString bytes)
      throws IOException {
    require(!bytes.isEmpty() && bytes.size() <= MetadataRpcClient.MAX_COMMAND_BYTES, "Invalid configuration length");
    JsonNode value = JSON.readTree(bytes.toByteArray());
    require(value != null && value.isObject(), "Configuration must be a JSON object");
    return value;
  }

  /// Validate an immutable OFFLINE content descriptor before publishing a wire snapshot or using its loading input.
  public static void validateContent(MetadataRpc.OfflineSegmentContent content)
      throws IOException {
    validateMessage(content);
    tableName(content.getTableName());
    identifier(content.getSegmentName(), 1024);
    require(content.hasLoader() && content.getContentSha256().matches("[0-9a-f]{64}"), "Invalid immutable content");
    uuid(content.getTableObjectId());
    uuid(content.getSegmentObjectId());
    identifier(content.getDownloadUri(), 8192);
    try {
      URI uri = URI.create(content.getDownloadUri());
      require(uri.isAbsolute() && uri.getFragment() == null, "Invalid immutable content URI");
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid immutable content URI", e);
    }
    MetadataRpc.SegmentLoaderDescriptor loader = content.getLoader();
    require(loader.getCrc() >= 0 && loader.getDataCrc() >= -1 && (!loader.getUseDataCrc() || loader.getDataCrc() >= 0)
        && loader.getCreationTimeMs() >= -1 && loader.getPushTimeMs() >= -1 && loader.getCustomConfigsCount() <= 128,
        "Invalid segment loader descriptor");
    if (loader.hasTier()) {
      identifier(loader.getTier(), 1024);
    }
    if (loader.hasCrypterName()) {
      identifier(loader.getCrypterName(), 1024);
    }
    for (Map.Entry<String, String> entry : loader.getCustomConfigsMap().entrySet()) {
      identifier(entry.getKey(), 1024);
      require(entry.getValue().length() <= 4096, "Loader custom value exceeds the supported bound");
    }
  }

  private static Participant participant(MetadataRpc.ParticipantToken token, UUID history,
      MetadataRpc.ParticipantRole role)
      throws IOException {
    require(uuid(token.getHistoryId()).equals(history) && token.getRole() == role && token.getRegistrationEpoch() > 0,
        "Invalid installation participant");
    identifier(token.getParticipantId(), 1024);
    return new Participant(token.getParticipantId(), NativeOfflineSegmentAction.Role.valueOf(role.name()),
        uuid(token.getIncarnation()), token.getRegistrationEpoch());
  }

  private static ObjectVersion version(MetadataRpc.ObjectVersion value, UUID history)
      throws IOException {
    require(uuid(value.getHistoryId()).equals(history) && value.getVersion() > 0, "Invalid installation dependency");
    return new ObjectVersion(uuid(value.getObjectId()), value.getVersion());
  }

  private static ObjectVersion recordVersion(MetadataRpc.CatalogRecord record)
      throws IOException {
    return new ObjectVersion(uuid(record.getObjectId()), record.getVersion());
  }

  private static UUID uuid(String value)
      throws IOException {
    try {
      UUID parsed = UUID.fromString(value);
      require(parsed.toString().equals(value), "Expected canonical UUID");
      return parsed;
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid metadata UUID", e);
    }
  }

  private static void identifier(String value, int maximum)
      throws IOException {
    require(!value.isBlank() && value.length() <= maximum && value.chars().noneMatch(Character::isISOControl)
        && StandardCharsets.UTF_8.newEncoder().canEncode(value),
        "Invalid installation identifier");
  }

  private static void tableName(String name)
      throws IOException {
    identifier(name, 1024);
    require(TableNameBuilder.getTableTypeFromTableName(name) == TableType.OFFLINE
        && name.chars().noneMatch(Character::isWhitespace), "Installation requires a canonical OFFLINE table");
    String raw = TableNameBuilder.extractRawTableName(name);
    int separator = raw.indexOf('.');
    require(!raw.isEmpty() && (separator < 0 || separator > 0 && separator < raw.length() - 1
        && separator == raw.lastIndexOf('.')), "Invalid database-qualified OFFLINE table name");
  }

  /// Refresh clears authority before remote I/O; the local guard covers only final publication, never an RPC.
  private final class ExecutionAuthority implements NativeOfflineSegmentAction.Authority {
    private final UUID _operationId;
    private final MetadataRpc.InstallationWork _expected;
    private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();
    @Nullable
    private MetadataRpc.InstallationWork _current;

    private ExecutionAuthority(UUID operationId, MetadataRpc.InstallationWork expected) {
      _operationId = operationId;
      _expected = expected;
      _current = expected;
    }

    @Override
    public void refresh()
        throws Exception {
      _lock.writeLock().lock();
      try {
        _current = null;
      } finally {
        _lock.writeLock().unlock();
      }
      MetadataRpc.InstallationWork current = lookup(_operationId);
      _lock.writeLock().lock();
      try {
        _current = current;
      } finally {
        _lock.writeLock().unlock();
      }
    }

    @Override
    public ReadGuard acquire(Identity expected)
        throws IOException {
      _lock.readLock().lock();
      boolean current;
      try {
        current = _current != null && expected.equals(identity(_current)) && _current.equals(_expected);
      } catch (IOException | RuntimeException e) {
        _lock.readLock().unlock();
        throw e;
      }
      return new ReadGuard() {
        @Override
        public boolean isCurrent() {
          return current;
        }

        @Override
        public void close() {
          _lock.readLock().unlock();
        }
      };
    }
  }

  private static void require(boolean condition, String message)
      throws IOException {
    if (!condition) {
      throw new IOException(message);
    }
  }
}
