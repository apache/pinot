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
package org.apache.pinot.common.config.provider;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.TableDataManagerMetadata.SegmentSnapshot;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;


/// Native local installation contract. An INSTALLED result proves durable local installation, not committed
/// execution success or query-serving authority. The coordinator must conditionally commit the complete receipt.
public final class NativeOfflineSegmentAction {
  private NativeOfflineSegmentAction() {
  }

  public enum Mode {
    LOAD, RELOAD, RECONCILE
  }

  public enum Status {
    INSTALLED, ABSENT, FENCED
  }

  public enum Role {
    SERVER, CONTROLLER
  }

  /// Executes only explicitly captured actions. The installed pointer is a durable hint, never a success receipt;
  /// execute must verify and restore the segment before returning INSTALLED, including after a process restart.
  public interface Executor {
    Result execute(Request request, Authority authority) throws Exception;

    @Nullable
    Installation installed(String segmentName) throws IOException;
  }

  /// Supplies a local read guard while the segment lock is held. Implementations must perform no remote I/O and
  /// must serialize metadata invalidation with this guard. Consensus validates the same identities again on receipt.
  public interface Authority {
    /// Refreshes a coherent authorized work image outside the segment lock, immediately before final validation.
    void refresh() throws Exception;

    ReadGuard acquire(Identity expected) throws Exception;
  }

  /// Holds local authority stable until the caller finishes publishing the selected installation.
  public interface ReadGuard extends AutoCloseable {
    boolean isCurrent();

    @Override
    void close();
  }

  /// Object incarnation and exact version within the enclosing identity's history.
  public static final class ObjectVersion {
    private final UUID _objectId;
    private final long _version;

    public ObjectVersion(UUID objectId, long version) {
      _objectId = requireNonNull(objectId, "objectId");
      checkArgument(version > 0, "Object version must be positive");
      _version = version;
    }

    public UUID objectId() {
      return _objectId;
    }

    public long version() {
      return _version;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof ObjectVersion)) {
        return false;
      }
      ObjectVersion that = (ObjectVersion) other;
      return _objectId.equals(that._objectId) && _version == that._version;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_objectId, _version);
    }
  }

  /// Exact process registration within the enclosing identity's history.
  public static final class Participant {
    private final String _participantId;
    private final Role _role;
    private final UUID _incarnation;
    private final long _registrationEpoch;

    public Participant(String participantId, Role role, UUID incarnation, long registrationEpoch) {
      _participantId = identifier(participantId);
      _role = requireNonNull(role, "role");
      _incarnation = requireNonNull(incarnation, "incarnation");
      checkArgument(registrationEpoch > 0, "Registration epoch must be positive");
      _registrationEpoch = registrationEpoch;
    }

    public String participantId() {
      return _participantId;
    }

    public Role role() {
      return _role;
    }

    public UUID incarnation() {
      return _incarnation;
    }

    public long registrationEpoch() {
      return _registrationEpoch;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Participant)) {
        return false;
      }
      Participant that = (Participant) other;
      return _participantId.equals(that._participantId) && _role == that._role
          && _incarnation.equals(that._incarnation) && _registrationEpoch == that._registrationEpoch;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_participantId, _role, _incarnation, _registrationEpoch);
    }
  }

  /// Exact table-shard owner; non-table controller work cannot authorize an offline installation.
  public static final class Owner {
    private final long _layoutGeneration;
    private final int _tableShard;
    private final long _ownerEpoch;
    private final Participant _controller;

    public Owner(long layoutGeneration, int tableShard, long ownerEpoch, Participant controller) {
      checkArgument(layoutGeneration > 0 && tableShard >= 0 && ownerEpoch > 0, "Invalid table owner fence");
      _controller = requireNonNull(controller, "controller");
      checkArgument(controller.role() == Role.CONTROLLER, "Owner must be a controller");
      _layoutGeneration = layoutGeneration;
      _tableShard = tableShard;
      _ownerEpoch = ownerEpoch;
    }

    public long layoutGeneration() {
      return _layoutGeneration;
    }

    public int tableShard() {
      return _tableShard;
    }

    public long ownerEpoch() {
      return _ownerEpoch;
    }

    public Participant controller() {
      return _controller;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Owner)) {
        return false;
      }
      Owner that = (Owner) other;
      return _layoutGeneration == that._layoutGeneration && _tableShard == that._tableShard
          && _ownerEpoch == that._ownerEpoch && _controller.equals(that._controller);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_layoutGeneration, _tableShard, _ownerEpoch, _controller);
    }
  }

  /// Stable execution attempt. Progress checkpoints may change a ledger record version without changing this fence.
  public static final class Operation {
    private final UUID _operationId;
    private final long _attempt;
    private final Owner _owner;

    public Operation(UUID operationId, long attempt, Owner owner) {
      _operationId = requireNonNull(operationId, "operationId");
      checkArgument(attempt > 0, "Operation attempt must be positive");
      _attempt = attempt;
      _owner = requireNonNull(owner, "owner");
    }

    public UUID operationId() {
      return _operationId;
    }

    public long attempt() {
      return _attempt;
    }

    public Owner owner() {
      return _owner;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Operation)) {
        return false;
      }
      Operation that = (Operation) other;
      return _operationId.equals(that._operationId) && _attempt == that._attempt && _owner.equals(that._owner);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_operationId, _attempt, _owner);
    }
  }

  /// Complete installation authority. All nested object and participant identities belong to historyId.
  /// The assignment version is its epoch; materializationId identifies that exact desired replica.
  public static final class Identity {
    private final UUID _historyId;
    private final String _tableName;
    private final ObjectVersion _table;
    private final String _schemaName;
    private final ObjectVersion _schema;
    private final String _segmentName;
    private final ObjectVersion _content;
    private final String _contentSha256;
    private final Participant _server;
    private final ObjectVersion _assignment;
    private final UUID _materializationId;
    private final Operation _operation;

    public Identity(UUID historyId, String tableName, ObjectVersion table, String schemaName, ObjectVersion schema,
        String segmentName, ObjectVersion content, String contentSha256, Participant server, ObjectVersion assignment,
        UUID materializationId, Operation operation) {
      _historyId = requireNonNull(historyId, "historyId");
      _tableName = identifier(tableName);
      _table = requireNonNull(table, "table");
      _schemaName = identifier(schemaName);
      _schema = requireNonNull(schema, "schema");
      _segmentName = identifier(segmentName);
      _content = requireNonNull(content, "content");
      _contentSha256 = sha256(contentSha256);
      _server = requireNonNull(server, "server");
      checkArgument(server.role() == Role.SERVER, "Replica participant must be a server");
      _assignment = requireNonNull(assignment, "assignment");
      _materializationId = requireNonNull(materializationId, "materializationId");
      _operation = requireNonNull(operation, "operation");
    }

    public UUID historyId() {
      return _historyId;
    }

    public String tableName() {
      return _tableName;
    }

    public ObjectVersion table() {
      return _table;
    }

    public String schemaName() {
      return _schemaName;
    }

    public ObjectVersion schema() {
      return _schema;
    }

    public String segmentName() {
      return _segmentName;
    }

    public ObjectVersion content() {
      return _content;
    }

    public String contentSha256() {
      return _contentSha256;
    }

    public Participant server() {
      return _server;
    }

    public ObjectVersion assignment() {
      return _assignment;
    }

    public UUID materializationId() {
      return _materializationId;
    }

    public Operation operation() {
      return _operation;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Identity)) {
        return false;
      }
      Identity that = (Identity) other;
      return _historyId.equals(that._historyId) && _tableName.equals(that._tableName) && _table.equals(that._table)
          && _schemaName.equals(that._schemaName) && _schema.equals(that._schema)
          && _segmentName.equals(that._segmentName) && _content.equals(that._content)
          && _contentSha256.equals(that._contentSha256) && _server.equals(that._server)
          && _assignment.equals(that._assignment) && _materializationId.equals(that._materializationId)
          && _operation.equals(that._operation);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_historyId, _tableName, _table, _schemaName, _schema, _segmentName, _content, _contentSha256,
          _server, _assignment, _materializationId, _operation);
    }
  }

  /// Captured local action. expectedInstallationId is a compare-and-set fence for the local active pointer.
  /// Exact completed retries restore their own installation before returning the same receipt.
  public static final class Request {
    private final Mode _mode;
    private final Identity _identity;
    private final SegmentSnapshot _snapshot;
    private final boolean _forceDownload;
    @Nullable
    private final UUID _expectedInstallationId;

    public Request(Mode mode, Identity identity, SegmentSnapshot snapshot, boolean forceDownload,
        @Nullable UUID expectedInstallationId) {
      _mode = requireNonNull(mode, "mode");
      _identity = requireNonNull(identity, "identity");
      _snapshot = requireNonNull(snapshot, "snapshot");
      checkArgument(identity.tableName().equals(snapshot.tableConfig().getTableName())
          && identity.schemaName().equals(snapshot.schema().getSchemaName())
          && identity.segmentName().equals(snapshot.segment().segmentName()), "Loading snapshot identity differs");
      _forceDownload = forceDownload;
      _expectedInstallationId = expectedInstallationId;
    }

    public Mode mode() {
      return _mode;
    }

    public Identity identity() {
      return _identity;
    }

    public SegmentSnapshot snapshot() {
      return _snapshot;
    }

    public boolean forceDownload() {
      return _forceDownload;
    }

    @Nullable
    public UUID expectedInstallationId() {
      return _expectedInstallationId;
    }
  }

  /// Durable installation identity and receipt digest. It does not prove current query-serving authority.
  public static final class Installation {
    private final Identity _identity;
    private final UUID _installationId;
    private final String _manifestSha256;

    public Installation(Identity identity, UUID installationId, String manifestSha256) {
      _identity = requireNonNull(identity, "identity");
      _installationId = requireNonNull(installationId, "installationId");
      _manifestSha256 = sha256(manifestSha256);
    }

    public Identity identity() {
      return _identity;
    }

    public UUID installationId() {
      return _installationId;
    }

    public String manifestSha256() {
      return _manifestSha256;
    }
  }

  /// Physical action outcome; only a verified installation carries a receipt for conditional consensus publication.
  public static final class Result {
    private final Status _status;
    @Nullable
    private final Installation _installation;

    public Result(Status status, @Nullable Installation installation) {
      _status = requireNonNull(status, "status");
      checkArgument((status == Status.INSTALLED) == (installation != null), "Only INSTALLED has a receipt");
      _installation = installation;
    }

    public Status status() {
      return _status;
    }

    @Nullable
    public Installation installation() {
      return _installation;
    }
  }

  private static String identifier(String value) {
    requireNonNull(value, "identifier");
    checkArgument(!value.isBlank() && value.length() <= 1024 && value.indexOf('\0') < 0, "Invalid identifier");
    return value;
  }

  private static String sha256(String value) {
    requireNonNull(value, "sha256");
    checkArgument(value.matches("[0-9a-f]{64}"), "Expected lowercase SHA-256");
    return value;
  }
}
