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
package org.apache.pinot.common.request.context;

import com.google.protobuf.Message;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Identity;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ObjectVersion;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Participant;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Role;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.NativeQueryIdentity;
import org.apache.pinot.common.request.NativeQueryObject;
import org.apache.pinot.common.request.NativeQueryProcess;
import org.apache.pinot.common.request.NativeQuerySegment;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.thrift.TException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;


/// Immutable, bounded identity of one offline single-table query attempt, shared by the Thrift and gRPC paths.
/// Version 1 is an identity-only contract: it provides neither serving permission nor a query deadline. Callers must
/// independently enforce membership, authenticated process identity, admission and response-delivery fences.
/// Required and optional names match as sets; duplicates or a name in both sets are rejected.
public final class NativeQueryContext {
  public static final int VERSION = 1;
  public static final int MAX_SEGMENTS = 4096;
  public static final int MAX_BYTES = 2 * 1024 * 1024;

  private final UUID _historyId;
  private final ProcessIdentity _broker;
  private final Participant _server;
  private final UUID _queryId;
  private final UUID _attemptId;
  private final String _tableName;
  private final ObjectVersion _table;
  private final String _schemaName;
  private final ObjectVersion _schema;
  private final List<Segment> _requiredSegments;
  private final List<Segment> _optionalSegments;
  private final List<String> _requiredNames;
  private final List<String> _optionalNames;
  private final Map<String, Segment> _segments;

  public NativeQueryContext(int version, UUID historyId, ProcessIdentity broker, Participant server, UUID queryId,
      UUID attemptId, String tableName, ObjectVersion table, String schemaName, ObjectVersion schema,
      List<Segment> requiredSegments, List<Segment> optionalSegments) {
    checkArgument(version == VERSION, "Unsupported native query identity version: %s", version);
    _historyId = requireNonNull(historyId, "historyId");
    _broker = requireNonNull(broker, "broker");
    _server = requireNonNull(server, "server");
    checkArgument(server.role() == Role.SERVER, "Query target must be a server");
    identifier(server.participantId());
    _queryId = requireNonNull(queryId, "queryId");
    _attemptId = requireNonNull(attemptId, "attemptId");
    _tableName = identifier(tableName);
    checkArgument(TableNameBuilder.getTableTypeFromTableName(tableName) == TableType.OFFLINE
        && tableName.chars().noneMatch(Character::isWhitespace), "Native queries require a canonical OFFLINE table");
    String raw = TableNameBuilder.extractRawTableName(tableName);
    int separator = raw.indexOf('.');
    checkArgument(!raw.isEmpty() && (separator < 0 || separator > 0 && separator < raw.length() - 1
        && separator == raw.lastIndexOf('.')), "Invalid database-qualified OFFLINE table name");
    _table = requireNonNull(table, "table");
    _schemaName = identifier(schemaName);
    checkArgument(raw.equals(schemaName), "Schema must match the database-qualified raw table name");
    _schema = requireNonNull(schema, "schema");
    checkArgument(requiredSegments.size() <= MAX_SEGMENTS
        && optionalSegments.size() <= MAX_SEGMENTS - requiredSegments.size(), "Too many native query segments");
    _requiredSegments = List.copyOf(requiredSegments);
    _optionalSegments = List.copyOf(optionalSegments);
    Map<String, Segment> segments = new HashMap<>();
    Set<UUID> objects = new HashSet<>();
    checkArgument(objects.add(table.objectId()) && objects.add(schema.objectId()), "Duplicate config identity");
    Set<UUID> materializations = new HashSet<>();
    for (List<Segment> group : List.of(_requiredSegments, _optionalSegments)) {
      for (Segment segment : group) {
        checkArgument(segments.putIfAbsent(segment.name(), segment) == null, "Duplicate native query segment");
        checkArgument(objects.add(segment.content().objectId()) && objects.add(segment.assignment().objectId())
            && materializations.add(segment.materializationId()), "Duplicate native segment identity");
      }
    }
    _segments = Map.copyOf(segments);
    _requiredNames = names(_requiredSegments);
    _optionalNames = names(_optionalSegments);
    checkArgument(toProto().getSerializedSize() <= MAX_BYTES, "Native query identity exceeds byte bound");
  }

  public int version() {
    return VERSION;
  }

  public UUID historyId() {
    return _historyId;
  }

  public ProcessIdentity broker() {
    return _broker;
  }

  public Participant server() {
    return _server;
  }

  public UUID queryId() {
    return _queryId;
  }

  public UUID attemptId() {
    return _attemptId;
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

  public List<Segment> requiredSegments() {
    return _requiredSegments;
  }

  public List<Segment> optionalSegments() {
    return _optionalSegments;
  }

  public List<String> requiredNames() {
    return _requiredNames;
  }

  public List<String> optionalNames() {
    return _optionalNames;
  }

  @Nullable
  public Segment segment(String name) {
    return _segments.get(name);
  }

  /// Checks the enclosing legacy routing fields without changing their required/optional semantics.
  public void validateRequest(String tableName, String brokerId, @Nullable List<String> requiredNames,
      @Nullable List<String> optionalNames, boolean logicalForm) {
    checkArgument(!logicalForm, "Native query identity does not support logical/per-table segment forms");
    checkArgument(_broker.participantId().equals(brokerId), "Native query broker differs from request");
    validateSegments(tableName, requiredNames, optionalNames);
  }

  /// Revalidates the exact names at the segment acquisition boundary, without resolving serving authority.
  public void validateSegments(String tableName, @Nullable List<String> requiredNames,
      @Nullable List<String> optionalNames) {
    checkArgument(_tableName.equals(tableName), "Native query table differs from request");
    checkArgument(sameNames(_requiredNames, requiredNames) && sameNames(_optionalNames, optionalNames),
        "Native query identities differ from request segment names");
  }

  /// Compares a pinned handle's installation with this request. Mutation owner/attempt and operation completion
  /// are deliberately excluded: completed installations may serve bounded reads independently of the old operation.
  public boolean matchesInstallation(Segment segment, Identity installed) {
    return segment.equals(_segments.get(segment.name())) && _historyId.equals(installed.historyId())
        && _server.equals(installed.server()) && _tableName.equals(installed.tableName())
        && _table.equals(installed.table()) && _schemaName.equals(installed.schemaName())
        && _schema.equals(installed.schema()) && segment.name().equals(installed.segmentName())
        && segment.content().equals(installed.content()) && segment.contentSha256().equals(installed.contentSha256())
        && segment.assignment().equals(installed.assignment())
        && segment.materializationId().equals(installed.materializationId());
  }

  public static NativeQueryContext fromThrift(NativeQueryIdentity wire) {
    requireNonNull(wire, "nativeQueryContext");
    try {
      wire.validate();
    } catch (TException e) {
      throw new IllegalArgumentException("Incomplete native query identity", e);
    }
    checkArgument(wire.getRequiredSegmentsSize() <= MAX_SEGMENTS
        && wire.getOptionalSegmentsSize() <= MAX_SEGMENTS - wire.getRequiredSegmentsSize(),
        "Too many native query segments");
    ProcessIdentity server = process(wire.getServer());
    return new NativeQueryContext(wire.getVersion(), uuid(wire.getHistoryId()), process(wire.getBroker()),
        new Participant(server.participantId(), Role.SERVER, server.incarnation(), server.registrationEpoch()),
        uuid(wire.getQueryId()), uuid(wire.getAttemptId()), wire.getTableName(), object(wire.getTable()),
        wire.getSchemaName(), object(wire.getSchema()),
        wire.getRequiredSegments().stream().map(NativeQueryContext::segment).collect(Collectors.toList()),
        wire.getOptionalSegments().stream().map(NativeQueryContext::segment).collect(Collectors.toList()));
  }

  public static NativeQueryContext fromProto(Server.NativeQueryIdentity wire) {
    requireNonNull(wire, "nativeQueryContext");
    checkArgument(wire.getSerializedSize() <= MAX_BYTES, "Native query identity exceeds byte bound");
    checkArgument(wire.getRequiredSegmentsCount() <= MAX_SEGMENTS
        && wire.getOptionalSegmentsCount() <= MAX_SEGMENTS - wire.getRequiredSegmentsCount(),
        "Too many native query segments");
    known(wire);
    ProcessIdentity server = process(wire.getServer());
    return new NativeQueryContext(wire.getVersion(), uuid(wire.getHistoryId()), process(wire.getBroker()),
        new Participant(server.participantId(), Role.SERVER, server.incarnation(), server.registrationEpoch()),
        uuid(wire.getQueryId()), uuid(wire.getAttemptId()), wire.getTableName(), object(wire.getTable()),
        wire.getSchemaName(), object(wire.getSchema()),
        wire.getRequiredSegmentsList().stream().map(NativeQueryContext::segment).collect(Collectors.toList()),
        wire.getOptionalSegmentsList().stream().map(NativeQueryContext::segment).collect(Collectors.toList()));
  }

  public NativeQueryIdentity toThrift() {
    NativeQueryIdentity wire = new NativeQueryIdentity();
    wire.setVersion(VERSION);
    wire.setHistoryId(_historyId.toString());
    wire.setBroker(thriftProcess(_broker));
    wire.setServer(thriftProcess(new ProcessIdentity(_server.participantId(), _server.incarnation(),
        _server.registrationEpoch())));
    wire.setQueryId(_queryId.toString());
    wire.setAttemptId(_attemptId.toString());
    wire.setTableName(_tableName);
    wire.setTable(thriftObject(_table));
    wire.setSchemaName(_schemaName);
    wire.setSchema(thriftObject(_schema));
    wire.setRequiredSegments(_requiredSegments.stream().map(NativeQueryContext::thriftSegment)
        .collect(Collectors.toList()));
    wire.setOptionalSegments(_optionalSegments.stream().map(NativeQueryContext::thriftSegment)
        .collect(Collectors.toList()));
    return wire;
  }

  public Server.NativeQueryIdentity toProto() {
    return Server.NativeQueryIdentity.newBuilder().setVersion(VERSION).setHistoryId(_historyId.toString())
        .setBroker(protoProcess(_broker)).setServer(protoProcess(new ProcessIdentity(_server.participantId(),
            _server.incarnation(), _server.registrationEpoch())))
        .setQueryId(_queryId.toString()).setAttemptId(_attemptId.toString()).setTableName(_tableName)
        .setTable(protoObject(_table)).setSchemaName(_schemaName).setSchema(protoObject(_schema))
        .addAllRequiredSegments(_requiredSegments.stream().map(NativeQueryContext::protoSegment)
            .collect(Collectors.toList()))
        .addAllOptionalSegments(_optionalSegments.stream().map(NativeQueryContext::protoSegment)
            .collect(Collectors.toList())).build();
  }

  private static ProcessIdentity process(NativeQueryProcess value) {
    return new ProcessIdentity(value.getParticipantId(), uuid(value.getIncarnation()), value.getRegistrationEpoch());
  }

  private static ProcessIdentity process(Server.NativeQueryProcess value) {
    known(value);
    return new ProcessIdentity(value.getParticipantId(), uuid(value.getIncarnation()), value.getRegistrationEpoch());
  }

  private static NativeQueryProcess thriftProcess(ProcessIdentity value) {
    return new NativeQueryProcess(value.participantId(), value.incarnation().toString(), value.registrationEpoch());
  }

  private static Server.NativeQueryProcess protoProcess(ProcessIdentity value) {
    return Server.NativeQueryProcess.newBuilder().setParticipantId(value.participantId())
        .setIncarnation(value.incarnation().toString()).setRegistrationEpoch(value.registrationEpoch()).build();
  }

  private static ObjectVersion object(NativeQueryObject value) {
    return new ObjectVersion(uuid(value.getObjectId()), value.getVersion());
  }

  private static ObjectVersion object(Server.NativeQueryObject value) {
    known(value);
    return new ObjectVersion(uuid(value.getObjectId()), value.getVersion());
  }

  private static NativeQueryObject thriftObject(ObjectVersion value) {
    return new NativeQueryObject(value.objectId().toString(), value.version());
  }

  private static Server.NativeQueryObject protoObject(ObjectVersion value) {
    return Server.NativeQueryObject.newBuilder().setObjectId(value.objectId().toString())
        .setVersion(value.version()).build();
  }

  private static Segment segment(NativeQuerySegment value) {
    return new Segment(value.getName(), object(value.getContent()), value.getContentSha256(),
        object(value.getAssignment()), uuid(value.getMaterializationId()));
  }

  private static Segment segment(Server.NativeQuerySegment value) {
    known(value);
    return new Segment(value.getName(), object(value.getContent()), value.getContentSha256(),
        object(value.getAssignment()), uuid(value.getMaterializationId()));
  }

  private static NativeQuerySegment thriftSegment(Segment value) {
    return new NativeQuerySegment(value.name(), thriftObject(value.content()), value.contentSha256(),
        thriftObject(value.assignment()), value.materializationId().toString());
  }

  private static Server.NativeQuerySegment protoSegment(Segment value) {
    return Server.NativeQuerySegment.newBuilder().setName(value.name()).setContent(protoObject(value.content()))
        .setContentSha256(value.contentSha256()).setAssignment(protoObject(value.assignment()))
        .setMaterializationId(value.materializationId().toString()).build();
  }

  private static void known(Message message) {
    checkArgument(message.getUnknownFields().asMap().isEmpty(), "Unknown native query identity fields");
  }

  private static UUID uuid(String value) {
    UUID parsed = UUID.fromString(requireNonNull(value, "uuid"));
    checkArgument(parsed.toString().equals(value), "Expected canonical UUID");
    return parsed;
  }

  private static String identifier(String value) {
    checkArgument(value != null && !value.isBlank() && value.length() <= 1024
        && value.chars().noneMatch(Character::isISOControl) && StandardCharsets.UTF_8.newEncoder().canEncode(value),
        "Invalid native query identifier");
    return value;
  }

  private static List<String> names(List<Segment> segments) {
    return List.copyOf(segments.stream().map(Segment::name).collect(Collectors.toList()));
  }

  private static boolean sameNames(List<String> expected, @Nullable List<String> actual) {
    if (actual == null) {
      return expected.isEmpty();
    }
    return actual.size() == expected.size() && new HashSet<>(actual).equals(new HashSet<>(expected));
  }

  /// Broker process identity within the enclosing history; the separate target participant always has SERVER role.
  public static final class ProcessIdentity {
    private final String _participantId;
    private final UUID _incarnation;
    private final long _registrationEpoch;

    public ProcessIdentity(String participantId, UUID incarnation, long registrationEpoch) {
      _participantId = identifier(participantId);
      _incarnation = requireNonNull(incarnation, "incarnation");
      checkArgument(registrationEpoch > 0, "Registration epoch must be positive");
      _registrationEpoch = registrationEpoch;
    }

    public String participantId() {
      return _participantId;
    }

    public UUID incarnation() {
      return _incarnation;
    }

    public long registrationEpoch() {
      return _registrationEpoch;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof ProcessIdentity)) {
        return false;
      }
      ProcessIdentity that = (ProcessIdentity) other;
      return _participantId.equals(that._participantId) && _incarnation.equals(that._incarnation)
          && _registrationEpoch == that._registrationEpoch;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_participantId, _incarnation, _registrationEpoch);
    }
  }

  /// Immutable content and replica materialization requested for one required or optional segment.
  public static final class Segment {
    private final String _name;
    private final ObjectVersion _content;
    private final String _contentSha256;
    private final ObjectVersion _assignment;
    private final UUID _materializationId;

    public Segment(String name, ObjectVersion content, String contentSha256, ObjectVersion assignment,
        UUID materializationId) {
      _name = identifier(name);
      _content = requireNonNull(content, "content");
      checkArgument(contentSha256 != null && contentSha256.matches("[0-9a-f]{64}"), "Invalid content SHA-256");
      _contentSha256 = contentSha256;
      _assignment = requireNonNull(assignment, "assignment");
      _materializationId = requireNonNull(materializationId, "materializationId");
    }

    public String name() {
      return _name;
    }

    public ObjectVersion content() {
      return _content;
    }

    public String contentSha256() {
      return _contentSha256;
    }

    public ObjectVersion assignment() {
      return _assignment;
    }

    public UUID materializationId() {
      return _materializationId;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Segment)) {
        return false;
      }
      Segment that = (Segment) other;
      return _name.equals(that._name) && _content.equals(that._content)
          && _contentSha256.equals(that._contentSha256) && _assignment.equals(that._assignment)
          && _materializationId.equals(that._materializationId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_name, _content, _contentSha256, _assignment, _materializationId);
    }
  }
}
