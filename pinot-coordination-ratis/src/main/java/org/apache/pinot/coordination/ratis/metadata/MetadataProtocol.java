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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;


/// Versioned internal catalog protocol. Client names are identities, not authentication credentials. These commands
/// are not a public table lifecycle API; domain services must additionally validate permissions and ownership fences.
/// Configuration transactions and membership commands have separate, closed mutation APIs.
public final class MetadataProtocol {
  public static final int FORMAT_VERSION = 1;
  /// The earlier experimental membership log format did not enforce safe timing across policy changes.
  public static final int MEMBERSHIP_COMMAND_FORMAT_VERSION = 2;
  public static final int MAX_COMMAND_BYTES = 1_048_576;
  public static final int MAX_MUTATIONS = 128;
  public static final int MAX_STREAMS_PER_CLIENT = 16;
  public static final int MAX_CHANGE_TRANSACTIONS = 128;
  public static final int MAX_CHANGE_RECORDS = MAX_MUTATIONS;
  public static final int MIN_CHANGE_BYTES = 1024;
  public static final int MAX_CHANGE_BYTES = 2 * MAX_COMMAND_BYTES;
  public static final int MAX_IMAGE_BYTES = 64 * MAX_COMMAND_BYTES;
  public static final int MAX_IMAGE_RECORDS = 100_000;
  public static final String CLUSTER_CONFIG_NAME = "cluster";

  private static final ObjectMapper MAPPER = JsonMapper.builder(JsonFactory.builder()
      .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
      .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(64)
          .maxStringLength(MAX_COMMAND_BYTES).build()).build())
      .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
      .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
      .enable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES)
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
      .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS).build();

  private MetadataProtocol() {
  }

  public enum RecordKind {
    CLUSTER_CONFIG, SCHEMA, TABLE_CONFIG, MEMBERSHIP_CONTROL, PARTICIPANT, OWNER_LAYOUT, OWNER_ASSIGNMENT,
    CONTROLLER_OPERATION
  }

  public enum Action {
    CREATE, UPDATE, DELETE
  }

  public enum Status {
    OK, CONFLICT, FENCED, HISTORY_MISMATCH, INVALID_REQUEST, NOT_FOUND, RESULT_EXPIRED, OUT_OF_ORDER,
    UNACKNOWLEDGED_RESULT, LIMIT_EXCEEDED, CURSOR_EXPIRED, RESPONSE_TOO_LARGE, NOT_READY
  }

  /// A revision identifies a committed image in one history, independently of an individual object's version.
  public record Revision(UUID historyId, long index) {
  }

  /// The stable object ID fences updates across delete/recreate of the same name.
  public record ObjectVersion(UUID historyId, UUID objectId, long version) {
  }

  /// Immutable catalog value. A tombstone retains the object's identity/version but has no value. Its schema version
  /// describes the record encoding, independently of command envelopes and disk-snapshot formats.
  public record CatalogRecord(RecordKind kind, String name, UUID objectId, long version, int schemaVersion,
                              long lastModifiedIndex, boolean deleted, JsonNode value) {
    public CatalogRecord {
      value = value == null ? null : value.deepCopy();
    }

    @Override
    public JsonNode value() {
      return value == null ? null : value.deepCopy();
    }
  }

  public record Result(Status status, Revision revision, String detail, Long clientEpoch, List<ObjectVersion> versions,
                       CatalogRecord record) {
    public Result {
      versions = versions == null ? null : List.copyOf(versions);
    }
  }

  /// Filter identity includes the transport's authorization scope. This identifier is not an access grant: the
  /// transport must authenticate the caller and revalidate the scope on every snapshot/change request.
  public record FeedScope(Set<RecordKind> kinds, String accessScopeId) {
    public FeedScope {
      Objects.requireNonNull(kinds, "kinds");
      if (kinds.isEmpty() || accessScopeId == null || accessScopeId.isBlank() || accessScopeId.length() > 1024) {
        throw new IllegalArgumentException("Feed scope requires record kinds and an access-scope identity");
      }
      kinds = Collections.unmodifiableSet(EnumSet.copyOf(kinds));
    }

    public String scopeId() {
      try {
        return sha256(encodeValue(this));
      } catch (IOException e) {
        throw new IllegalStateException("Cannot encode metadata feed scope", e);
      }
    }
  }

  public record FeedCursor(UUID historyId, long index, String scopeId) {
  }

  /// Detached live records captured at one revision. The transport owns bounded paging handles; it must verify this
  /// cursor has not expired before serving each page and the final snapshot-to-feed handoff.
  public record CatalogImage(FeedCursor cursor, List<CatalogRecord> records) {
    public CatalogImage {
      records = List.copyOf(records);
    }
  }

  /// One complete applied command, including an empty progress event for a rejection, retry or unrelated update.
  /// previousIndex is the preceding catalog-command index; Raft configuration/no-op indexes may be skipped.
  public record ChangeTransaction(long previousIndex, long index, List<CatalogRecord> records) {
    public ChangeTransaction {
      records = List.copyOf(records);
    }
  }

  /// An OK page covers one contiguous chain from `from` through `next`, including empty progress events. A complete
  /// filtered transaction exceeding either record or byte bounds returns RESPONSE_TOO_LARGE rather than splitting it.
  public record ChangePage(Status status, FeedCursor from, FeedCursor next, long retentionFloor,
                           List<ChangeTransaction> transactions, boolean hasMore) {
    public ChangePage {
      transactions = List.copyOf(transactions);
    }
  }

  /// A bounded export cannot contain the requested complete image. Transports map this to RESPONSE_TOO_LARGE and
  /// must not retry indefinitely with an unchanged scope or advertise a partially captured image as complete.
  public static final class ResponseTooLargeException extends IOException {
    public ResponseTooLargeException(String message) {
      super(message);
    }
  }

  /// Mutations are internal catalog changes, not completed segment/table lifecycle actions. A create expects version
  /// zero and an unused object ID. Updates/deletes expect the complete currently active incarnation and version.
  public record Mutation(Action action, RecordKind kind, String name, ObjectVersion expected, JsonNode value) {
    public Mutation {
      value = value == null ? null : value.deepCopy();
    }

    @Override
    public JsonNode value() {
      return value == null ? null : value.deepCopy();
    }

    public static Mutation createClusterConfig(UUID historyId, UUID objectId, Map<String, String> values) {
      return new Mutation(Action.CREATE, RecordKind.CLUSTER_CONFIG, CLUSTER_CONFIG_NAME,
          new ObjectVersion(historyId, objectId, 0), JsonUtils.objectToJsonNode(values));
    }

    public static Mutation updateClusterConfig(ObjectVersion expected, Map<String, String> values) {
      return new Mutation(Action.UPDATE, RecordKind.CLUSTER_CONFIG, CLUSTER_CONFIG_NAME, expected,
          JsonUtils.objectToJsonNode(values));
    }

    public static Mutation createSchema(UUID historyId, UUID objectId, Schema schema) {
      return new Mutation(Action.CREATE, RecordKind.SCHEMA, schema.getSchemaName(),
          new ObjectVersion(historyId, objectId, 0), JsonUtils.objectToJsonNode(schema));
    }

    public static Mutation updateSchema(ObjectVersion expected, Schema schema) {
      return new Mutation(Action.UPDATE, RecordKind.SCHEMA, schema.getSchemaName(), expected,
          JsonUtils.objectToJsonNode(schema));
    }

    public static Mutation createTableConfig(UUID historyId, UUID objectId, TableConfig tableConfig) {
      return new Mutation(Action.CREATE, RecordKind.TABLE_CONFIG, tableConfig.getTableName(),
          new ObjectVersion(historyId, objectId, 0), JsonUtils.objectToJsonNode(tableConfig));
    }

    public static Mutation updateTableConfig(ObjectVersion expected, TableConfig tableConfig) {
      return new Mutation(Action.UPDATE, RecordKind.TABLE_CONFIG, tableConfig.getTableName(), expected,
          JsonUtils.objectToJsonNode(tableConfig));
    }

    public static Mutation delete(RecordKind kind, String name, ObjectVersion expected) {
      return new Mutation(Action.DELETE, kind, name, expected, null);
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "operation")
  @JsonSubTypes({@JsonSubTypes.Type(value = RegisterClient.class, name = "REGISTER_CLIENT"),
      @JsonSubTypes.Type(value = RetireClient.class, name = "RETIRE_CLIENT"),
      @JsonSubTypes.Type(value = Transaction.class, name = "TRANSACTION"),
      @JsonSubTypes.Type(value = Acknowledge.class, name = "ACKNOWLEDGE"),
      @JsonSubTypes.Type(value = MembershipCommand.class, name = "MEMBERSHIP"),
      @JsonSubTypes.Type(value = OwnershipCommand.class, name = "OWNERSHIP"),
      @JsonSubTypes.Type(value = OperationCommand.class, name = "CONTROLLER_OPERATION"),
      @JsonSubTypes.Type(value = ReadBarrier.class, name = "READ_BARRIER")})
  public sealed interface Command permits RegisterClient, RetireClient, SequencedCommand, Acknowledge,
      MembershipCommand, ReadBarrier {
    UUID historyId();

    String clientId();
  }

  /// All effect-producing controller commands share the same durable request stream and retained outcome contract.
  public sealed interface SequencedCommand extends Command permits Transaction, OwnershipCommand, OperationCommand {
    long clientEpoch();

    String streamId();

    long sequence();
  }

  public record OwnershipCommand(UUID historyId, String clientId, long clientEpoch, String streamId, long sequence,
                                 OwnershipProtocol.Operation request) implements SequencedCommand {
  }

  public record OperationCommand(UUID historyId, String clientId, long clientEpoch, String streamId, long sequence,
                                 OperationProtocol.Operation request) implements SequencedCommand {
  }

  /// Internal committed control-plane read. It carries no mutation or durable client request identity; each new
  /// invocation establishes its own log position. Public transports authorize the enclosed typed query first.
  public record ReadBarrier(Query request) implements Command {
    @Override
    public UUID historyId() {
      return request == null ? null : request.historyId();
    }

    @Override
    public String clientId() {
      return "read-barrier";
    }
  }

  /// Transport authorization must distinguish internal leader commands from participant operations.
  public record MembershipCommand(UUID historyId, String clientId, MembershipProtocol.Operation request)
      implements Command {
  }

  /// A new client incarnation conditionally advances the durable epoch. Retrying the current incarnation is safe.
  public record RegisterClient(UUID historyId, String clientId, UUID incarnation, long expectedEpoch)
      implements Command {
  }

  /// Retired epochs remain rejected even after all of their outcomes have been discarded.
  public record RetireClient(UUID historyId, String clientId, long clientEpoch) implements Command {
  }

  /// Streams are serialized: acknowledge the previous result before advancing the sequence. At most one outcome
  /// remains outstanding per stream, bounding result memory without silently expiring retry protection.
  public record Transaction(UUID historyId, String clientId, long clientEpoch, String streamId, long sequence,
                            List<Mutation> mutations) implements SequencedCommand {
    public Transaction {
      mutations = mutations == null ? null : List.copyOf(mutations);
    }
  }

  public record Acknowledge(UUID historyId, String clientId, long clientEpoch, String streamId, long throughSequence)
      implements Command {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "operation")
  @JsonSubTypes({@JsonSubTypes.Type(value = GetRecord.class, name = "GET_RECORD"),
      @JsonSubTypes.Type(value = GetOutcome.class, name = "GET_OUTCOME"),
      @JsonSubTypes.Type(value = GetRevision.class, name = "GET_REVISION"),
      @JsonSubTypes.Type(value = MembershipQuery.class, name = "MEMBERSHIP")})
  public sealed interface Query permits GetRecord, GetOutcome, GetRevision, MembershipQuery {
    UUID historyId();
  }

  public record MembershipQuery(UUID historyId, MembershipProtocol.Read request) implements Query {
  }

  public record GetRecord(UUID historyId, RecordKind kind, String name) implements Query {
  }

  /// A small consensus read-barrier response; catalog images are captured locally after reaching this revision.
  public record GetRevision(UUID historyId) implements Query {
  }

  /// Lookup authority must be checked by the transport; possession of the identity is not an authorization grant.
  public record GetOutcome(UUID historyId, String clientId, long clientEpoch, String streamId, long sequence)
      implements Query {
  }

  record CommandEnvelope(int formatVersion, Command command) {
  }

  record QueryEnvelope(int formatVersion, Query query) {
  }

  public static byte[] encode(Command command)
      throws IOException {
    int version = command instanceof MembershipCommand ? MEMBERSHIP_COMMAND_FORMAT_VERSION : FORMAT_VERSION;
    return bounded(encodeValue(new CommandEnvelope(version, command)));
  }

  public static byte[] encode(Query query)
      throws IOException {
    return bounded(encodeValue(new QueryEnvelope(FORMAT_VERSION, query)));
  }

  public static Command decodeCommand(byte[] bytes)
      throws IOException {
    CommandEnvelope envelope = MAPPER.readValue(bounded(bytes), CommandEnvelope.class);
    Command command = envelope.command();
    if (command instanceof MembershipCommand && envelope.formatVersion() == FORMAT_VERSION) {
      throw new IOException("Legacy membership log requires an explicit maintenance-window migration to a new history");
    }
    int expected = command instanceof MembershipCommand ? MEMBERSHIP_COMMAND_FORMAT_VERSION : FORMAT_VERSION;
    if (envelope.formatVersion() != expected || command == null) {
      throw new IOException("Unsupported command format");
    }
    return command;
  }

  public static Result decodeResult(byte[] bytes)
      throws IOException {
    if (bytes == null || bytes.length == 0 || bytes.length > MAX_CHANGE_BYTES) {
      throw new IOException("Metadata result exceeds the supported size");
    }
    return MAPPER.readValue(bytes, Result.class);
  }

  static <T> T decodeValue(byte[] bytes, Class<T> type)
      throws IOException {
    if (bytes == null || bytes.length == 0 || bytes.length > MAX_CHANGE_BYTES) {
      throw new IOException("Metadata response exceeds the supported size");
    }
    return MAPPER.readValue(bytes, type);
  }

  public static Query decodeQuery(byte[] bytes)
      throws IOException {
    QueryEnvelope envelope = MAPPER.readValue(bounded(bytes), QueryEnvelope.class);
    if (envelope.formatVersion() != FORMAT_VERSION || envelope.query() == null) {
      throw new IOException("Unsupported query format");
    }
    return envelope.query();
  }

  static String digest(Command command)
      throws IOException {
    return sha256(encode(command));
  }

  private static String sha256(byte[] bytes) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  static JsonNode toTree(Object value) {
    return MAPPER.valueToTree(value);
  }

  static <T> T fromTree(JsonNode value, Class<T> type)
      throws IOException {
    return MAPPER.treeToValue(value, type);
  }

  static byte[] encodeValue(Object value)
      throws IOException {
    return MAPPER.writeValueAsBytes(canonical(toTree(value)));
  }

  static JsonNode canonical(JsonNode value) {
    if (value.isObject()) {
      ObjectNode result = MAPPER.createObjectNode();
      List<String> names = new ArrayList<>();
      value.fieldNames().forEachRemaining(names::add);
      names.sort(String::compareTo);
      names.forEach(name -> result.set(name, canonical(value.get(name))));
      return result;
    }
    if (value.isArray()) {
      ArrayNode result = MAPPER.createArrayNode();
      value.forEach(element -> result.add(canonical(element)));
      return result;
    }
    if (value.isFloatingPointNumber()) {
      return MAPPER.getNodeFactory().numberNode(value.decimalValue().stripTrailingZeros());
    }
    return value;
  }

  private static byte[] bounded(byte[] bytes)
      throws IOException {
    if (bytes == null || bytes.length == 0 || bytes.length > MAX_COMMAND_BYTES) {
      throw new IOException("Metadata request exceeds the supported size");
    }
    return bytes;
  }
}
