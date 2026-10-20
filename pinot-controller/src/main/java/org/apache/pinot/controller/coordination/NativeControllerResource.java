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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessControlUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.coordination.NativeControllerMetadata.Snapshot;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Mutation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Result;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.SchemaInfo;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Native metadata routes with the normal controller JSON shapes and authorization actions. All successful
/// responses identify one fresh committed image. Membership is projected explicitly, without Helix instance or
/// ExternalView emulation; a live lease does not certify query serving. Conditional writes publish configuration
/// only, and reject changes requiring native segment reconciliation. No query or table cleanup route is registered.
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public final class NativeControllerResource {
  public static final String REVISION_HEADER = "X-Pinot-Metadata-Revision";
  private static final ObjectMapper REQUEST_JSON = JsonMapper.builder(JsonFactory.builder()
      .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
      .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(64)
          .maxStringLength(MetadataProtocol.MAX_COMMAND_BYTES).build()).build())
      .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).build();
  private final NativeControllerMetadata _metadata;
  private final AccessControlFactory _accessControlFactory;

  @Inject
  public NativeControllerResource(NativeControllerMetadata metadata, AccessControlFactory accessControlFactory) {
    _metadata = metadata;
    _accessControlFactory = accessControlFactory;
  }

  @GET
  @Path("/cluster/info")
  @Authenticate(AccessType.READ)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_CLUSTER_CONFIG)
  public Response clusterInfo(@Context HttpHeaders headers) {
    Snapshot snapshot = read(headers, null, Actions.Cluster.GET_CLUSTER_CONFIG, "/cluster/info");
    return response(snapshot, Map.of("clusterName", _metadata.clusterName()));
  }

  @GET
  @Path("/cluster/configs")
  @Authenticate(AccessType.READ)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_CLUSTER_CONFIG)
  public Response clusterConfigs(@Context HttpHeaders headers) {
    Snapshot snapshot = read(headers, null, Actions.Cluster.GET_CLUSTER_CONFIG, "/cluster/configs");
    return tagged(snapshot, snapshot.clusterConfig(), snapshot.clusterRecord());
  }

  @GET
  @Path("/schemas")
  @Authenticate(AccessType.READ)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_SCHEMA)
  public Response schemas(@Context HttpHeaders headers) {
    Snapshot snapshot = read(headers, null, Actions.Cluster.GET_SCHEMA, "/schemas");
    List<String> names = snapshot.schemas().keySet().stream().filter(NativeControllerResource::supportedRawName)
        .filter(name -> inDatabase(name, headers))
        .filter(name -> allowed(name, headers, Actions.Table.GET_SCHEMA, "/schemas")).sorted().toList();
    return response(snapshot, names);
  }

  @GET
  @Path("/schemas/info")
  @Authenticate(AccessType.READ)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_SCHEMAS_INFO)
  public Response schemasInfo(@Context HttpHeaders headers) {
    Snapshot snapshot = read(headers, null, Actions.Cluster.GET_SCHEMAS_INFO, "/schemas/info");
    List<SchemaInfo> schemas = new ArrayList<>();
    snapshot.schemas().keySet().stream().sorted().filter(NativeControllerResource::supportedRawName)
        .filter(name -> inDatabase(name, headers))
        .filter(name -> allowed(name, headers, Actions.Table.GET_SCHEMA, "/schemas/info"))
        .forEach(name -> schemas.add(new SchemaInfo(schema(snapshot.schemas().get(name)))));
    return response(snapshot, Map.of("schemasInfo", schemas));
  }

  @GET
  @Path("/schemas/{schemaName}")
  @Authenticate(AccessType.READ)
  @Authorize(targetType = TargetType.TABLE, paramName = "schemaName", action = Actions.Table.GET_SCHEMA)
  public Response schema(@PathParam("schemaName") String name, @Context HttpHeaders headers) {
    name = translateSchemaName(name, headers);
    Snapshot snapshot = read(headers, name, Actions.Table.GET_SCHEMA, "/schemas/" + name);
    CatalogRecord record = snapshot.schemas().get(name);
    if (record == null) {
      throw new WebApplicationException("Schema not found", Response.Status.NOT_FOUND);
    }
    return tagged(snapshot, record.value(), record);
  }

  @GET
  @Path("/tables")
  @Authenticate(AccessType.READ)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TABLE)
  public Response tables(@QueryParam("type") String type, @QueryParam("taskType") String taskType,
      @QueryParam("sortType") String sortType, @DefaultValue("true") @QueryParam("sortAsc") boolean sortAsc,
      @Context HttpHeaders headers) {
    if (taskType != null && !taskType.isBlank() || sortType != null && !"name".equalsIgnoreCase(sortType)) {
      throw new WebApplicationException("Native table listing supports name sorting and no task filter",
          Response.Status.NOT_IMPLEMENTED);
    }
    boolean dimension = "dimension".equalsIgnoreCase(type);
    TableType tableType = dimension ? TableType.OFFLINE : tableType(type);
    Snapshot snapshot = read(headers, null, Actions.Cluster.GET_TABLE, "/tables");
    List<String> names = snapshot.tables().values().stream().filter(record -> inDatabase(record.name(), headers))
        .filter(record -> tableType == null || TableNameBuilder.getTableTypeFromTableName(record.name()) == tableType)
        .filter(record -> !dimension || table(record).isDimTable())
        .filter(record -> allowed(record.name(), headers, Actions.Table.GET_TABLE_CONFIG, "/tables"))
        .map(record -> tableType == null ? TableNameBuilder.extractRawTableName(record.name()) : record.name())
        .distinct().sorted(sortAsc ? Comparator.naturalOrder() : Comparator.reverseOrder()).toList();
    return response(snapshot, Map.of("tables", names));
  }

  @GET
  @Path("/tables/{tableName}")
  @Authenticate(AccessType.READ)
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_TABLE_CONFIG)
  public Response table(@PathParam("tableName") String name, @QueryParam("type") String type,
      @Context HttpHeaders headers) {
    name = translateTableName(name, headers);
    TableType requested = tableType(type);
    TableType suffix = TableNameBuilder.getTableTypeFromTableName(name);
    if (requested != null && suffix != null && requested != suffix) {
      throw new WebApplicationException("Table name and type disagree", Response.Status.BAD_REQUEST);
    }
    requested = requested == null ? suffix : requested;
    String raw = TableNameBuilder.extractRawTableName(name);
    Snapshot snapshot = read(headers, raw, Actions.Table.GET_TABLE_CONFIG, "/tables/" + name);
    ObjectNode configs = JsonUtils.newObjectNode();
    CatalogRecord selected = null;
    for (TableType candidate : TableType.values()) {
      if (requested == null || requested == candidate) {
        CatalogRecord record = snapshot.tables().get(TableNameBuilder.forType(candidate).tableNameWithType(raw));
        if (record != null) {
          configs.set(candidate.name(), record.value());
          selected = record;
        }
      }
    }
    if (configs.isEmpty()) {
      throw new WebApplicationException("Table not found", Response.Status.NOT_FOUND);
    }
    return configs.size() == 1 ? tagged(snapshot, configs, selected) : response(snapshot, configs);
  }

  @GET
  @Path("/tables/{tableName}/schema")
  @Authenticate(AccessType.READ)
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_SCHEMA)
  public Response tableSchema(@PathParam("tableName") String name, @Context HttpHeaders headers) {
    name = translateTableName(name, headers);
    Snapshot snapshot = read(headers, name, Actions.Table.GET_SCHEMA, "/tables/" + name + "/schema");
    CatalogRecord record = snapshot.schemas().get(TableNameBuilder.extractRawTableName(name));
    if (record == null) {
      throw new WebApplicationException("Schema not found for table", Response.Status.NOT_FOUND);
    }
    return tagged(snapshot, record.value(), record);
  }

  @GET
  @Path("/metadata/participants")
  @Authenticate(AccessType.READ)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_INSTANCE)
  public Response participants(@Context HttpHeaders headers) {
    Snapshot snapshot = read(headers, null, Actions.Cluster.GET_INSTANCE, "/metadata/participants");
    ObjectNode result = JsonUtils.newObjectNode();
    result.set("revision", revision(snapshot.revision()));
    result.putObject("control").put("round", Long.toString(snapshot.control().round()));
    ArrayNode participants = result.putArray("participants");
    snapshot.participants().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
      ParticipantState state = entry.getValue();
      ParticipantToken token = state.participant();
      ObjectNode participant = participants.addObject();
      participant.putObject("participant").put("historyId", token.historyId().toString())
          .put("participantId", token.participantId()).put("role", token.role().name())
          .put("incarnation", token.incarnation().toString())
          .put("registrationEpoch", Long.toString(token.registrationEpoch()));
      participant.set("endpoints", JsonUtils.objectToJsonNode(state.endpoints()));
      participant.set("capabilities", JsonUtils.objectToJsonNode(state.capabilities()));
      participant.put("expiryRound", Long.toString(state.expiryRound())).put("ready", state.ready())
          .put("draining", state.draining()).put("retired", state.retired())
          .put("live", !state.retired() && state.expiryRound() > snapshot.control().round());
      participant.set("appliedRevision", revision(state.appliedRevision()));
    });
    return response(snapshot, result);
  }

  @GET
  @Path("/metadata/capabilities")
  @Authenticate(AccessType.READ)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_APP_CONFIG)
  public Response capabilities(@Context HttpHeaders headers) {
    Snapshot snapshot = read(headers, null, Actions.Cluster.GET_APP_CONFIG, "/metadata/capabilities");
    return response(snapshot, Map.of("schemaCreate", true, "schemaUpdate", true, "schemaDelete", true,
        "tableCreate", true, "tableUpdate", true, "tableDelete", false, "clusterConfigUpdate", true,
        "clusterConfigDelete", true, "tableSize", true, "columnCompressionStats", false));
  }

  @POST
  @Path("/schemas")
  @Consumes(MediaType.APPLICATION_JSON)
  @ManualAuthorization
  public Response createSchema(String body, @DefaultValue("false") @QueryParam("override") boolean override,
      @DefaultValue("false") @QueryParam("force") boolean force, @Context HttpHeaders headers) {
    if (override || force) {
      throw error(501, "Native schema creation does not override existing schemas or skip validation", false);
    }
    Schema schema = parse(body, Schema.class);
    schema.setSchemaName(translateSchemaName(schema.getSchemaName(), headers));
    String name = schema.getSchemaName();
    Snapshot snapshot = writeSnapshot(headers, name, AccessType.CREATE, Actions.Table.CREATE_SCHEMA, "/schemas");
    UUID id = createId(snapshot, RecordKind.SCHEMA, name, snapshot.schemas().get(name), headers);
    validate(() -> _metadata.validateSchema(snapshot, schema, null));
    return commit(snapshot, Mutation.createSchema(snapshot.revision().historyId(), id, schema), headers, name,
        AccessType.CREATE, Actions.Table.CREATE_SCHEMA, "/schemas", "Schema configuration published");
  }

  @PUT
  @Path("/schemas/{schemaName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @Authorize(targetType = TargetType.TABLE, paramName = "schemaName", action = Actions.Table.UPDATE_SCHEMA)
  public Response updateSchema(@PathParam("schemaName") String name, String body,
      @DefaultValue("false") @QueryParam("reload") boolean reload,
      @DefaultValue("false") @QueryParam("force") boolean force, @Context HttpHeaders headers) {
    if (reload || force) {
      throw error(501, "Native schema reload and forced updates are not supported", false);
    }
    name = translateSchemaName(name, headers);
    Schema schema = parse(body, Schema.class);
    schema.setSchemaName(translateSchemaName(schema.getSchemaName(), headers));
    if (!name.equals(schema.getSchemaName())) {
      throw error(400, "Schema path and body names differ", false);
    }
    String path = "/schemas/" + name;
    Snapshot snapshot = writeSnapshot(headers, name, AccessType.UPDATE, Actions.Table.UPDATE_SCHEMA, path);
    CatalogRecord previous = snapshot.schemas().get(name);
    ObjectVersion expected = match(snapshot, previous, headers);
    validate(() -> _metadata.validateSchema(snapshot, schema, schema(previous)));
    return commit(snapshot, Mutation.updateSchema(expected, schema), headers, name, AccessType.UPDATE,
        Actions.Table.UPDATE_SCHEMA, path, "Schema configuration published");
  }

  @DELETE
  @Path("/schemas/{schemaName}")
  @Authenticate(AccessType.DELETE)
  @Authorize(targetType = TargetType.TABLE, paramName = "schemaName", action = Actions.Table.DELETE_SCHEMA)
  public Response deleteSchema(@PathParam("schemaName") String name, @Context HttpHeaders headers) {
    name = translateSchemaName(name, headers);
    String path = "/schemas/" + name;
    Snapshot snapshot = writeSnapshot(headers, name, AccessType.DELETE, Actions.Table.DELETE_SCHEMA, path);
    ObjectVersion expected = match(snapshot, snapshot.schemas().get(name), headers);
    try {
      _metadata.validateSchemaDeletion(snapshot, name);
    } catch (UnsupportedOperationException e) {
      throw error(501, e.getMessage(), false);
    } catch (IllegalStateException e) {
      throw error(409, e.getMessage(), false);
    }
    return commit(snapshot, Mutation.delete(RecordKind.SCHEMA, name, expected), headers, name, AccessType.DELETE,
        Actions.Table.DELETE_SCHEMA, path, "Schema configuration deleted");
  }

  @POST
  @Path("/tables")
  @Consumes(MediaType.APPLICATION_JSON)
  @ManualAuthorization
  public Response createTable(String body, @QueryParam("validationTypesToSkip") String typesToSkip,
      @Context HttpHeaders headers) {
    if (typesToSkip != null && !typesToSkip.isBlank()) {
      throw error(501, "Native table creation does not skip validation", false);
    }
    TableConfig table = parse(body, TableConfig.class);
    table.setTableName(translateTableName(table.getTableName(), headers));
    String name = table.getTableName();
    Snapshot snapshot = writeSnapshot(headers, name, AccessType.CREATE, Actions.Table.CREATE_TABLE, "/tables");
    UUID id = createId(snapshot, RecordKind.TABLE_CONFIG, name, snapshot.tables().get(name), headers);
    validate(() -> _metadata.validateTable(snapshot, table, null));
    return commit(snapshot, Mutation.createTableConfig(snapshot.revision().historyId(), id, table), headers, name,
        AccessType.CREATE, Actions.Table.CREATE_TABLE, "/tables",
        "Table configuration published; no segments assigned");
  }

  @PUT
  @Path("/tables/{tableName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.UPDATE_TABLE_CONFIG)
  public Response updateTable(@PathParam("tableName") String name, String body,
      @QueryParam("validationTypesToSkip") String typesToSkip,
      @DefaultValue("false") @QueryParam("force") boolean force,
      @Context HttpHeaders headers) {
    if (force || typesToSkip != null && !typesToSkip.isBlank()) {
      throw error(501, "Native table updates do not skip validation", false);
    }
    TableConfig table = parse(body, TableConfig.class);
    table.setTableName(translateTableName(table.getTableName(), headers));
    if (table.getTableType() == null) {
      throw error(400, "Table type is required", false);
    }
    name = translateTableName(TableNameBuilder.forType(table.getTableType()).tableNameWithType(name), headers);
    if (!name.equals(table.getTableName())) {
      throw error(400, "Table path and body names differ", false);
    }
    String path = "/tables/" + name;
    Snapshot snapshot = writeSnapshot(headers, name, AccessType.UPDATE, Actions.Table.UPDATE_TABLE_CONFIG, path);
    CatalogRecord previous = snapshot.tables().get(name);
    ObjectVersion expected = match(snapshot, previous, headers);
    validate(() -> _metadata.validateTable(snapshot, table, table(previous)));
    return commit(snapshot, Mutation.updateTableConfig(expected, table), headers, name, AccessType.UPDATE,
        Actions.Table.UPDATE_TABLE_CONFIG, path, "Table configuration published");
  }

  @POST
  @Path("/cluster/configs")
  @Consumes(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.UPDATE_CLUSTER_CONFIG)
  public Response updateClusterConfig(String body, @Context HttpHeaders headers) {
    JsonNode changes = parseObject(body);
    Snapshot snapshot = writeSnapshot(headers, null, AccessType.UPDATE, Actions.Cluster.UPDATE_CLUSTER_CONFIG,
        "/cluster/configs");
    Map<String, String> values = new HashMap<>(snapshot.clusterConfig());
    for (var entry : changes.properties()) {
      if (entry.getKey().isBlank() || entry.getKey().length() > 1024
          || !entry.getValue().isTextual() && !entry.getValue().isNull()) {
        throw error(400, "Cluster config requires nonblank keys and string or null values", false);
      }
      if (entry.getValue().isNull()) {
        values.remove(entry.getKey());
      } else {
        values.put(entry.getKey(), entry.getValue().textValue());
      }
    }
    Mutation mutation;
    if (snapshot.clusterRecord() == null) {
      UUID id = createId(snapshot, RecordKind.CLUSTER_CONFIG, MetadataProtocol.CLUSTER_CONFIG_NAME, null, headers);
      mutation = Mutation.createClusterConfig(snapshot.revision().historyId(), id, values);
    } else {
      mutation = Mutation.updateClusterConfig(match(snapshot, snapshot.clusterRecord(), headers), values);
    }
    validate(() -> _metadata.validateClusterConfig(snapshot, values));
    return commit(snapshot, mutation, headers, null, AccessType.UPDATE, Actions.Cluster.UPDATE_CLUSTER_CONFIG,
        "/cluster/configs", "Cluster configuration published");
  }

  @DELETE
  @Path("/cluster/configs/{configName}")
  @Authenticate(AccessType.DELETE)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.DELETE_CLUSTER_CONFIG)
  public Response deleteClusterConfig(@PathParam("configName") String name, @Context HttpHeaders headers) {
    String path = "/cluster/configs/" + name;
    Snapshot snapshot = writeSnapshot(headers, null, AccessType.DELETE, Actions.Cluster.DELETE_CLUSTER_CONFIG, path);
    ObjectVersion expected = match(snapshot, snapshot.clusterRecord(), headers);
    Map<String, String> values = new HashMap<>(snapshot.clusterConfig());
    values.remove(name);
    validate(() -> _metadata.validateClusterConfig(snapshot, values));
    return commit(snapshot, Mutation.updateClusterConfig(expected, values), headers, null, AccessType.DELETE,
        Actions.Cluster.DELETE_CLUSTER_CONFIG, path, "Cluster configuration key deleted");
  }

  private Snapshot writeSnapshot(HttpHeaders headers, @Nullable String name, AccessType type, String action,
      String path) {
    authorize(headers, name, type, action, path);
    try {
      Snapshot snapshot = _metadata.snapshot();
      authorize(headers, name, type, action, path);
      return snapshot;
    } catch (IOException | IllegalStateException e) {
      throw error(503, "Native metadata is unavailable before mutation submission", false);
    }
  }

  private Response commit(Snapshot snapshot, Mutation mutation, HttpHeaders headers, @Nullable String name,
      AccessType type, String action, String path, String message) {
    authorize(headers, name, type, action, path);
    Result result;
    try {
      result = _metadata.commit(snapshot, mutation);
    } catch (IOException e) {
      throw error(503, "Mutation outcome is unknown. Inspect fresh metadata; absence does not prove no pending commit. "
          + "Any retry must retain the same body, precondition and creation key.", true);
    }
    try {
      authorize(headers, name, type, action, path);
    } catch (WebApplicationException e) {
      throw error(e.getResponse().getStatus(), "Authorization changed while the mutation was in flight", true);
    }
    if (result.status() != Status.OK) {
      int status = switch (result.status()) {
        case CONFLICT, OUT_OF_ORDER, UNACKNOWLEDGED_RESULT -> 409;
        case HISTORY_MISMATCH, FENCED -> 412;
        case NOT_FOUND -> 404;
        case INVALID_REQUEST -> 400;
        case LIMIT_EXCEEDED, RESPONSE_TOO_LARGE -> 413;
        default -> 503;
      };
      throw error(status, result.detail(), false);
    }
    ObjectVersion version = result.versions().get(0);
    return Response.ok(Map.of("status", "OK", "message", message), MediaType.APPLICATION_JSON_TYPE)
        .header(REVISION_HEADER, result.revision().historyId() + ":" + result.revision().index())
        .header(HttpHeaders.ETAG, etag(version)).header(HttpHeaders.CACHE_CONTROL, "no-store").build();
  }

  private static UUID createId(Snapshot snapshot, RecordKind kind, String name, @Nullable CatalogRecord current,
      HttpHeaders headers) {
    if (!"*".equals(headers.getHeaderString(HttpHeaders.IF_NONE_MATCH))) {
      throw error(428, "Creation requires If-None-Match: *", false);
    }
    if (headers.getHeaderString(HttpHeaders.IF_MATCH) != null) {
      throw error(400, "Creation must not supply If-Match", false);
    }
    String key = headers.getHeaderString("Idempotency-Key");
    UUID requestId;
    try {
      requestId = UUID.fromString(key);
      if (!requestId.toString().equals(key)) {
        throw new IllegalArgumentException("Noncanonical UUID");
      }
    } catch (IllegalArgumentException | NullPointerException e) {
      throw error(428, "Creation requires a canonical UUID Idempotency-Key retained across retries", false);
    }
    if (current != null) {
      throw error(409, "Metadata name already exists", false);
    }
    return UUID.nameUUIDFromBytes((snapshot.revision().historyId() + "/" + kind + "/" + name + "/" + requestId)
        .getBytes(StandardCharsets.UTF_8));
  }

  private static ObjectVersion match(Snapshot snapshot, @Nullable CatalogRecord record, HttpHeaders headers) {
    String supplied = headers.getHeaderString(HttpHeaders.IF_MATCH);
    if (supplied == null) {
      throw error(428, "Mutation requires the exact If-Match ETag from a fresh metadata read", false);
    }
    if (headers.getHeaderString(HttpHeaders.IF_NONE_MATCH) != null) {
      throw error(400, "An update must not supply If-None-Match", false);
    }
    if (record == null) {
      throw error(404, "Metadata object not found", false);
    }
    ObjectVersion expected = new ObjectVersion(snapshot.revision().historyId(), record.objectId(), record.version());
    if (!etag(expected).equals(supplied)) {
      throw error(412, "Metadata incarnation or version changed; reload before preparing a new edit", false);
    }
    return expected;
  }

  private static String etag(ObjectVersion version) {
    return "\"" + version.historyId() + ":" + version.objectId() + ":" + version.version() + "\"";
  }

  private static Response tagged(Snapshot snapshot, Object body, @Nullable CatalogRecord record) {
    String tag = record == null ? "\"" + snapshot.revision().historyId() + ":absent\""
        : etag(new ObjectVersion(snapshot.revision().historyId(), record.objectId(), record.version()));
    return Response.fromResponse(response(snapshot, body)).header(HttpHeaders.ETAG, tag).build();
  }

  private static <T> T parse(String body, Class<T> type) {
    try {
      var parsed = JsonUtils.stringToObjectAndUnrecognizedProperties(parseObject(body).toString(), type);
      if (!parsed.getRight().isEmpty()) {
        throw error(400, "Unrecognized configuration properties: " + parsed.getRight().keySet(), false);
      }
      return parsed.getLeft();
    } catch (IOException | IllegalArgumentException e) {
      throw error(400, "Invalid configuration body: " + e.getMessage(), false);
    }
  }

  private static JsonNode parseObject(String body) {
    if (body == null || body.getBytes(StandardCharsets.UTF_8).length > MetadataProtocol.MAX_COMMAND_BYTES) {
      throw error(413, "Configuration body exceeds the supported request size", false);
    }
    try {
      JsonNode node = REQUEST_JSON.readTree(body);
      if (node == null || !node.isObject()) {
        throw error(400, "Configuration body must be a JSON object", false);
      }
      return node;
    } catch (IOException e) {
      throw error(400, "Malformed configuration JSON", false);
    }
  }

  private static void validate(Validation validation) {
    try {
      validation.run();
    } catch (UnsupportedOperationException e) {
      throw error(501, e.getMessage(), false);
    } catch (IOException | IllegalArgumentException | IllegalStateException e) {
      throw error(400, e.getMessage(), false);
    }
  }

  private static WebApplicationException error(int status, String message, boolean unknown) {
    return new WebApplicationException(Response.status(status).type(MediaType.APPLICATION_JSON_TYPE)
        .entity(Map.of("error", message == null ? "Native metadata request rejected" : message,
            "outcomeUnknown", unknown)).header(HttpHeaders.CACHE_CONTROL, "no-store").build());
  }

  @FunctionalInterface
  private interface Validation {
    void run() throws IOException;
  }

  private Snapshot read(HttpHeaders headers, @Nullable String name, String action, String path) {
    authorize(headers, name, action, path);
    Snapshot snapshot;
    try {
      snapshot = _metadata.snapshot();
    } catch (IOException | IllegalStateException e) {
      throw new WebApplicationException("Native metadata is unavailable; no cached result was returned", e,
          Response.Status.SERVICE_UNAVAILABLE);
    }
    // A quorum read may take time; consult current policy again before returning the captured data.
    authorize(headers, name, action, path);
    return snapshot;
  }

  private void authorize(HttpHeaders headers, @Nullable String name, String action, String path) {
    authorize(headers, name, AccessType.READ, action, path);
  }

  private void authorize(HttpHeaders headers, @Nullable String name, AccessType type, String action, String path) {
    String raw = name == null ? null : TableNameBuilder.extractRawTableName(name);
    if (raw != null && !supportedRawName(raw)) {
      throw error(400, "Native metadata requires an unambiguous raw table name", false);
    }
    AccessControl access = _accessControlFactory.create();
    AccessControlUtils.validatePermission(raw, type, headers, path, access);
    if (!access.hasAccess(headers, raw == null ? TargetType.CLUSTER : TargetType.TABLE, raw, action)) {
      throw new WebApplicationException("Permission denied", Response.Status.FORBIDDEN);
    }
  }

  private boolean allowed(String name, HttpHeaders headers, String action, String path) {
    String raw = TableNameBuilder.extractRawTableName(name);
    if (!supportedRawName(raw)) {
      return false;
    }
    AccessControl access = _accessControlFactory.create();
    return access.hasAccess(raw, AccessType.READ, headers, path)
        && access.hasAccess(headers, TargetType.TABLE, raw, action);
  }

  private static boolean inDatabase(String name, HttpHeaders headers) {
    return DatabaseUtils.isPartOfDatabase(name, headers.getHeaderString(CommonConstants.DATABASE));
  }

  // Authorization implementations normalize type suffixes too. Only one suffix may be consumed across all
  // layers; schema identities are already raw and must never carry a suffix themselves.
  private static boolean supportedRawName(@Nullable String name) {
    return name != null && !name.isBlank() && TableNameBuilder.getTableTypeFromTableName(name) == null;
  }

  private static String translateSchemaName(String name, HttpHeaders headers) {
    String translated = translate(name, headers);
    if (!supportedRawName(translated)) {
      throw error(400, "Native schema names must be raw names without a table type suffix", false);
    }
    return translated;
  }

  private static String translateTableName(String name, HttpHeaders headers) {
    String translated = translate(name, headers);
    if (!supportedRawName(TableNameBuilder.extractRawTableName(translated))) {
      throw error(400, "Native table names must contain at most one table type suffix", false);
    }
    return translated;
  }

  private static String translate(String name, HttpHeaders headers) {
    try {
      return DatabaseUtils.translateTableName(name, headers);
    } catch (RuntimeException e) {
      throw new WebApplicationException("Invalid database-qualified name", e, Response.Status.BAD_REQUEST);
    }
  }

  @Nullable
  private static TableType tableType(@Nullable String value) {
    if (value == null) {
      return null;
    }
    if ("OFFLINE".equalsIgnoreCase(value)) {
      return TableType.OFFLINE;
    }
    if ("REALTIME".equalsIgnoreCase(value)) {
      return TableType.REALTIME;
    }
    throw new WebApplicationException("Unknown table type", Response.Status.BAD_REQUEST);
  }

  private static Schema schema(CatalogRecord record) {
    try {
      return JsonUtils.jsonNodeToObject(record.value(), Schema.class);
    } catch (IOException e) {
      throw new WebApplicationException("Invalid committed schema", e, Response.Status.SERVICE_UNAVAILABLE);
    }
  }

  private static TableConfig table(CatalogRecord record) {
    try {
      return JsonUtils.jsonNodeToObject(record.value(), TableConfig.class);
    } catch (IOException e) {
      throw new WebApplicationException("Invalid committed table config", e, Response.Status.SERVICE_UNAVAILABLE);
    }
  }

  private static ObjectNode revision(Revision revision) {
    return JsonUtils.newObjectNode().put("historyId", revision.historyId().toString())
        .put("index", Long.toString(revision.index()));
  }

  private static Response response(Snapshot snapshot, Object body) {
    try {
      return Response.ok(JsonUtils.objectToString(body), MediaType.APPLICATION_JSON_TYPE)
          .header(REVISION_HEADER, snapshot.revision().historyId() + ":" + snapshot.revision().index())
          .header(HttpHeaders.CACHE_CONTROL, "no-store").build();
    } catch (IOException e) {
      throw new WebApplicationException("Cannot encode native metadata response", e,
          Response.Status.SERVICE_UNAVAILABLE);
    }
  }
}
