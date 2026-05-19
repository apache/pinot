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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.exception.SchemaAlreadyExistsException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.LogicalTableConfigUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessControlUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.exception.TableAlreadyExistsException;
import org.apache.pinot.controller.api.resources.ddl.DdlExecutionRequest;
import org.apache.pinot.controller.api.resources.ddl.DdlExecutionResponse;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.tuner.TableConfigTunerUtils;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.DatabaseConflictException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.ddl.compile.CompiledCreateTable;
import org.apache.pinot.sql.ddl.compile.CompiledDdl;
import org.apache.pinot.sql.ddl.compile.CompiledDropTable;
import org.apache.pinot.sql.ddl.compile.CompiledShowCreateTable;
import org.apache.pinot.sql.ddl.compile.DdlCompilationException;
import org.apache.pinot.sql.ddl.compile.DdlCompiler;
import org.apache.pinot.sql.ddl.compile.DdlOperation;
import org.apache.pinot.sql.ddl.reverse.CanonicalDdlEmitter;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/// Controller endpoint for executing Pinot SQL DDL statements. Currently supports CREATE TABLE,
/// DROP TABLE, SHOW TABLES, and SHOW CREATE TABLE.
///
/// Pipeline:
/// 1. [DdlCompiler] parses + compiles the SQL into a [CompiledDdl].
/// 1. Database/table names are translated through [DatabaseUtils#translateTableName]
/// so the `Database` HTTP header is honoured uniformly.
/// 1. Authorization is invoked based on the operation type.
/// 1. Execution either persists via [PinotHelixResourceManager] or, when `dryRun`
/// is true, returns the compiled artifacts without mutating cluster state.
///
/// The endpoint is intentionally a single POST that dispatches by operation. This keeps the
/// client surface area small and matches the canonical `POST /sql/ddl` contract from the
/// design.
@Api(tags = "SQL DDL", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```"),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")}))
@Path("/")
public class PinotDdlRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotDdlRestletResource.class);
  /// Maximum accepted SQL input length, measured in [Java characters][String#length()]
  /// (UTF-16 code units), to prevent unbounded parser memory allocation. Up to ~4× this value
  /// in UTF-8 wire bytes can be accepted by Jackson before the length check rejects; operators
  /// sizing reverse-proxy body limits should plan accordingly.
  private static final int MAX_DDL_SQL_CHARS = 256 * 1024;

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  PinotTaskManager _pinotTaskManager;

  @Inject
  PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;

  @Inject
  AccessControlFactory _accessControlFactory;

  @Inject
  ControllerConf _controllerConf;

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/sql/ddl")
  @ManualAuthorization // permission check is done after parsing the DDL so we know the operation
  @ApiOperation(value = "Execute a Pinot SQL DDL statement",
      notes = "Supports CREATE TABLE, DROP TABLE, SHOW TABLES, and SHOW CREATE TABLE. Returns the "
          + "generated Schema/TableConfig (CREATE), operation outcome (DROP/SHOW TABLES), or "
          + "canonical DDL string (SHOW CREATE TABLE).")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success (DROP, SHOW TABLES, SHOW CREATE TABLE, dry-run, IF NOT EXISTS)"),
      @ApiResponse(code = 201, message = "Table created"),
      @ApiResponse(code = 400, message = "Bad request (parse error, semantic error, oversize input, "
          + "unsupported emitter type, or type-incompatible default value)"),
      @ApiResponse(code = 404, message = "Table or schema not found (DROP without IF EXISTS, SHOW CREATE)"),
      @ApiResponse(code = 409, message = "Conflict (duplicate CREATE without IF NOT EXISTS, logical-table "
          + "reference blocking DROP, or race lost to a concurrent writer)"),
      @ApiResponse(code = 500, message = "Internal server error (Helix/ZK inconsistency or controller defect)")
  })
  public Response executeDdl(
      @ApiParam(value = "DDL request body with 'sql' field", required = true)
          DdlExecutionRequest request,
      @ApiParam(value = "When true, compile and validate but do not persist.")
          @QueryParam("dryRun") @DefaultValue("false") boolean dryRun,
      @Context HttpHeaders httpHeaders, @Context Request httpRequest) {
    if (request == null || StringUtils.isBlank(request.getSql())) {
      throw badRequest("Request body must include a non-empty 'sql' field.");
    }
    // Guard against arbitrarily large inputs that would force the Calcite parser to allocate
    // excessive memory building the AST in-memory.
    if (request.getSql().length() > MAX_DDL_SQL_CHARS) {
      throw badRequest("DDL statement exceeds maximum length of " + MAX_DDL_SQL_CHARS + " characters.");
    }

    CompiledDdl compiled;
    try {
      compiled = DdlCompiler.compile(request.getSql());
    } catch (DdlCompilationException e) {
      throw badRequest(e.getMessage());
    } catch (RuntimeException e) {
      LOGGER.warn("Unexpected DDL compilation failure", e);
      throw badRequest("DDL compilation failed: " + e.getMessage());
    }

    DdlOperation op = compiled.getOperation();
    String requestedDatabase = compiled.getDatabaseName();
    String effectiveDatabase = resolveDatabase(requestedDatabase, httpHeaders);

    // Authorization is performed inside each execute*() method, AFTER the target table name has
    // been DB-translated. Authorizing on the pre-translation name would let a header-supplied
    // database substitute past the auth check.
    switch (op) {
      case CREATE_TABLE:
        return executeCreate((CompiledCreateTable) compiled, effectiveDatabase, dryRun,
            httpHeaders, httpRequest);
      case DROP_TABLE:
        return Response.ok(
            executeDrop((CompiledDropTable) compiled, effectiveDatabase, dryRun,
                httpHeaders, httpRequest)).build();
      case SHOW_TABLES:
        return Response.ok(executeShow(effectiveDatabase, httpHeaders, httpRequest)).build();
      case SHOW_CREATE_TABLE:
        return Response.ok(
            executeShowCreate((CompiledShowCreateTable) compiled, effectiveDatabase,
                httpHeaders, httpRequest)).build();
      default:
        throw new ControllerApplicationException(LOGGER, "Unhandled DDL operation: " + op,
            Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  // -------------------------------------------------------------------------------------------
  // CREATE
  // -------------------------------------------------------------------------------------------

  private Response executeCreate(CompiledCreateTable create, String database,
      boolean dryRun, HttpHeaders headers, Request httpRequest) {
    // The compiled TableConfig.tableName carries the SQL `db.tbl` qualifier when one was given;
    // translateTableName then reconciles it against the Database header (and rejects conflicts).
    String tableNameWithType = translateTableNameForDdl(
        TableNameBuilder.forType(create.getTableConfig().getTableType())
            .tableNameWithType(create.getTableConfig().getTableName()), headers);
    create.getTableConfig().setTableName(tableNameWithType);

    // The schema name has the same DB-scoping requirement: PinotHelixResourceManager.addTable
    // looks up the schema using the raw table name extracted from tableNameWithType, so the two
    // must agree on the database prefix.
    String compiledSchemaName = create.getSchema().getSchemaName();
    String dottedSchemaName = create.getDatabaseName() == null
        ? compiledSchemaName
        : create.getDatabaseName() + "." + compiledSchemaName;
    String schemaName = translateTableNameForDdl(dottedSchemaName, headers);
    create.getSchema().setSchemaName(schemaName);

    // Authorize against the FULLY-QUALIFIED, post-translation table name. Checking the bare
    // compiled name would let a Database header substitute the resource we're checking against,
    // enabling cross-DB privilege escalation.
    ResourceUtils.checkPermissionAndAccess(tableNameWithType, httpRequest, headers,
        AccessType.CREATE, Actions.Table.CREATE_TABLE, _accessControlFactory, LOGGER);

    DdlExecutionResponse response = new DdlExecutionResponse()
        .setOperation(DdlOperation.CREATE_TABLE)
        .setDryRun(dryRun)
        .setDatabaseName(database)
        .setTableName(tableNameWithType)
        .setTableType(create.getTableConfig().getTableType().toString())
        .setIfNotExists(create.isIfNotExists())
        .setSchema(toJson(create.getSchema()))
        .setTableConfig(toJson(create.getTableConfig()))
        .setWarnings(create.getWarnings());

    // Existence check runs first so CREATE TABLE IF NOT EXISTS is a successful no-op when the
    // table already exists, matching the SQL-standard semantics (PostgreSQL, MySQL, SQLite,
    // Snowflake, Trino, BigQuery): a deployment script that re-runs the same DDL against an
    // existing table must succeed even if the column list or properties have drifted from the
    // current stored config. Without this ordering, idempotent provisioning scripts break the
    // moment a config-validator rule changes between Pinot versions.
    if (_pinotHelixResourceManager.hasTable(tableNameWithType)) {
      if (create.isIfNotExists()) {
        response.setMessage("Table " + tableNameWithType + " already exists; CREATE IF NOT EXISTS is a no-op.");
        return Response.ok(response).build();
      }
      throw new ControllerApplicationException(LOGGER,
          "Table " + tableNameWithType + " already exists.", Response.Status.CONFLICT);
    }

    // Look up the stored schema first. When the second physical variant of a hybrid pair is
    // created, the stored schema is the canonical source of metadata (primary keys, tags,
    // null-handling) that the DDL column list does not itself express. Validating against the
    // stored schema ensures upsert/dedup PK checks see the real PK list rather than a
    // synthesized empty one from the DDL-compiled schema.
    Schema storedSchema = _pinotHelixResourceManager.getSchema(schemaName);
    boolean schemaPreexisted = storedSchema != null;

    if (schemaPreexisted) {
      // Compare only the column-shape attributes that the DDL column list actually controls, plus
      // schema metadata that was explicitly supplied in this DDL statement (currently PRIMARY KEY).
      // Comparing full JSON would include schema-level metadata (primary keys, null-handling,
      // tags, description) that the DDL does not express when a column list is given for the
      // second hybrid variant — e.g. a DDL without PRIMARY KEY would spuriously conflict with
      // a stored schema whose primary keys were set by the first variant.
      String mismatch = describeColumnShapeMismatch(storedSchema, create.getSchema());
      if (mismatch != null) {
        throw new ControllerApplicationException(LOGGER,
            "Schema '" + schemaName + "' already exists and does not match the column list in the DDL: "
                + mismatch
                + ". Either omit the column list to reuse the existing schema, or drop and recreate the "
                + "table pair if the schema has genuinely changed.",
            Response.Status.CONFLICT);
      }
    }

    // Run the full schema/table validation stack that the existing /tables and /tableConfigs APIs
    // apply before any ZK write. This catches invalid combinations (upsert without primary keys,
    // field configs referencing non-existent columns, task configs with bad column references,
    // etc.) that the compiler alone cannot detect. Runs for both dry-run and live create.
    // When a stored schema exists (hybrid second-variant case), validate against the stored
    // schema so the validators see the canonical PK list / null-handling / tags rather than the
    // DDL's column-list-only projection — otherwise upsert/dedup tables would falsely fail PK
    // validation when the DDL omits PRIMARY KEY in the second variant.
    Schema schemaForValidation = schemaPreexisted ? storedSchema : create.getSchema();
    response.setSchema(toJson(schemaForValidation));
    // Apply tuner configs before validation, mirroring POST /tables. Tuners may rewrite the
    // table config (e.g. fill in defaulted index configs) and the validators must run against
    // the post-tuner shape, otherwise a tuner-introduced setting bypasses validation.
    TableConfigTunerUtils.applyTunerConfigs(_pinotHelixResourceManager, create.getTableConfig(),
        schemaForValidation, Collections.emptyMap());
    // Refresh the response's tableConfig snapshot now that tuners have run; otherwise the
    // response advertises a pre-tuner config while ZK persists the post-tuner shape (and dry-run
    // would mis-predict what a real CREATE would write).
    response.setTableConfig(toJson(create.getTableConfig()));
    validateTableConfig(schemaForValidation, create.getTableConfig());
    PinotTableRestletResource.tableTasksValidation(create.getTableConfig(), _pinotHelixTaskResourceManager);

    if (dryRun) {
      response.setMessage("Dry run: validated CREATE TABLE without persisting.");
      return Response.ok(response).build();
    }

    try {
      // override=false: an existing schema with the same name is a precondition violation, not
      // something we silently overwrite. The other-typed table variant or another caller's
      // schema would otherwise be clobbered out from under them.
      if (!schemaPreexisted) {
        _pinotHelixResourceManager.addSchema(create.getSchema(), false, false);
      }
      _pinotHelixResourceManager.addTable(create.getTableConfig());
      response.setMessage("Successfully created table " + tableNameWithType);
      LOGGER.info("DDL created table {}", tableNameWithType);
      return Response.status(Response.Status.CREATED).entity(response).build();
    } catch (TableAlreadyExistsException e) {
      // Race: another caller added the table between our hasTable check and addTable. Do NOT
      // roll back the schema here — the winner of the race may be using the same shared schema
      // (legitimate hybrid-pair pattern), and a hasTable re-check followed by deleteSchema is
      // racy in the same way the generic-failure branch below is. Stale schemas can be removed
      // via DELETE /schemas/{name} if needed.
      //
      // Second-order race: between TableAlreadyExistsException and the IF-NOT-EXISTS hasTable
      // re-check, a third caller may have DROPped the table. In that narrow window the
      // re-check returns false and we fall through to throw 409 even though IF NOT EXISTS
      // would normally be satisfied. The user can retry; Pinot's `addTable` does not currently
      // expose a version-checked create-or-no-op primitive that would close this window. If
      // one is added, switch this branch to use it.
      if (create.isIfNotExists() && _pinotHelixResourceManager.hasTable(tableNameWithType)) {
        response.setMessage("Table " + tableNameWithType
            + " already exists; CREATE IF NOT EXISTS is a no-op.");
        return Response.ok(response).build();
      }
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
    } catch (SchemaAlreadyExistsException e) {
      // The override=false addSchema call lost a race with another schema writer. If the table was
      // concurrently created and the statement is idempotent, honour IF NOT EXISTS. Otherwise,
      // verify the raced schema is compatible and continue with addTable(), matching the normal
      // pre-existing-schema hybrid path.
      return retryCreateAfterSchemaRace(create, response, schemaName, tableNameWithType, e);
    } catch (Exception e) {
      // Intentionally do NOT roll back the schema on a generic addTable() failure. The two
      // hasOfflineTable/hasRealtimeTable reads required to decide "is this schema orphaned?"
      // are non-atomic, and a concurrent sibling CREATE that succeeds between the two reads
      // would have its schema deleted out from under it — orphaning a live table. Pinot's
      // existing /tables endpoint also leaves the schema in place when table creation fails,
      // so the contract is consistent: schemas can outlive tables, and stale schemas can be
      // removed via DELETE /schemas/{name}.
      //
      // Don't append a "remove via DELETE /schemas" hint here: arbitrary RuntimeException /
      // IOException from addTable can be a transient ZK or Helix blip, and pointing the
      // operator at a destructive schema-deletion command in response to a transient failure
      // would encourage premature cleanup. The error message is logged with the original
      // cause so an operator can investigate via log/metrics.
      // ControllerApplicationException(LOGGER, ...) logs the exception, so don't double-log here.
      throw new ControllerApplicationException(LOGGER,
          "Failed to create table " + tableNameWithType + ": " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  private Response retryCreateAfterSchemaRace(CompiledCreateTable create, DdlExecutionResponse response,
      String schemaName, String tableNameWithType, SchemaAlreadyExistsException schemaFailure) {
    if (create.isIfNotExists() && _pinotHelixResourceManager.hasTable(tableNameWithType)) {
      response.setMessage("Table " + tableNameWithType + " already exists; CREATE IF NOT EXISTS is a no-op.");
      return Response.ok(response).build();
    }
    Schema racedSchema = _pinotHelixResourceManager.getSchema(schemaName);
    if (racedSchema == null) {
      throw new ControllerApplicationException(LOGGER, schemaFailure.getMessage(), Response.Status.CONFLICT,
          schemaFailure);
    }
    String mismatch = describeColumnShapeMismatch(racedSchema, create.getSchema());
    if (mismatch != null) {
      throw new ControllerApplicationException(LOGGER,
          "Schema '" + schemaName + "' was concurrently created and does not match the column list in the DDL: "
              + mismatch,
          Response.Status.CONFLICT, schemaFailure);
    }
    response.setSchema(toJson(racedSchema));
    validateTableConfig(racedSchema, create.getTableConfig());
    PinotTableRestletResource.tableTasksValidation(create.getTableConfig(), _pinotHelixTaskResourceManager);
    try {
      _pinotHelixResourceManager.addTable(create.getTableConfig());
      response.setMessage("Successfully created table " + tableNameWithType);
      LOGGER.info("DDL created table {} after concurrent schema create", tableNameWithType);
      return Response.status(Response.Status.CREATED).entity(response).build();
    } catch (TableAlreadyExistsException e) {
      if (create.isIfNotExists() && _pinotHelixResourceManager.hasTable(tableNameWithType)) {
        response.setMessage("Table " + tableNameWithType
            + " already exists; CREATE IF NOT EXISTS is a no-op.");
        return Response.ok(response).build();
      }
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to create table " + tableNameWithType + " after concurrent schema create: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  /// Compares two schemas by the column-shape attributes that a DDL column list actually controls
  /// (column name, data type, field type, single/multi-value, NOT NULL, default null value, and —
  /// for DATETIME columns — format and granularity) and schema metadata explicitly supplied by
  /// DDL (`PRIMARY KEY`) and returns a human-readable description of the first mismatch, or `null`
  /// if the shapes are equivalent. Schema-level metadata that a DDL column list does not express
  /// (`tags`, `enableColumnBasedNullHandling`, `description`, and omitted primary keys) is
  /// intentionally ignored so the second hybrid variant can be created via DDL without restating
  /// metadata set by the first variant.
  // Package-private for unit testing.
  @Nullable
  static String describeColumnShapeMismatch(Schema stored, Schema compiled) {
    Set<String> storedColumns = stored.getColumnNames();
    Set<String> compiledColumns = compiled.getColumnNames();
    if (!storedColumns.equals(compiledColumns)) {
      Set<String> missing = new HashSet<>(storedColumns);
      missing.removeAll(compiledColumns);
      Set<String> extra = new HashSet<>(compiledColumns);
      extra.removeAll(storedColumns);
      StringBuilder sb = new StringBuilder("column sets differ");
      if (!missing.isEmpty()) {
        sb.append("; missing from DDL: ").append(missing);
      }
      if (!extra.isEmpty()) {
        sb.append("; extra in DDL: ").append(extra);
      }
      return sb.toString();
    }
    for (String columnName : storedColumns) {
      FieldSpec storedSpec = stored.getFieldSpecFor(columnName);
      FieldSpec compiledSpec = compiled.getFieldSpecFor(columnName);
      if (storedSpec.getDataType() != compiledSpec.getDataType()) {
        return "column '" + columnName + "' data type differs (stored=" + storedSpec.getDataType()
            + ", DDL=" + compiledSpec.getDataType() + ")";
      }
      if (storedSpec.getFieldType() != compiledSpec.getFieldType()) {
        return "column '" + columnName + "' field type differs (stored=" + storedSpec.getFieldType()
            + ", DDL=" + compiledSpec.getFieldType() + ")";
      }
      if (storedSpec.isSingleValueField() != compiledSpec.isSingleValueField()) {
        return "column '" + columnName + "' single-valued flag differs";
      }
      if (storedSpec.isNotNull() != compiledSpec.isNotNull()) {
        return "column '" + columnName + "' NOT NULL flag differs (stored=" + storedSpec.isNotNull()
            + ", DDL=" + compiledSpec.isNotNull() + ")";
      }
      // Compare typed default null value. For BYTES, DataType.equals delegates to Arrays.equals
      // (byte[] reference comparison would falsely flag every BYTES default as a mismatch).
      // For BIG_DECIMAL, prefer compareTo over equals — BigDecimal.equals is scale-sensitive
      // (new BigDecimal("1").equals(new BigDecimal("1.0")) is false), and the same numeric
      // default arriving via different literal forms (DDL "1" vs JSON-API "1.0") would
      // otherwise produce a phantom mismatch. For other types, DataType.equals delegates to
      // the boxed value's equals.
      Object storedDefault = storedSpec.getDefaultNullValue();
      Object compiledDefault = compiledSpec.getDefaultNullValue();
      boolean defaultsEqual = defaultValuesEqual(storedSpec.getDataType(), storedDefault, compiledDefault);
      if (!defaultsEqual) {
        return "column '" + columnName + "' default null value differs (stored="
            + storedSpec.getDefaultNullValueString()
            + ", DDL=" + compiledSpec.getDefaultNullValueString() + ")";
      }
      if (storedSpec instanceof DateTimeFieldSpec && compiledSpec instanceof DateTimeFieldSpec) {
        DateTimeFieldSpec storedDt = (DateTimeFieldSpec) storedSpec;
        DateTimeFieldSpec compiledDt = (DateTimeFieldSpec) compiledSpec;
        if (!Objects.equals(storedDt.getFormat(), compiledDt.getFormat())) {
          return "column '" + columnName + "' DATETIME format differs (stored="
              + storedDt.getFormat() + ", DDL=" + compiledDt.getFormat() + ")";
        }
        if (!Objects.equals(storedDt.getGranularity(), compiledDt.getGranularity())) {
          return "column '" + columnName + "' DATETIME granularity differs (stored="
              + storedDt.getGranularity() + ", DDL=" + compiledDt.getGranularity() + ")";
        }
      }
    }
    List<String> compiledPrimaryKeys = compiled.getPrimaryKeyColumns();
    if (compiledPrimaryKeys != null && !compiledPrimaryKeys.isEmpty()
        && !Objects.equals(stored.getPrimaryKeyColumns(), compiledPrimaryKeys)) {
      return "PRIMARY KEY columns differ (stored=" + stored.getPrimaryKeyColumns()
          + ", DDL=" + compiledPrimaryKeys + ")";
    }
    return null;
  }

  /// Compares two default-null-values for content equality, accounting for type-specific
  /// gotchas: BYTES requires Arrays.equals (each getter allocates a fresh byte[]); BIG_DECIMAL
  /// requires compareTo so different scales of the same numeric value compare equal.
  private static boolean defaultValuesEqual(FieldSpec.DataType dataType,
      @Nullable Object storedDefault, @Nullable Object compiledDefault) {
    if (storedDefault == null || compiledDefault == null) {
      return storedDefault == null && compiledDefault == null;
    }
    if (dataType == FieldSpec.DataType.BIG_DECIMAL && storedDefault instanceof BigDecimal
        && compiledDefault instanceof BigDecimal) {
      return ((BigDecimal) storedDefault).compareTo((BigDecimal) compiledDefault) == 0;
    }
    return dataType.equals(storedDefault, compiledDefault);
  }

  /// Runs the same schema/table validation stack that `POST /tables` and
  /// `/tableConfigs` apply before any ZK write, so DDL-created configs are subject to the
  /// same rules as JSON-API-created configs. Delegates to [TableConfigValidationUtils] so
  /// the two endpoints share a single validation pipeline (min replicas, storage quota, hybrid
  /// pair compatibility, instance assignment, tenant tags, task configs, registry-level checks).
  private void validateTableConfig(Schema schema, TableConfig tableConfig) {
    try {
      TableConfigValidationUtils.validateTableConfig(tableConfig, schema, null,
          _pinotHelixResourceManager, _controllerConf, _pinotTaskManager);
    } catch (ControllerApplicationException e) {
      throw e;
    } catch (IllegalArgumentException | IllegalStateException e) {
      // The Pinot validators consistently raise these for user-facing config errors
      // (upsert without primary keys, field configs referencing non-existent columns,
      // bad task configs, etc.). Surface as 400 — the caller can fix their DDL.
      throw new ControllerApplicationException(LOGGER,
          "Table config validation failed: " + e.getMessage(), Response.Status.BAD_REQUEST, e);
    } catch (Exception e) {
      // Any other exception is unexpected — most likely a controller-side defect (e.g. NPE in
      // a registry validator, ZK connectivity blip during validateTableTenantConfig). 400 would
      // mislead the caller into "fix your DDL"; surface as 500 so monitoring picks it up.
      throw new ControllerApplicationException(LOGGER,
          "Internal error during table config validation: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  // -------------------------------------------------------------------------------------------
  // DROP
  // -------------------------------------------------------------------------------------------

  private DdlExecutionResponse executeDrop(CompiledDropTable drop, String database, boolean dryRun,
      HttpHeaders headers, Request httpRequest) {
    // The SQL `db.tbl` qualifier is preserved through to translateTableName so the resolved
    // raw name carries the right database scope. Without this, `DROP TABLE analyticsDb.events`
    // with no Database header would silently target `events` in the default DB.
    String dottedRaw = drop.getDatabaseName() == null
        ? drop.getRawTableName()
        : drop.getDatabaseName() + "." + drop.getRawTableName();
    String fullyQualifiedRaw = translateTableNameForDdl(dottedRaw, headers);

    // Compute the candidate typed names BEFORE existence filtering so we can authorize against
    // the user's intent (not just whatever happens to exist now). This prevents an unauthorized
    // caller from probing existence via 200/404 vs 403.
    //
    // NB: the no-TYPE form (`DROP TABLE foo` without `TYPE OFFLINE | REALTIME`) authorizes
    // BOTH variants up-front under access-control plugins that grant per-type permissions, so
    // a caller with permission only on OFFLINE will receive 403 even if the REALTIME variant
    // does not exist. This mirrors the SHOW CREATE TABLE no-TYPE policy and is intentional:
    // "drop both variants atomically" requires authorization on both. Callers with partial
    // permission must use the explicit `TYPE` clause to target a single variant.
    List<String> candidates = new ArrayList<>(2);
    if (drop.getTableType() == null || drop.getTableType() == TableType.OFFLINE) {
      candidates.add(TableNameBuilder.OFFLINE.tableNameWithType(fullyQualifiedRaw));
    }
    if (drop.getTableType() == null || drop.getTableType() == TableType.REALTIME) {
      candidates.add(TableNameBuilder.REALTIME.tableNameWithType(fullyQualifiedRaw));
    }
    for (String candidate : candidates) {
      ResourceUtils.checkPermissionAndAccess(candidate, httpRequest, headers,
          AccessType.DELETE, Actions.Table.DELETE_TABLE, _accessControlFactory, LOGGER);
    }

    List<String> targets = new ArrayList<>(2);
    for (String candidate : candidates) {
      if (_pinotHelixResourceManager.hasTable(candidate)
          || _pinotHelixResourceManager.getTableConfig(candidate) != null) {
        targets.add(candidate);
      }
    }

    if (targets.isEmpty()) {
      if (drop.isIfExists()) {
        return new DdlExecutionResponse()
            .setOperation(DdlOperation.DROP_TABLE)
            .setDryRun(dryRun)
            .setDatabaseName(database)
            .setTableName(fullyQualifiedRaw)
            .setIfExists(true)
            .setDeletedTables(targets)
            .setMessage("No matching table to drop; IF EXISTS satisfied.");
      }
      throw new ControllerApplicationException(LOGGER,
          "Table not found: " + fullyQualifiedRaw, Response.Status.NOT_FOUND);
    }

    DdlExecutionResponse response = new DdlExecutionResponse()
        .setOperation(DdlOperation.DROP_TABLE)
        .setDryRun(dryRun)
        .setDatabaseName(database)
        .setTableName(fullyQualifiedRaw)
        .setIfExists(drop.isIfExists())
        .setTableType(drop.getTableType() == null ? null : drop.getTableType().toString())
        .setDeletedTables(targets);

    // Reject drop if any target is referenced by a logical table, matching the safeguard in
    // the existing /tables and /tableConfigs DELETE endpoints.
    assertNoLogicalTableReferences(targets);
    assertNoActiveTasksBeforeDrop(targets);

    if (dryRun) {
      response.setMessage("Dry run: " + targets.size() + " table(s) would be dropped.");
      return response;
    }

    List<String> dropped = new ArrayList<>();
    for (String target : targets) {
      boolean tasksCleaned = false;
      try {
        cleanupTableTasksBeforeDrop(target);
        tasksCleaned = true;
        // deleteTable(rawName, type, retention) takes the raw name and re-derives the typed
        // name internally via TableNameBuilder.forType(type).tableNameWithType(rawName); see
        // PinotHelixResourceManager.deleteTable. Pass `fullyQualifiedRaw` (DB-qualified raw
        // name) and the type extracted from `target` so the call is unambiguous.
        TableType type = TableNameBuilder.getTableTypeFromTableName(target);
        _pinotHelixResourceManager.deleteTable(fullyQualifiedRaw, type, null);
        dropped.add(target);
        LOGGER.info("DDL dropped table {}", target);
      } catch (ControllerApplicationException e) {
        LOGGER.warn("DROP TABLE on {} failed: {}", target, e.getMessage());
        throw dropFailed(target, dropped, tasksCleaned, e.getResponse().getStatus(), e);
      } catch (Exception e) {
        LOGGER.warn("DROP TABLE on {} failed unexpectedly: {}", target, e.toString());
        throw dropFailed(target, dropped, tasksCleaned,
            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e);
      }
    }
    // Intentionally do NOT delete the shared schema when the last physical variant is removed.
    // This matches the existing `/tables/{name}` DELETE contract, which also leaves the schema
    // intact. Two doors into the same state machine must have the same side effects; a caller
    // who wants to remove the schema can issue an explicit DELETE /schemas/{name} afterwards.
    response.setDeletedTables(dropped);
    response.setMessage("Dropped " + dropped.size() + " table metadata target(s).");
    LOGGER.info("DDL dropped tables {}", dropped);
    return response;
  }

  /// Rejects the DROP when any target physical table is currently referenced by a logical table.
  /// Matches the same safeguard enforced by the existing `DELETE /tables/{name}` endpoint.
  ///
  /// Cost: O(L) ZK reads to fetch every logical-table config, plus O(L × T) reference checks
  /// where L is the cluster's logical-table count and T is the DROP target count (1 or 2). For
  /// clusters with hundreds of logical tables, a tight loop of DDL DROPs can produce a
  /// noticeable controller-side ZK read load; callers performing bulk drops may prefer the
  /// existing JSON `DELETE` endpoint, which has the same cost per call but is friendlier to
  /// concurrent batch tooling.
  private void assertNoLogicalTableReferences(List<String> targets) {
    List<LogicalTableConfig> allLogicalTableConfigs =
        ZKMetadataProvider.getAllLogicalTableConfigs(_pinotHelixResourceManager.getPropertyStore());
    for (String target : targets) {
      for (LogicalTableConfig logicalTableConfig : allLogicalTableConfigs) {
        if (LogicalTableConfigUtils.checkPhysicalTableRefExists(logicalTableConfig, target)) {
          throw new ControllerApplicationException(LOGGER,
              "Cannot drop table '" + target + "': it is referenced by logical table '"
                  + logicalTableConfig.getTableName() + "'.",
              Response.Status.CONFLICT);
        }
      }
    }
  }

  private void assertNoActiveTasksBeforeDrop(List<String> targets) {
    List<String> pendingTasks = new ArrayList<>();
    for (String target : targets) {
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(target);
      if (tableConfig == null || tableConfig.getTaskConfig() == null) {
        continue;
      }
      for (String taskType : tableConfig.getTaskConfig().getTaskTypeConfigsMap().keySet()) {
        Map<String, TaskState> taskStates;
        try {
          taskStates = _pinotHelixTaskResourceManager.getTaskStatesByTable(taskType, target);
        } catch (IllegalArgumentException e) {
          LOGGER.info(e.getMessage());
          continue;
        }
        for (Map.Entry<String, TaskState> taskState : taskStates.entrySet()) {
          String taskName = taskState.getKey();
          if (TaskState.IN_PROGRESS.equals(taskState.getValue())
              && _pinotHelixTaskResourceManager.getTaskCount(taskName).getRunning() > 0) {
            pendingTasks.add(taskName);
          }
        }
      }
    }
    if (!pendingTasks.isEmpty()) {
      throw new ControllerApplicationException(LOGGER,
          "DROP TABLE blocked because active running tasks exist: " + pendingTasks
              + ". No table metadata was changed by this DDL preflight; retry once the tasks finish.",
          Response.Status.BAD_REQUEST);
    }
  }

  private void cleanupTableTasksBeforeDrop(String tableWithType)
      throws Exception {
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableWithType);
    if (tableConfig == null || tableConfig.getTaskConfig() == null) {
      return;
    }
    Map<String, Map<String, String>> taskTypeConfigsMap = tableConfig.getTaskConfig().getTaskTypeConfigsMap();
    Set<String> taskTypes = new HashSet<>(taskTypeConfigsMap.keySet());
    TaskCleanupScan scan = scanTasksBeforeDrop(tableWithType, taskTypes);
    if (!scan._pendingTasks.isEmpty()) {
      throw activeTasksBlocked(scan._pendingTasks, "No table metadata was changed by this DDL preflight");
    }

    Map<String, String> removedSchedules = new HashMap<>();
    for (String taskType : taskTypes) {
      String schedule = taskTypeConfigsMap.get(taskType).remove(PinotTaskManager.SCHEDULE_KEY);
      if (schedule != null) {
        removedSchedules.put(taskType, schedule);
      }
    }
    boolean schedulesPersisted = persistTaskScheduleRemoval(tableConfig, removedSchedules);
    try {
      scan = scanTasksBeforeDrop(tableWithType, taskTypes);
      if (!scan._pendingTasks.isEmpty()) {
        if (schedulesPersisted) {
          restoreTaskSchedules(tableWithType, tableConfig, removedSchedules);
        }
        throw activeTasksBlocked(scan._pendingTasks,
            "Task schedules were restored because active tasks appeared during DROP TABLE preflight");
      }
      for (String taskName : scan._deletableTasks) {
        _pinotHelixTaskResourceManager.deleteTask(taskName, true);
      }
    } catch (ControllerApplicationException e) {
      throw e;
    } catch (Exception e) {
      if (schedulesPersisted) {
        try {
          restoreTaskSchedules(tableWithType, tableConfig, removedSchedules);
        } catch (ControllerApplicationException restoreFailure) {
          e.addSuppressed(restoreFailure);
        }
      }
      throw e;
    }
  }

  private boolean persistTaskScheduleRemoval(TableConfig tableConfig, Map<String, String> removedSchedules) {
    if (removedSchedules.isEmpty()) {
      return false;
    }
    try {
      _pinotHelixResourceManager.updateTableConfig(tableConfig);
      return true;
    } catch (Exception e) {
      restoreTaskSchedulesInMemory(tableConfig, removedSchedules);
      LOGGER.warn("Unable to remove task schedules before DROP TABLE on {}. "
              + "Proceeding with table deletion because no active tasks were found. Reason: {}",
          tableConfig.getTableName(), e.getMessage());
      return false;
    }
  }

  private void restoreTaskSchedules(String tableWithType, TableConfig tableConfig,
      Map<String, String> removedSchedules) {
    restoreTaskSchedulesInMemory(tableConfig, removedSchedules);
    try {
      _pinotHelixResourceManager.updateTableConfig(tableConfig);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "DROP TABLE detected active tasks after clearing task schedules for " + tableWithType
              + " and failed to restore those schedules: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  private static void restoreTaskSchedulesInMemory(TableConfig tableConfig,
      Map<String, String> removedSchedules) {
    Map<String, Map<String, String>> taskTypeConfigsMap = tableConfig.getTaskConfig().getTaskTypeConfigsMap();
    for (Map.Entry<String, String> removedSchedule : removedSchedules.entrySet()) {
      taskTypeConfigsMap.get(removedSchedule.getKey())
          .put(PinotTaskManager.SCHEDULE_KEY, removedSchedule.getValue());
    }
  }

  private TaskCleanupScan scanTasksBeforeDrop(String tableWithType, Set<String> taskTypes) {
    TaskCleanupScan scan = new TaskCleanupScan();
    for (String taskType : taskTypes) {
      Map<String, TaskState> taskStates;
      try {
        taskStates = _pinotHelixTaskResourceManager.getTaskStatesByTable(taskType, tableWithType);
      } catch (IllegalArgumentException e) {
        LOGGER.info(e.getMessage());
        continue;
      }
      for (Map.Entry<String, TaskState> taskState : taskStates.entrySet()) {
        String taskName = taskState.getKey();
        if (TaskState.IN_PROGRESS.equals(taskState.getValue())
            && _pinotHelixTaskResourceManager.getTaskCount(taskName).getRunning() > 0) {
          scan._pendingTasks.add(taskName);
        } else {
          scan._deletableTasks.add(taskName);
        }
      }
    }
    return scan;
  }

  private ControllerApplicationException activeTasksBlocked(List<String> pendingTasks, String mutationContext) {
    return new ControllerApplicationException(LOGGER,
        "DROP TABLE blocked because active running tasks exist: " + pendingTasks
            + ". " + mutationContext + "; retry once the tasks finish.",
        Response.Status.BAD_REQUEST);
  }

  private static final class TaskCleanupScan {
    private final List<String> _pendingTasks = new ArrayList<>();
    private final List<String> _deletableTasks = new ArrayList<>();
  }

  private ControllerApplicationException dropFailed(String target, List<String> dropped,
      boolean taskSchedulesCleared, int statusCode, Exception cause) {
    String causeDesc = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
    String partialPrefix = dropped.isEmpty() ? "" : "Partial DROP TABLE: dropped " + dropped + ", ";
    String reCreateHint = dropped.isEmpty() ? ""
        : ". The successfully-dropped variant must be re-created if the original DROP was unintended.";
    String taskScheduleHint = taskSchedulesCleared
        ? ". Task schedules were already cleared for " + target
            + "; if that table remains in service the operator must restore the schedule entries"
            + " in its TableConfig taskTypeConfigsMap before scheduled tasks resume."
        : "";
    return new ControllerApplicationException(LOGGER,
        partialPrefix + "failed to drop " + target + ": " + causeDesc + reCreateHint + taskScheduleHint,
        statusCode, cause);
  }

  // -------------------------------------------------------------------------------------------
  // SHOW
  // -------------------------------------------------------------------------------------------

  // -------------------------------------------------------------------------------------------
  // SHOW CREATE TABLE
  // -------------------------------------------------------------------------------------------

  private DdlExecutionResponse executeShowCreate(CompiledShowCreateTable show, String database,
      HttpHeaders headers, Request httpRequest) {
    String dottedRaw = show.getDatabaseName() == null
        ? show.getRawTableName()
        : show.getDatabaseName() + "." + show.getRawTableName();
    String fullyQualifiedRaw = translateTableNameForDdl(dottedRaw, headers);

    // Resolve which typed variant to render. Explicit TYPE clause wins; otherwise check both.
    // Authorize BEFORE existence checks so an unauthorized caller cannot distinguish
    // "exists but forbidden" (403) from "not found" (404) — mirroring the DROP path.
    TableType resolvedType = show.getTableType();
    String tableNameWithType;
    if (resolvedType != null) {
      tableNameWithType = TableNameBuilder.forType(resolvedType).tableNameWithType(fullyQualifiedRaw);
      ResourceUtils.checkPermissionAndAccess(tableNameWithType, httpRequest, headers,
          AccessType.READ, Actions.Table.GET_TABLE_CONFIG, _accessControlFactory, LOGGER);
      if (!_pinotHelixResourceManager.hasTable(tableNameWithType)) {
        throw new ControllerApplicationException(LOGGER,
            "Table not found: " + tableNameWithType, Response.Status.NOT_FOUND);
      }
    } else {
      // No-TYPE form: authorize BOTH candidate variants before any existence-revealing branch.
      // This is required to prevent fingerprinting under access-control plugins that grant
      // per-type permissions (the SPI permits this and enterprise plugins use it). With both
      // checks up-front:
      //   - A caller without permission on either variant always gets 403, regardless of
      //     existence.
      //   - A caller with permission on only one variant must use SHOW CREATE TABLE ... TYPE
      //     {OFFLINE|REALTIME} to read that single variant — the bare form is intentionally
      //     more restrictive because answering it requires reading both candidates' state.
      // This mirrors the DROP TABLE pattern (lines 469-474) for symmetry between read and
      // delete operations.
      String off = TableNameBuilder.OFFLINE.tableNameWithType(fullyQualifiedRaw);
      String rt = TableNameBuilder.REALTIME.tableNameWithType(fullyQualifiedRaw);
      ResourceUtils.checkPermissionAndAccess(off, httpRequest, headers,
          AccessType.READ, Actions.Table.GET_TABLE_CONFIG, _accessControlFactory, LOGGER);
      ResourceUtils.checkPermissionAndAccess(rt, httpRequest, headers,
          AccessType.READ, Actions.Table.GET_TABLE_CONFIG, _accessControlFactory, LOGGER);
      boolean offExists = _pinotHelixResourceManager.hasTable(off);
      boolean rtExists = _pinotHelixResourceManager.hasTable(rt);
      if (offExists && rtExists) {
        throw new ControllerApplicationException(LOGGER,
            "Table '" + fullyQualifiedRaw + "' has both OFFLINE and REALTIME variants. "
                + "Use 'SHOW CREATE TABLE ... TYPE OFFLINE' or 'TYPE REALTIME' to specify which.",
            Response.Status.BAD_REQUEST);
      } else if (offExists) {
        tableNameWithType = off;
        resolvedType = TableType.OFFLINE;
      } else if (rtExists) {
        tableNameWithType = rt;
        resolvedType = TableType.REALTIME;
      } else {
        throw new ControllerApplicationException(LOGGER,
            "Table not found: " + fullyQualifiedRaw + ". If you only have permission for "
                + "one variant, use 'SHOW CREATE TABLE ... TYPE OFFLINE' or 'TYPE REALTIME'.",
            Response.Status.NOT_FOUND);
      }
    }

    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      // hasTable returned true but getTableConfig returned null. This indicates ZK inconsistency
      // (torn write or concurrent delete between the two reads), not a missing table from the
      // caller's perspective. Surface as 500 so monitoring catches the inconsistency rather than
      // mislead the caller into thinking their reference is wrong.
      throw new ControllerApplicationException(LOGGER,
          "Table " + tableNameWithType + " has IdealState but no TableConfig in ZK; "
              + "possible torn write or concurrent delete.",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    Schema schema = _pinotHelixResourceManager.getTableSchema(tableNameWithType);
    if (schema == null) {
      // Unlike a missing TableConfig (which would indicate a torn-write inconsistency since
      // hasTable just returned true), a missing schema is reachable in normal operation:
      // schemas can be deleted independently via DELETE /schemas/{name} while a table
      // still references them. Surface as 404 rather than 500 so the operator sees an
      // actionable user-error code rather than a phantom controller-bug code.
      throw new ControllerApplicationException(LOGGER,
          "Schema '" + tableNameWithType
              + "' not found; SHOW CREATE TABLE requires the schema to exist. "
              + "Re-create it via POST /schemas if it was deleted.",
          Response.Status.NOT_FOUND);
    }

    // Use the resolved database (which incorporates the Database header) so the emitted DDL
    // carries the correct qualifier even when the caller omits the db. prefix in the SQL.
    String ddl;
    try {
      ddl = CanonicalDdlEmitter.emit(schema, tableConfig, database);
    } catch (IllegalArgumentException e) {
      // The emitter explicitly throws IllegalArgumentException to signal "this schema or config
      // cannot be expressed in the current DDL grammar":
      //   - SchemaEmitter.validateEmittable: unsupported column types (MAP, LIST, STRUCT, UNKNOWN)
      //   - PropertyExtractor: TableCustomConfig key collides with a reserved DDL property name
      // Both are caller-actionable: rename the offending column/property or use the JSON API for
      // SHOW. Surface as 400 so the caller sees a deterministic client error rather than a 500.
      throw new ControllerApplicationException(LOGGER,
          "SHOW CREATE TABLE is not supported for table " + tableNameWithType
              + ": " + e.getMessage(),
          Response.Status.BAD_REQUEST, e);
    } catch (RuntimeException e) {
      // Anything else from emit() is an unexpected controller-side failure (NPE in extractor,
      // JsonProcessingException wrapped as runtime, etc.). 400 would mislead the caller —
      // surface as 500 so monitoring picks it up.
      throw new ControllerApplicationException(LOGGER,
          "Internal error rendering SHOW CREATE TABLE for " + tableNameWithType
              + ": " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }

    return new DdlExecutionResponse()
        .setOperation(DdlOperation.SHOW_CREATE_TABLE)
        .setDatabaseName(database)
        .setTableName(tableNameWithType)
        .setTableType(resolvedType.toString())
        .setDdl(ddl)
        .setMessage("Rendered canonical CREATE TABLE for " + tableNameWithType + ".");
  }

  private DdlExecutionResponse executeShow(String database, HttpHeaders headers, Request httpRequest) {
    // SHOW TABLES is scoped to a single database to prevent silently leaking table names across
    // databases the caller may not have access to. The database resolution chain (SQL FROM
    // clause -> Database header -> DEFAULT_DATABASE) ensures we always have an explicit scope.
    String scopedDatabase = database == null ? CommonConstants.DEFAULT_DATABASE : database;
    // SHOW TABLES is a cluster-level listing operation, not a per-table read. Use the same
    // TargetType.CLUSTER + GET_TABLE action pair that the existing @Authorize-annotated
    // GET /tables endpoint uses, so secured deployments grant SHOW TABLES to callers who
    // already have cluster-level table-listing access rather than requiring a fictitious
    // per-table READ on a resource named after the database.
    String endpointUrl = httpRequest.getRequestURL().toString();
    AccessControl accessControl = _accessControlFactory.create();
    AccessControlUtils.validatePermission(null, AccessType.READ, headers, endpointUrl, accessControl);
    if (!accessControl.hasAccess(headers, TargetType.CLUSTER, scopedDatabase,
        Actions.Cluster.GET_TABLE)) {
      throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
    }
    List<String> tables = _pinotHelixResourceManager.getAllRawTables(scopedDatabase);
    return new DdlExecutionResponse()
        .setOperation(DdlOperation.SHOW_TABLES)
        .setDatabaseName(scopedDatabase)
        .setTableNames(tables)
        .setMessage("Found " + tables.size() + " table(s).");
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  /// Returns the database name to use for this request. Precedence: explicit `db.name` in
  /// the SQL statement → `Database` HTTP header → `null` (default database).
  private static String resolveDatabase(String fromSql, HttpHeaders headers) {
    String fromHeader = headers == null ? null : headers.getHeaderString(DATABASE);
    if (fromSql != null) {
      if (StringUtils.isNotEmpty(fromHeader) && !fromSql.equals(fromHeader)
          && !(CommonConstants.DEFAULT_DATABASE.equalsIgnoreCase(fromSql)
          && CommonConstants.DEFAULT_DATABASE.equalsIgnoreCase(fromHeader))) {
        throw new ControllerApplicationException(LOGGER,
            "Database name '" + fromSql + "' from SQL does not match database name '"
                + fromHeader + "' from header",
            Response.Status.BAD_REQUEST);
      }
      return fromSql;
    }
    return fromHeader;
  }

  private static JsonNode toJson(Object obj) {
    try {
      return JsonUtils.objectToJsonNode(obj);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to serialize compiled DDL artifact: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  private static ControllerApplicationException badRequest(String message) {
    return new ControllerApplicationException(LOGGER, message, Response.Status.BAD_REQUEST);
  }

  private static String translateTableNameForDdl(String tableName, HttpHeaders headers) {
    try {
      return DatabaseUtils.translateTableName(tableName, headers);
    } catch (DatabaseConflictException | IllegalArgumentException e) {
      throw new ControllerApplicationException(LOGGER,
          "Invalid database/table reference '" + tableName + "': " + e.getMessage(),
          Response.Status.BAD_REQUEST, e);
    }
  }
}
