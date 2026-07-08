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
import org.apache.pinot.sql.ddl.compile.CompiledCreateMaterializedView;
import org.apache.pinot.sql.ddl.compile.CompiledCreateTable;
import org.apache.pinot.sql.ddl.compile.CompiledDdl;
import org.apache.pinot.sql.ddl.compile.CompiledDropMaterializedView;
import org.apache.pinot.sql.ddl.compile.CompiledDropTable;
import org.apache.pinot.sql.ddl.compile.CompiledShowCreateMaterializedView;
import org.apache.pinot.sql.ddl.compile.CompiledShowCreateTable;
import org.apache.pinot.sql.ddl.compile.DdlCompilationException;
import org.apache.pinot.sql.ddl.compile.DdlCompileContext;
import org.apache.pinot.sql.ddl.compile.DdlCompiler;
import org.apache.pinot.sql.ddl.compile.DdlOperation;
import org.apache.pinot.sql.ddl.reverse.CanonicalDdlEmitter;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/// Controller endpoint for executing Pinot SQL DDL statements. Currently supports CREATE TABLE,
/// CREATE MATERIALIZED VIEW, DROP TABLE, DROP MATERIALIZED VIEW, SHOW TABLES,
/// SHOW MATERIALIZED VIEWS, SHOW CREATE TABLE, and SHOW CREATE MATERIALIZED VIEW.
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
      notes = "Supports CREATE TABLE, CREATE MATERIALIZED VIEW, DROP TABLE, DROP MATERIALIZED VIEW, SHOW TABLES, "
          + "SHOW MATERIALIZED VIEWS, SHOW CREATE TABLE, and SHOW CREATE MATERIALIZED VIEW. Returns the generated "
          + "Schema/TableConfig (CREATE), operation outcome (DROP/SHOW TABLES/SHOW MATERIALIZED VIEWS), or canonical "
          + "DDL string (SHOW CREATE TABLE / SHOW CREATE MATERIALIZED VIEW).")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success (DROP, SHOW TABLES, SHOW MATERIALIZED VIEWS, "
          + "SHOW CREATE TABLE, SHOW CREATE MATERIALIZED VIEW, dry-run, IF NOT EXISTS / IF EXISTS)"),
      @ApiResponse(code = 201, message = "Table or materialized view created"),
      @ApiResponse(code = 400, message = "Bad request (parse error, semantic error, oversize input, "
          + "unsupported emitter type, type-incompatible default value, or DROP/SHOW CREATE form does "
          + "not match the underlying table shape)"),
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

    // Compile context carries the TableCache (for the MV schema inferer's Calcite catalog)
    // and the request-header database (so an unqualified `FROM <src>` resolves under the
    // operator's intended database). The `_pinotHelixResourceManager == null` branch is the
    // unit-test escape hatch for non-inferer DDL forms (CREATE TABLE, DROP, SHOW, SHOW CREATE,
    // explicit-column CREATE MATERIALIZED VIEW). The inferer surfaces a clear "no TableCache
    // configured" error if it ever runs under STATELESS in production.
    String requestDatabase = httpHeaders == null ? null : httpHeaders.getHeaderString(DATABASE);
    DdlCompileContext compileCtx = (_pinotHelixResourceManager == null)
        ? DdlCompileContext.STATELESS
        : new DdlCompileContext(_pinotHelixResourceManager.getTableCache(), requestDatabase);

    CompiledDdl compiled;
    try {
      compiled = DdlCompiler.compile(request.getSql(), compileCtx);
    } catch (DdlCompilationException e) {
      throw badRequest(e.getMessage());
    }
    // Any other RuntimeException is a programmer error or unexpected upstream failure;
    // let it propagate to the JAX-RS default handler as 500 so monitoring fires on it
    // instead of seeing it as a user-facing 400.

    DdlOperation op = compiled.getOperation();
    String requestedDatabase = compiled.getDatabaseName();
    String effectiveDatabase = resolveDatabase(requestedDatabase, httpHeaders);

    // Authorization is performed inside each execute*() method, AFTER the target table name has
    // been DB-translated. Authorizing on the pre-translation name would let a header-supplied
    // database substitute past the auth check.
    //
    // Dispatch order mirrors `DdlOperation`: Catalog → Table → Materialized View, lifecycle
    // CREATE → SHOW CREATE → DROP within each object-level family.
    switch (op) {
      // Catalog DDL.
      case SHOW_TABLES:
        return Response.ok(executeShow(effectiveDatabase, httpHeaders, httpRequest)).build();
      case SHOW_MATERIALIZED_VIEWS:
        return Response.ok(executeShowMaterializedViews(effectiveDatabase, httpHeaders, httpRequest)).build();
      // Table DDL.
      case CREATE_TABLE:
        return executeCreate((CompiledCreateTable) compiled, effectiveDatabase, dryRun,
            httpHeaders, httpRequest);
      case SHOW_CREATE_TABLE:
        return Response.ok(
            executeShowCreate((CompiledShowCreateTable) compiled, effectiveDatabase,
                httpHeaders, httpRequest)).build();
      case DROP_TABLE:
        return Response.ok(
            executeDrop((CompiledDropTable) compiled, effectiveDatabase, dryRun,
                httpHeaders, httpRequest)).build();
      // Materialized View DDL.
      case CREATE_MATERIALIZED_VIEW:
        return executeCreateMaterializedView((CompiledCreateMaterializedView) compiled,
            effectiveDatabase, dryRun, httpHeaders, httpRequest);
      case SHOW_CREATE_MATERIALIZED_VIEW:
        return Response.ok(
            executeShowCreateMaterializedView((CompiledShowCreateMaterializedView) compiled,
                effectiveDatabase, httpHeaders, httpRequest)).build();
      case DROP_MATERIALIZED_VIEW:
        return Response.ok(
            executeDropMaterializedView((CompiledDropMaterializedView) compiled, effectiveDatabase,
                dryRun, httpHeaders, httpRequest)).build();
      default:
        throw new ControllerApplicationException(LOGGER, "Unhandled DDL operation: " + op,
            Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  // -------------------------------------------------------------------------------------------
  // Table DDL: CREATE TABLE
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

    // Q2=B preflight (raw-name exclusivity rule, mirror of `executeCreateMaterializedView`):
    // an MV must not share its raw name with EITHER a plain OFFLINE table OR a REALTIME table
    // at the same name. The two checks together — split across the CREATE TABLE and CREATE
    // MATERIALIZED VIEW endpoints — enforce a symmetric invariant: any given raw name is
    // exclusively owned by one of {plain OFFLINE table, REALTIME table, hybrid pair,
    // materialized view}, regardless of the creation order.
    //
    // On the OFFLINE create path: if an MV occupies the OFFLINE znode, refuse with 400. Without
    // this guard a CREATE TABLE could 409 with the generic "already exists" message — true but
    // unhelpful, because the operator would not know they were colliding with a derived MV that
    // has its own DROP form, refresh schedule, and minion task wiring.
    //
    // On the REALTIME create path: an MV at the OFFLINE half would form a hybrid pair whose
    // OFFLINE half is a materialized view — a state with no defined semantics in the broker
    // rewrite, minion task generator, or consistency manager. Refuse symmetrically with 400.
    //
    // IF NOT EXISTS does not suppress these type-mismatch errors: IF NOT EXISTS suppresses
    // "object not found at the requested type", not "wrong DDL verb for the conflicting
    // object". Mirror of `executeDrop`'s Q2=B preflight.
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    TableConfig conflictingOfflineConfig = null;
    if (create.getTableConfig().getTableType() == TableType.OFFLINE) {
      conflictingOfflineConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    } else {
      String offlineNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
      conflictingOfflineConfig = _pinotHelixResourceManager.getTableConfig(offlineNameWithType);
    }
    if (conflictingOfflineConfig != null && conflictingOfflineConfig.isMaterializedView()) {
      String tableTypeLabel = create.getTableConfig().getTableType().toString();
      throw new ControllerApplicationException(LOGGER,
          "Cannot create " + tableTypeLabel + " table '" + rawTableName
              + "': a materialized view already exists at this name. A "
              + (create.getTableConfig().getTableType() == TableType.OFFLINE ? "plain" : tableTypeLabel)
              + " table cannot share its name with a materialized view — pick a different name "
              + "for the table, or drop the existing materialized view first.",
          Response.Status.BAD_REQUEST);
    }

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
        schemaForValidation, Map.of());
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
          _pinotHelixResourceManager, _controllerConf, _pinotTaskManager, null);
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
  // Materialized View DDL: CREATE MATERIALIZED VIEW
  // -------------------------------------------------------------------------------------------

  /// Persists a `CREATE MATERIALIZED VIEW` DDL. Mirrors [#executeCreate] but is hard-wired
  /// to OFFLINE: MVs have no realtime form, so the hybrid-pair coordination in the regular
  /// CREATE TABLE path does not apply.
  ///
  /// The MV runtime watermark znode is intentionally NOT seeded by this endpoint. The
  /// scheduler's cold-start path (see `MaterializedViewTaskScheduler#getWatermarkMs`) derives
  /// the watermark from the source table's earliest segment on the first tick, which is the
  /// only durable source of truth a user can rely on; piping a user-supplied watermark
  /// through DDL added a second, easily-out-of-sync writer and tied the DDL contract to a
  /// runtime znode lifecycle. The grammar accordingly has no `REFRESH START(...)` clause.
  ///
  /// MV-specific semantic validation (source table existence, time-column TIMESTAMP contract,
  /// `bucketTimePeriod` alignment with any `DATETRUNC` in the SELECT, definedSQL parseability,
  /// nested-SELECT rejection) is performed inside [#validateTableConfig] via
  /// `TaskConfigUtils.validateTaskConfigs` → `MaterializedViewTaskGenerator.validateTaskConfigs`
  /// → `MaterializedViewAnalyzer.analyze`. The DDL endpoint does not call the analyzer
  /// directly — the same validation pipeline POST `/tables` uses governs DDL-created MVs.
  private Response executeCreateMaterializedView(CompiledCreateMaterializedView create,
      String database, boolean dryRun, HttpHeaders headers, Request httpRequest) {
    // MV is always OFFLINE: the compiler enforces this. Don't read getTableType() because the
    // compiler-set value would shadow that contract for any reader of this method.
    String tableNameWithType = translateTableNameForDdl(
        TableNameBuilder.OFFLINE.tableNameWithType(create.getTableConfig().getTableName()), headers);
    create.getTableConfig().setTableName(tableNameWithType);

    String compiledSchemaName = create.getSchema().getSchemaName();
    String dottedSchemaName = create.getDatabaseName() == null
        ? compiledSchemaName
        : create.getDatabaseName() + "." + compiledSchemaName;
    String schemaName = translateTableNameForDdl(dottedSchemaName, headers);
    create.getSchema().setSchemaName(schemaName);

    // Authorize against the FULLY-QUALIFIED, post-translation table name. Re-use the
    // CREATE_TABLE action because the MV is implemented as an OFFLINE Pinot table — operators
    // who own the table-create surface should be able to materialize a view over data they
    // already manage. Introducing a separate Actions.Table.CREATE_MATERIALIZED_VIEW would
    // silently lock existing privileged callers out of the new endpoint.
    ResourceUtils.checkPermissionAndAccess(tableNameWithType, httpRequest, headers,
        AccessType.CREATE, Actions.Table.CREATE_TABLE, _accessControlFactory, LOGGER);

    DdlExecutionResponse response = new DdlExecutionResponse()
        .setOperation(DdlOperation.CREATE_MATERIALIZED_VIEW)
        .setDryRun(dryRun)
        .setDatabaseName(database)
        .setTableName(tableNameWithType)
        .setTableType(TableType.OFFLINE.toString())
        .setIfNotExists(create.isIfNotExists())
        .setSchema(toJson(create.getSchema()))
        .setTableConfig(toJson(create.getTableConfig()))
        .setWarnings(create.getWarnings());

    // Q2=B preflight (raw-name exclusivity rule): a materialized view occupies the entire raw
    // name, so it must not collide with EITHER an existing plain OFFLINE table at the same name
    // OR an existing REALTIME table at the same raw name. The two checks together form the
    // CREATE-MV side of the symmetric invariant that `executeCreate` enforces from the
    // CREATE-TABLE side: any given raw name is exclusively owned by one of {plain OFFLINE table,
    // REALTIME table, hybrid pair, materialized view}, never two at once, regardless of the
    // order in which the operator runs CREATE statements.
    //
    // The OFFLINE check runs first because the OFFLINE znode is where an existing MV would live
    // — if we find an MV there, `IF NOT EXISTS` legitimately resolves to a no-op (idempotent
    // re-deployment), matching the CREATE TABLE policy. A plain OFFLINE table at the same name
    // is a type mismatch and is rejected with 400 (NOT 409) so the operator gets a pointer at
    // the resolution rather than a generic "already exists" — mirroring `executeDrop`'s Q2=B
    // preflight on the symmetric DROP side. IF NOT EXISTS does NOT suppress the type-mismatch
    // error: IF NOT EXISTS suppresses "object not found", not "wrong DDL verb for this object".
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    TableConfig existingOfflineConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (existingOfflineConfig != null) {
      if (existingOfflineConfig.isMaterializedView()) {
        if (create.isIfNotExists()) {
          // Echo the persisted state, not the client-attempted body: an idempotent re-run
          // must not advertise the new (rejected) config as if it had been applied.
          populateNoOpResponseFromPersisted(response, existingOfflineConfig, schemaName);
          response.setMessage("Materialized view " + tableNameWithType
              + " already exists; CREATE IF NOT EXISTS is a no-op.");
          return Response.ok(response).build();
        }
        throw new ControllerApplicationException(LOGGER,
            "Materialized view " + tableNameWithType + " already exists.", Response.Status.CONFLICT);
      }
      // Existing OFFLINE config is a plain table (or a corrupted MV missing `definedSQL`):
      // either way, the raw name is held by a non-MV resource and creating an MV here is
      // refused. The 400 message is intentional and identical regardless of IF NOT EXISTS.
      throw new ControllerApplicationException(LOGGER,
          "Cannot create materialized view '" + rawTableName + "': a plain OFFLINE table already "
              + "exists at this name. A materialized view cannot share its name with a plain "
              + "table — pick a different name for the materialized view, or drop the existing "
              + "table first.",
          Response.Status.BAD_REQUEST);
    }
    // No OFFLINE config: check the REALTIME side. REALTIME tables and MVs live in disjoint
    // znodes (foo_REALTIME vs foo_OFFLINE) and would not collide physically, but the contract
    // says raw-name exclusivity is whole-namespace so an MV cannot squat on a name that already
    // has a base-table half. Without this guard, an operator could legitimately end up with
    // both an MV at foo_OFFLINE and a base REALTIME at foo_REALTIME — the resulting "hybrid
    // table whose OFFLINE half is an MV" has no well-defined semantics in the broker rewrite,
    // minion task generator, or consistency manager.
    String realtimeNameWithType = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
    if (_pinotHelixResourceManager.hasTable(realtimeNameWithType)) {
      throw new ControllerApplicationException(LOGGER,
          "Cannot create materialized view '" + rawTableName + "': a REALTIME table already "
              + "exists at this name. A materialized view cannot share its name with a base "
              + "table — pick a different name for the materialized view, or drop the existing "
              + "REALTIME table first.",
          Response.Status.BAD_REQUEST);
    }

    // A pre-existing schema can survive a prior failed CREATE (addSchema succeeded but
    // addTable failed). Accept it iff the column shape matches; otherwise 409 so the
    // operator can clean up the stale schema explicitly rather than silently mutate it.
    Schema storedSchema = _pinotHelixResourceManager.getSchema(schemaName);
    boolean schemaPreexisted = storedSchema != null;
    if (schemaPreexisted) {
      String mismatch = describeColumnShapeMismatch(storedSchema, create.getSchema());
      if (mismatch != null) {
        throw new ControllerApplicationException(LOGGER,
            "Schema '" + schemaName + "' already exists and does not match the column list in the DDL: "
                + mismatch
                + ". Either omit the column list to reuse the existing schema, or drop the stale schema "
                + "and recreate the materialized view.",
            Response.Status.CONFLICT);
      }
    }

    Schema schemaForValidation = schemaPreexisted ? storedSchema : create.getSchema();
    response.setSchema(toJson(schemaForValidation));
    // Apply tuner configs before validation so the validators see the post-tuner shape, matching
    // POST /tables. The MV TableConfig already carries the MaterializedViewTask wiring set up
    // by the compiler; tuners only fill in defaulted index/segment configs.
    TableConfigTunerUtils.applyTunerConfigs(_pinotHelixResourceManager, create.getTableConfig(),
        schemaForValidation, Map.of());
    response.setTableConfig(toJson(create.getTableConfig()));

    // validateTableConfig fans out to TaskConfigUtils.validateTaskConfigs which triggers
    // MaterializedViewTaskGenerator → MaterializedViewAnalyzer.analyze. That path enforces:
    // source-table existence, source/MV time column TIMESTAMP contract, bucketTimePeriod
    // alignment with DATETRUNC unit, SELECT-list coverage of MV schema, no nested SELECTs,
    // and parseable LIMIT injection. There is no need for the DDL endpoint to call
    // MaterializedViewAnalyzer directly — doing so would duplicate the validation and
    // create a code path that drifts from the JSON /tables endpoint over time.
    validateTableConfig(schemaForValidation, create.getTableConfig());
    PinotTableRestletResource.tableTasksValidation(create.getTableConfig(), _pinotHelixTaskResourceManager);

    if (dryRun) {
      response.setMessage("Dry run: validated CREATE MATERIALIZED VIEW without persisting.");
      return Response.ok(response).build();
    }

    try {
      if (!schemaPreexisted) {
        _pinotHelixResourceManager.addSchema(create.getSchema(), false, false);
      }
      _pinotHelixResourceManager.addTable(create.getTableConfig());
    } catch (TableAlreadyExistsException e) {
      Response noOp = mvIfNotExistsNoOpResponse(create.isIfNotExists(), tableNameWithType, schemaName, response);
      if (noOp != null) {
        return noOp;
      }
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
    } catch (SchemaAlreadyExistsException e) {
      // Schema-create race: another writer beat us. Validate-then-retry against the raced
      // schema, mirroring the CREATE TABLE recovery path so the two endpoints behave the same
      // under concurrent provisioning.
      return retryCreateMaterializedViewAfterSchemaRace(create, response, schemaName,
          tableNameWithType, e);
    } catch (Exception e) {
      // Same intentional decision as CREATE TABLE: do not roll back the schema on a generic
      // addTable failure. A concurrent sibling CREATE may already be reusing the schema and
      // a non-atomic orphan-check + deleteSchema would race with it. Operators can DELETE
      // stale schemas explicitly via /schemas.
      throw new ControllerApplicationException(LOGGER,
          "Failed to create materialized view " + tableNameWithType + ": " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }

    response.setMessage("Successfully created materialized view " + tableNameWithType);
    LOGGER.info("DDL created materialized view {}", tableNameWithType);
    return Response.status(Response.Status.CREATED).entity(response).build();
  }

  private Response retryCreateMaterializedViewAfterSchemaRace(CompiledCreateMaterializedView create,
      DdlExecutionResponse response, String schemaName, String tableNameWithType,
      SchemaAlreadyExistsException schemaFailure) {
    Response noOp = mvIfNotExistsNoOpResponse(create.isIfNotExists(), tableNameWithType, schemaName, response);
    if (noOp != null) {
      return noOp;
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
    } catch (TableAlreadyExistsException e) {
      Response noOp2 = mvIfNotExistsNoOpResponse(create.isIfNotExists(), tableNameWithType, schemaName, response);
      if (noOp2 != null) {
        return noOp2;
      }
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to create materialized view " + tableNameWithType
              + " after concurrent schema create: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    response.setMessage("Successfully created materialized view " + tableNameWithType);
    LOGGER.info("DDL created materialized view {} after concurrent schema create", tableNameWithType);
    return Response.status(Response.Status.CREATED).entity(response).build();
  }

  /// Overload: takes an already-fetched [TableConfig] (the fast-path pre-check has it in
  /// hand) and only re-fetches the schema. Avoids a redundant ZK read while preserving the
  /// "echo persisted, not client-attempted" contract documented on the other overload.
  private void populateNoOpResponseFromPersisted(DdlExecutionResponse response,
      TableConfig persistedTableConfig, String schemaName) {
    if (persistedTableConfig != null) {
      response.setTableConfig(toJson(persistedTableConfig));
    }
    Schema persistedSchema = _pinotHelixResourceManager.getSchema(schemaName);
    if (persistedSchema != null) {
      response.setSchema(toJson(persistedSchema));
    }
  }

  /// Rewrites `response.tableConfig` and `response.schema` to reflect what is currently
  /// persisted in Helix/ZK rather than what the client submitted. Without this rewrite, a
  /// `CREATE MATERIALIZED VIEW IF NOT EXISTS` that lands on an existing MV returns 200 with
  /// the client-attempted body echoed back — leading an operator running an idempotent
  /// deployment script to believe their drifted body was applied when in fact the persisted
  /// MV is unchanged. If either ZK read returns null (transient blip, raced DROP, etc.) the
  /// corresponding response field is left as-is so the operator never sees an empty payload.
  private void populateNoOpResponseFromPersisted(DdlExecutionResponse response,
      String tableNameWithType, String schemaName) {
    populateNoOpResponseFromPersisted(response,
        _pinotHelixResourceManager.getTableConfig(tableNameWithType), schemaName);
  }

  /// CREATE MATERIALIZED VIEW IF NOT EXISTS no-op helper for `addTable` retry catches. Returns
  /// a 200 response built from the already-persisted state when the raced table at this name
  /// is itself an MV. Returns null when `ifNotExists` is false, when no config is present, or
  /// when a non-MV plain OFFLINE table raced in (in which case the caller must throw CONFLICT
  /// rather than silently report success). Centralizes the three identical retry checks
  /// (`executeCreateMaterializedView` main path, schema-race retry, and inner addTable retry).
  @Nullable
  private Response mvIfNotExistsNoOpResponse(boolean ifNotExists, String tableNameWithType,
      String schemaName, DdlExecutionResponse response) {
    if (!ifNotExists) {
      return null;
    }
    TableConfig raced = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (raced == null || !raced.isMaterializedView()) {
      return null;
    }
    populateNoOpResponseFromPersisted(response, tableNameWithType, schemaName);
    response.setMessage("Materialized view " + tableNameWithType
        + " already exists; CREATE IF NOT EXISTS is a no-op.");
    return Response.ok(response).build();
  }

  // -------------------------------------------------------------------------------------------
  // Table DDL: DROP TABLE
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

    // Q2=B contract enforcement (mirror of `executeShowCreate`): the user wrote `DROP TABLE`
    // but the target is actually a materialized view. The two DROP forms are strictly
    // partitioned by underlying TableConfig shape — silently allowing this would mean
    // operators could destroy an MV with a copy/paste of a vanilla `DROP TABLE` from a
    // sibling-table runbook, with no syntactic signal that they were dropping a derived
    // asset (and, in the future, cascading dependent views). Returning 400 with a pointer at
    // the correct form forces an explicit acknowledgement that the target is an MV.
    //
    // Important: the check happens BEFORE the IF EXISTS short-circuit above resolves to a
    // no-op for a missing target — `targets` is non-empty here, so we know at least one
    // candidate table really exists, and we can compare its shape against the requested form.
    for (String target : targets) {
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(target);
      if (tableConfig != null && tableConfig.isMaterializedView()) {
        throw new ControllerApplicationException(LOGGER,
            "Table " + target + " is a materialized view. "
                + "Use 'DROP MATERIALIZED VIEW " + fullyQualifiedRaw
                + "' to drop it.",
            Response.Status.BAD_REQUEST);
      }
    }

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
      TaskCleanupRestorer restorer = null;
      try {
        restorer = cleanupTableTasksBeforeDrop(target);
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
        if (restorer != null && restorer.restore()) {
          tasksCleaned = false;
        }
        LOGGER.warn("DROP TABLE on {} failed: {}", target, e.getMessage());
        throw dropFailed(target, dropped, tasksCleaned, e.getResponse().getStatus(), e);
      } catch (Exception e) {
        if (restorer != null && restorer.restore()) {
          tasksCleaned = false;
        }
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

  // -------------------------------------------------------------------------------------------
  // Materialized View DDL: DROP MATERIALIZED VIEW
  // -------------------------------------------------------------------------------------------

  /// Drops a materialized view. MVs are always realized as OFFLINE Pinot tables (the grammar
  /// has no `TYPE` clause for this form, see `SqlPinotDrop`), so the resolve step skips the
  /// dual-variant handshake that [#executeDrop] needs.
  ///
  /// Q2=B contract: this form refuses any target whose TableConfig is **not** an MV (no
  /// `task.MaterializedViewTask.definedSQL` marker), even under `IF EXISTS`. We deliberately do
  /// NOT collapse a type-mismatched target to a 200 no-op: `IF EXISTS` exists to make repeated
  /// runs of an idempotent provisioning script tolerate "already gone" — not to silently accept
  /// a name that resolves to a different kind of object. Returning 200 in that case would
  /// silently leave a plain table in place that the operator expected to be either an MV or
  /// missing; surfacing 400 forces them to inspect the cluster before acting.
  ///
  /// Cleanup is delegated entirely to [PinotHelixResourceManager#deleteTable], which already
  /// (a) blocks the drop when other MVs depend on this base/MV (matching the legacy
  /// `DELETE /materializedViews/{name}` endpoint), (b) removes the MV definition and runtime
  /// znodes via `MaterializedViewDefinitionMetadataUtils.delete` /
  /// `MaterializedViewRuntimeMetadataUtils.delete`, and (c) unregisters from the consistency
  /// manager. Adding any of those steps here would be a maintenance hazard (two doors, one
  /// state machine, drift over time).
  private DdlExecutionResponse executeDropMaterializedView(CompiledDropMaterializedView drop,
      String database, boolean dryRun, HttpHeaders headers, Request httpRequest) {
    String dottedRaw = drop.getDatabaseName() == null
        ? drop.getRawTableName()
        : drop.getDatabaseName() + "." + drop.getRawTableName();
    String fullyQualifiedRaw = translateTableNameForDdl(dottedRaw, headers);
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(fullyQualifiedRaw);

    // Authorize BEFORE existence-revealing branches so 403 and 404 are indistinguishable to an
    // unauthorized caller — same pattern as executeDrop / executeShowCreate. MVs reuse the
    // regular table DELETE permission because MV-as-OFFLINE-table is the underlying realization
    // model and operators already authorized to delete tables should be able to delete MVs.
    ResourceUtils.checkPermissionAndAccess(tableNameWithType, httpRequest, headers,
        AccessType.DELETE, Actions.Table.DELETE_TABLE, _accessControlFactory, LOGGER);

    boolean exists = _pinotHelixResourceManager.hasTable(tableNameWithType)
        || _pinotHelixResourceManager.getTableConfig(tableNameWithType) != null;

    DdlExecutionResponse response = new DdlExecutionResponse()
        .setOperation(DdlOperation.DROP_MATERIALIZED_VIEW)
        .setDryRun(dryRun)
        .setDatabaseName(database)
        .setTableName(tableNameWithType)
        .setTableType(TableType.OFFLINE.toString())
        .setIfExists(drop.isIfExists());

    if (!exists) {
      if (drop.isIfExists()) {
        return response
            .setDeletedTables(List.of())
            .setMessage("No matching materialized view to drop; IF EXISTS satisfied.");
      }
      throw new ControllerApplicationException(LOGGER,
          "Materialized view not found: " + fullyQualifiedRaw,
          Response.Status.NOT_FOUND);
    }

    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      // hasTable was true but the config disappeared between the two reads. Same diagnosis as
      // the SHOW CREATE path: ZK torn write or concurrent delete. Surface as 500 so monitoring
      // catches the inconsistency.
      throw new ControllerApplicationException(LOGGER,
          "Table " + tableNameWithType + " has IdealState but no TableConfig in ZK; "
              + "possible torn write or concurrent delete.",
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    // Q2=B contract enforcement: a plain OFFLINE table is not an MV. We refuse to drop it
    // through this form because the request explicitly said "MATERIALIZED VIEW" — silently
    // accepting plain-table drops here would make the two DROP forms interchangeable, the same
    // pitfall the SHOW CREATE Q2=B prevents on the read side. We refuse even under IF EXISTS
    // for the reason called out in the Javadoc above.
    if (!tableConfig.isMaterializedView()) {
      throw new ControllerApplicationException(LOGGER,
          "Table " + tableNameWithType + " is not a materialized view. "
              + "Use 'DROP TABLE " + fullyQualifiedRaw + "' to drop it.",
          Response.Status.BAD_REQUEST);
    }

    List<String> targets = List.of(tableNameWithType);

    // Reuse the same logical-table and active-task safeguards as executeDrop. An MV is realized
    // as an OFFLINE table, so it is reachable from a logical-table union and may be a target of
    // active MaterializedViewTask runs; the two checks apply for the same reasons.
    assertNoLogicalTableReferences(targets);
    assertNoActiveTasksBeforeDrop(targets);

    if (dryRun) {
      return response
          .setDeletedTables(targets)
          .setMessage("Dry run: would drop materialized view " + tableNameWithType + ".");
    }

    boolean tasksCleaned = false;
    TaskCleanupRestorer restorer = null;
    try {
      restorer = cleanupTableTasksBeforeDrop(tableNameWithType);
      tasksCleaned = true;
      // deleteTable already handles MV znode cleanup (definition + runtime) and consistency-
      // manager unregister; see PinotHelixResourceManager.deleteTable for the full flow. Pass
      // the raw DB-qualified name + explicit OFFLINE type so the call is unambiguous.
      _pinotHelixResourceManager.deleteTable(fullyQualifiedRaw, TableType.OFFLINE, null);
      LOGGER.info("DDL dropped materialized view {}", tableNameWithType);
    } catch (ControllerApplicationException e) {
      if (restorer != null && restorer.restore()) {
        tasksCleaned = false;
      }
      LOGGER.warn("DROP MATERIALIZED VIEW on {} failed: {}", tableNameWithType, e.getMessage());
      throw dropFailed(tableNameWithType, List.of(), tasksCleaned,
          e.getResponse().getStatus(), e);
    } catch (IllegalStateException e) {
      // Thrown by deleteTable when a dependent MV blocks the delete (matches the legacy
      // DELETE /materializedViews/{name} contract). Surface as 409 so the caller sees the same
      // status code that endpoint returns for the same condition.
      if (restorer != null && restorer.restore()) {
        tasksCleaned = false;
      }
      LOGGER.warn("DROP MATERIALIZED VIEW on {} blocked: {}", tableNameWithType, e.getMessage());
      throw dropFailed(tableNameWithType, List.of(), tasksCleaned,
          Response.Status.CONFLICT.getStatusCode(), e);
    } catch (Exception e) {
      if (restorer != null && restorer.restore()) {
        tasksCleaned = false;
      }
      LOGGER.warn("DROP MATERIALIZED VIEW on {} failed unexpectedly: {}",
          tableNameWithType, e.toString());
      throw dropFailed(tableNameWithType, List.of(), tasksCleaned,
          Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e);
    }

    // Schema is intentionally NOT deleted, matching the DROP TABLE contract and the legacy
    // DELETE /materializedViews/{name} endpoint. A caller who wants to remove the schema can
    // issue an explicit DELETE /schemas/{name} afterwards.
    return response
        .setDeletedTables(targets)
        .setMessage("Dropped materialized view " + tableNameWithType + ".");
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

  /// Removes task schedules + completed task entities ahead of a DROP. Returns a token the
  /// caller can use to restore schedules if the subsequent `deleteTable` call rejects the
  /// drop (e.g. blocked by a dependent MV). Returns null if nothing was changed.
  @Nullable
  private TaskCleanupRestorer cleanupTableTasksBeforeDrop(String tableWithType)
      throws Exception {
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableWithType);
    if (tableConfig == null || tableConfig.getTaskConfig() == null) {
      return null;
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
    return schedulesPersisted ? new TaskCleanupRestorer(tableWithType, tableConfig, removedSchedules) : null;
  }

  /// Captures the state needed to undo `cleanupTableTasksBeforeDrop` if a downstream
  /// `deleteTable` rejects the drop (e.g. dependent MV blocks deletion).
  private final class TaskCleanupRestorer {
    private final String _tableWithType;
    private final TableConfig _tableConfig;
    private final Map<String, String> _removedSchedules;

    TaskCleanupRestorer(String tableWithType, TableConfig tableConfig, Map<String, String> removedSchedules) {
      _tableWithType = tableWithType;
      _tableConfig = tableConfig;
      _removedSchedules = removedSchedules;
    }

    /// Returns true iff the restore succeeded; callers use this to decide whether the
    /// "schedules need manual restoration" hint should still appear in the error response.
    boolean restore() {
      try {
        restoreTaskSchedules(_tableWithType, _tableConfig, _removedSchedules);
        return true;
      } catch (ControllerApplicationException restoreFailure) {
        LOGGER.warn("DROP TABLE on {} could not restore task schedules after deleteTable rejected the drop: {}",
            _tableWithType, restoreFailure.getMessage());
        return false;
      }
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
  // SHOW sections begin here.
  //
  // Physical section order in this file is historical (CREATE → DROP → SHOW). The canonical
  // logical grouping — mirrored by `DdlOperation`, `executeDdl`'s switch above, and
  // `DESIGN.md` §3/§4 — is Catalog → Table → Materialized View, lifecycle CREATE → SHOW CREATE
  // → DROP inside each object-level family. Banner prefixes on every section below name the
  // family explicitly so navigation by section is unambiguous regardless of physical position.
  // -------------------------------------------------------------------------------------------

  // -------------------------------------------------------------------------------------------
  // Table DDL: SHOW CREATE TABLE
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

    // Q2=B contract enforcement: an MV-backed OFFLINE table must be inspected via
    // `SHOW CREATE MATERIALIZED VIEW`, not `SHOW CREATE TABLE`. The two emit different DDL
    // (CREATE MATERIALIZED VIEW vs CREATE TABLE) and silently emitting the wrong header would
    // produce DDL that, when replayed, recreates the MV as a plain table (the parser refuses
    // to compile `CREATE TABLE` with an `AS <query>` clause, so the round-trip would simply
    // fail at apply-time — long after the operator copied the wrong text). 400 here points
    // the caller at the correct form before they can act on the misleading output.
    //
    // The check delegates to the canonical `TableConfig#isMaterializedView` flag (single source
    // of truth per PR #18564) — the same predicate the emitter dispatches on — so a config that
    // the SHOW CREATE TABLE path would have routed through the MV branch anyway is caught here
    // before any rendering happens.
    if (tableConfig != null && tableConfig.isMaterializedView()) {
      throw new ControllerApplicationException(LOGGER,
          "Table " + tableNameWithType + " is a materialized view. "
              + "Use 'SHOW CREATE MATERIALIZED VIEW " + fullyQualifiedRaw
              + "' to render its canonical DDL.",
          Response.Status.BAD_REQUEST);
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

  // -------------------------------------------------------------------------------------------
  // Materialized View DDL: SHOW CREATE MATERIALIZED VIEW
  // -------------------------------------------------------------------------------------------

  /// Renders canonical DDL for a materialized view. MVs are always backed by an OFFLINE Pinot
  /// table (the grammar has no `TYPE` clause for this form, see `SqlPinotShow`), so the resolve
  /// step skips the dual-variant handshake that [#executeShowCreate] needs.
  ///
  /// Symmetric guard to the Q2=B enforcement on the regular `SHOW CREATE TABLE` path: a
  /// `SHOW CREATE MATERIALIZED VIEW` against a plain OFFLINE table — i.e. a config whose
  /// canonical `isMaterializedView` flag is false (per PR #18564) — is a 400. We do not
  /// silently fall back to the table emitter because the caller asked for the MV view of the
  /// world; returning a `CREATE TABLE` statement in response would mislead any tooling that
  /// compares the response DDL header against the request.
  private DdlExecutionResponse executeShowCreateMaterializedView(
      CompiledShowCreateMaterializedView show, String database, HttpHeaders headers,
      Request httpRequest) {
    String dottedRaw = show.getDatabaseName() == null
        ? show.getRawTableName()
        : show.getDatabaseName() + "." + show.getRawTableName();
    String fullyQualifiedRaw = translateTableNameForDdl(dottedRaw, headers);
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(fullyQualifiedRaw);

    // Authorize before any existence check so 403 and 404 are indistinguishable to an
    // unauthorized caller (same pattern as executeShowCreate). MVs reuse the regular
    // table READ permission for the same reason CREATE_MATERIALIZED_VIEW reuses CREATE_TABLE:
    // an MV is realized as an OFFLINE Pinot table, and operators already authorized to inspect
    // tables should be able to inspect MVs through this dedicated form without an additional
    // grant.
    ResourceUtils.checkPermissionAndAccess(tableNameWithType, httpRequest, headers,
        AccessType.READ, Actions.Table.GET_TABLE_CONFIG, _accessControlFactory, LOGGER);

    if (!_pinotHelixResourceManager.hasTable(tableNameWithType)) {
      throw new ControllerApplicationException(LOGGER,
          "Materialized view not found: " + fullyQualifiedRaw,
          Response.Status.NOT_FOUND);
    }

    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      throw new ControllerApplicationException(LOGGER,
          "Table " + tableNameWithType + " has IdealState but no TableConfig in ZK; "
              + "possible torn write or concurrent delete.",
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    // Q2=B mirror: refuse the MV form for a plain OFFLINE table. The error is symmetric to the
    // one `executeShowCreate` throws when handed an MV — together they keep the two DDL forms
    // strictly partitioned by what the underlying TableConfig actually is.
    //
    // A previous version of this method also handled the "MaterializedViewTask block present
    // but definedSQL missing" corrupted shape with a distinct 400 / fix hint. That state is no
    // longer reachable for any persisted config: the SPI invariant introduced in PR #18564
    // (TableConfigUtils#validateMaterializedViewInvariants) rejects it at addTable / updateTable
    // time, and the canonical isMaterializedView flag — not the task block — is the identity
    // source. Anything still failing this branch is a plain table that was never an MV.
    if (!tableConfig.isMaterializedView()) {
      throw new ControllerApplicationException(LOGGER,
          "Table " + tableNameWithType + " is not a materialized view. "
              + "Use 'SHOW CREATE TABLE " + fullyQualifiedRaw + "' to render its canonical DDL.",
          Response.Status.BAD_REQUEST);
    }

    Schema schema = _pinotHelixResourceManager.getTableSchema(tableNameWithType);
    if (schema == null) {
      throw new ControllerApplicationException(LOGGER,
          "Schema '" + tableNameWithType
              + "' not found; SHOW CREATE MATERIALIZED VIEW requires the schema to exist. "
              + "Re-create it via POST /schemas if it was deleted.",
          Response.Status.NOT_FOUND);
    }

    String ddl;
    try {
      ddl = CanonicalDdlEmitter.emit(schema, tableConfig, database);
    } catch (IllegalArgumentException e) {
      // IllegalArgumentException from the MV branch of CanonicalDdlEmitter covers the
      // caller-actionable failure modes: non-round-trippable cron schedule (Q1=A —
      // a hand-typed cron that `cronToPeriod` cannot invert), schema/config columns the
      // grammar cannot express, or PROPERTIES collisions. All are 400.
      throw new ControllerApplicationException(LOGGER,
          "SHOW CREATE MATERIALIZED VIEW is not supported for " + tableNameWithType
              + ": " + e.getMessage(),
          Response.Status.BAD_REQUEST, e);
    } catch (RuntimeException e) {
      throw new ControllerApplicationException(LOGGER,
          "Internal error rendering SHOW CREATE MATERIALIZED VIEW for " + tableNameWithType
              + ": " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }

    return new DdlExecutionResponse()
        .setOperation(DdlOperation.SHOW_CREATE_MATERIALIZED_VIEW)
        .setDatabaseName(database)
        .setTableName(tableNameWithType)
        .setTableType(TableType.OFFLINE.toString())
        .setDdl(ddl)
        .setMessage("Rendered canonical CREATE MATERIALIZED VIEW for " + tableNameWithType + ".");
  }

  // -------------------------------------------------------------------------------------------
  // Catalog DDL: SHOW TABLES
  // -------------------------------------------------------------------------------------------

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
  // Catalog DDL: SHOW MATERIALIZED VIEWS
  // -------------------------------------------------------------------------------------------

  private DdlExecutionResponse executeShowMaterializedViews(String database, HttpHeaders headers,
      Request httpRequest) {
    // Same database-scoping convention as SHOW TABLES: SQL `FROM db` -> Database header ->
    // DEFAULT_DATABASE. A scoped listing prevents silently leaking MV names across databases
    // the caller may not have access to.
    String scopedDatabase = database == null ? CommonConstants.DEFAULT_DATABASE : database;
    // Authorization parity with SHOW TABLES and the existing GET /materializedViews REST: an
    // MV is physically an OFFLINE table, so the cluster-level GET_TABLE action is the
    // appropriate scope. Introducing a new `GET_MATERIALIZED_VIEW` action would split the
    // listing auth surface for what is the same underlying read, and would also diverge from
    // the existing REST endpoint without a behaviour change to justify it.
    String endpointUrl = httpRequest.getRequestURL().toString();
    AccessControl accessControl = _accessControlFactory.create();
    AccessControlUtils.validatePermission(null, AccessType.READ, headers, endpointUrl, accessControl);
    if (!accessControl.hasAccess(headers, TargetType.CLUSTER, scopedDatabase,
        Actions.Cluster.GET_TABLE)) {
      throw new ControllerApplicationException(LOGGER, "Permission denied", Response.Status.FORBIDDEN);
    }
    // The lister returns raw names (no `_OFFLINE` suffix) — matching SHOW TABLES and the MV
    // DDL input surface (CREATE / SHOW CREATE / DROP MATERIALIZED VIEW all take raw names
    // because the MV form has no TYPE clause). Returning suffixed names here would mean the
    // listing output could not be copy-pasted back into any other MV DDL.
    //
    // NOTE: An MV may also appear in SHOW TABLES because the underlying TableConfig is an
    // OFFLINE resource. The two listings are intentionally not mutually exclusive — SHOW
    // TABLES has always returned every OFFLINE/REALTIME resource, and we preserve that
    // contract rather than mutate the meaning of SHOW TABLES post-merge.
    List<String> materializedViews =
        _pinotHelixResourceManager.getAllRawMaterializedViewNames(scopedDatabase);
    return new DdlExecutionResponse()
        .setOperation(DdlOperation.SHOW_MATERIALIZED_VIEWS)
        .setDatabaseName(scopedDatabase)
        .setTableNames(materializedViews)
        .setMessage("Found " + materializedViews.size() + " materialized view(s).");
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
