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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import org.apache.pinot.common.exception.SchemaAlreadyExistsException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.LogicalTableConfigUtils;
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
import org.apache.pinot.controller.util.TaskConfigUtils;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.ManualAuthorization;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableConfigValidatorRegistry;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
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


/**
 * Controller endpoint for executing Pinot SQL DDL statements. Currently supports CREATE TABLE,
 * DROP TABLE, SHOW TABLES, and SHOW CREATE TABLE.
 *
 * <p>Pipeline:
 * <ol>
 *   <li>{@link DdlCompiler} parses + compiles the SQL into a {@link CompiledDdl}.</li>
 *   <li>Database/table names are translated through {@link DatabaseUtils#translateTableName}
 *       so the {@code Database} HTTP header is honoured uniformly.</li>
 *   <li>Authorization is invoked based on the operation type.</li>
 *   <li>Execution either persists via {@link PinotHelixResourceManager} or, when {@code dryRun}
 *       is true, returns the compiled artifacts without mutating cluster state.</li>
 * </ol>
 *
 * <p>The endpoint is intentionally a single POST that dispatches by operation. This keeps the
 * client surface area small and matches the canonical {@code POST /sql/ddl} contract from the
 * design.
 */
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
  /** Maximum accepted SQL input length (256 KB) to prevent unbounded parser memory allocation. */
  private static final int MAX_DDL_SQL_LENGTH = 256 * 1024;

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  PinotTaskManager _pinotTaskManager;

  @Inject
  PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;

  @Inject
  AccessControlFactory _accessControlFactory;

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
      @ApiResponse(code = 400, message = "Bad request (parse/semantic error)"),
      @ApiResponse(code = 409, message = "Table already exists"),
      @ApiResponse(code = 500, message = "Internal server error")
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
    if (request.getSql().length() > MAX_DDL_SQL_LENGTH) {
      throw badRequest("DDL statement exceeds maximum length of " + MAX_DDL_SQL_LENGTH + " characters.");
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
    String tableNameWithType = DatabaseUtils.translateTableName(
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
    String schemaName = DatabaseUtils.translateTableName(dottedSchemaName, headers);
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

    // Run the full schema/table validation stack that the existing /tables and /tableConfigs APIs
    // apply before any ZK write. This catches invalid combinations (upsert without primary keys,
    // field configs referencing non-existent columns, task configs with bad column references,
    // etc.) that the compiler alone cannot detect. Runs for both dry-run and live create.
    validateTableConfig(create.getSchema(), create.getTableConfig());

    if (dryRun) {
      response.setMessage("Dry run: validated CREATE TABLE without persisting.");
      return Response.ok(response).build();
    }

    // When a schema for this raw table name already exists (the common case when adding the
    // second physical variant of a hybrid pair), verify the DDL column list is equivalent to
    // the stored schema. Silently accepting a mismatched column list would create a table whose
    // runtime schema differs from what the DDL described.
    Schema storedSchema = _pinotHelixResourceManager.getSchema(schemaName);
    boolean schemaPreexisted = storedSchema != null;
    if (schemaPreexisted) {
      // Compare only the column-shape attributes that the DDL column list actually controls.
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

    boolean schemaCreatedHere = false;
    try {
      // override=false: an existing schema with the same name is a precondition violation, not
      // something we silently overwrite. The other-typed table variant or another caller's
      // schema would otherwise be clobbered out from under them.
      if (!schemaPreexisted) {
        _pinotHelixResourceManager.addSchema(create.getSchema(), false, false);
        schemaCreatedHere = true;
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
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
    } catch (SchemaAlreadyExistsException e) {
      // The override=false addSchema call lost a race with another schema writer. Surface 409.
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
    } catch (Exception e) {
      // Intentionally do NOT roll back the schema on a generic addTable() failure. The two
      // hasOfflineTable/hasRealtimeTable reads required to decide "is this schema orphaned?"
      // are non-atomic, and a concurrent sibling CREATE that succeeds between the two reads
      // would have its schema deleted out from under it — orphaning a live table. Pinot's
      // existing /tables endpoint also leaves the schema in place when table creation fails,
      // so the contract is consistent: schemas can outlive tables, and stale schemas can be
      // removed via DELETE /schemas/{name}. Surface a hint so the operator knows the schema
      // remains and how to clean it up if the failure is genuinely permanent.
      String hint = schemaCreatedHere
          ? " (schema '" + schemaName + "' was created and remains; remove via DELETE /schemas/"
              + schemaName + " if the failure is permanent and no other variant uses it)"
          : "";
      // ControllerApplicationException(LOGGER, ...) logs the exception, so don't double-log here.
      throw new ControllerApplicationException(LOGGER,
          "Failed to create table " + tableNameWithType + ": " + e.getMessage() + hint,
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  /**
   * Compares two schemas by the column-shape attributes that a DDL column list actually controls
   * (column name, data type, field type, single/multi-value, NOT NULL, default null value, and —
   * for DATETIME columns — format and granularity) and returns a human-readable description of
   * the first mismatch, or {@code null} if the shapes are equivalent. Schema-level metadata that
   * a DDL column list does not express ({@code primaryKeyColumns}, {@code tags},
   * {@code enableColumnBasedNullHandling}, {@code description}) is intentionally ignored so the
   * second hybrid variant can be created via DDL without restating metadata set by the first
   * variant.
   */
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
      // Compare default null value by its string form: the DDL always sets defaults from a
      // string literal, and FieldSpec normalizes the stored representation to a typed value.
      // The string form is the stable projection that survives both serialization round trips.
      String storedDefault = storedSpec.getDefaultNullValueString();
      String compiledDefault = compiledSpec.getDefaultNullValueString();
      if (!Objects.equals(storedDefault, compiledDefault)) {
        return "column '" + columnName + "' default null value differs (stored=" + storedDefault
            + ", DDL=" + compiledDefault + ")";
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
    return null;
  }

  /**
   * Runs the same schema/table validation stack that {@code /tables} and {@code /tableConfigs}
   * apply before any ZK write, so DDL-created configs are subject to the same rules as
   * JSON-API-created configs (upsert/dedup primary-key requirements, field config column
   * references, task config validation, registry-level semantic checks, etc.).
   */
  private void validateTableConfig(Schema schema, TableConfig tableConfig) {
    try {
      TableConfigUtils.validateTableName(tableConfig);
      TableConfigUtils.validate(tableConfig, schema, null);
      _pinotHelixResourceManager.validateTableTenantConfig(tableConfig);
      _pinotHelixResourceManager.validateTableTaskMinionInstanceTagConfig(tableConfig);
      TaskConfigUtils.validateTaskConfigs(tableConfig, schema, _pinotTaskManager, null);
      TableConfigValidatorRegistry.validate(tableConfig, schema);
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
    String fullyQualifiedRaw = DatabaseUtils.translateTableName(dottedRaw, headers);

    // Compute the candidate typed names BEFORE existence filtering so we can authorize against
    // the user's intent (not just whatever happens to exist now). This prevents an unauthorized
    // caller from probing existence via 200/404 vs 403.
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
      if (_pinotHelixResourceManager.hasTable(candidate)) {
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

    if (dryRun) {
      response.setMessage("Dry run: " + targets.size() + " table(s) would be dropped.");
      return response;
    }

    // Reject drop if any target is referenced by a logical table, matching the safeguard in
    // the existing /tables and /tableConfigs DELETE endpoints.
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

    // Drop each target individually and track outcomes. A failure on one variant should not
    // prevent the response from reporting what was already deleted — partial deletes on a hybrid
    // table are expensive to recover from, so we surface per-target status instead of surfacing
    // only the first failure and hiding the rest.
    List<String> dropped = new ArrayList<>();
    List<String> failed = new ArrayList<>();
    Exception firstFailure = null;
    Response.Status firstFailureStatus = null;
    for (String target : targets) {
      try {
        // Remove task schedules before deletion so tasks are not triggered during the drop.
        // tableTasksCleanup may throw ControllerApplicationException (e.g. BAD_REQUEST when
        // active tasks are still running). That status code carries actionable user-level
        // information and must be preserved instead of being collapsed to 500 below.
        PinotTableRestletResource.tableTasksCleanup(target, false,
            _pinotHelixResourceManager, _pinotHelixTaskResourceManager);
        TableType type = TableNameBuilder.getTableTypeFromTableName(target);
        _pinotHelixResourceManager.deleteTable(fullyQualifiedRaw, type, null);
        dropped.add(target);
        LOGGER.info("DDL dropped table {}", target);
      } catch (ControllerApplicationException e) {
        // The CAE constructor already logs the underlying error at the appropriate level, and
        // the wrapping CAE thrown after the loop will log again. A third log here would be
        // redundant noise — record a one-line breadcrumb at WARN without the throwable.
        LOGGER.warn("DROP TABLE on {} failed: {}", target, e.getMessage());
        failed.add(target);
        if (firstFailure == null) {
          firstFailure = e;
          firstFailureStatus = Response.Status.fromStatusCode(e.getResponse().getStatus());
        }
      } catch (Exception e) {
        // Genuinely unexpected errors get full stack traces — the wrapping CAE below will also
        // log, but for arbitrary RuntimeExceptions / Helix failures the duplicate is acceptable
        // because the operator may need both the per-target context and the aggregated message.
        LOGGER.error("DROP TABLE on {} failed unexpectedly", target, e);
        failed.add(target);
        if (firstFailure == null) {
          firstFailure = e;
          firstFailureStatus = Response.Status.INTERNAL_SERVER_ERROR;
        }
      }
    }
    // Intentionally do NOT delete the shared schema when the last physical variant is removed.
    // This matches the existing `/tables/{name}` DELETE contract, which also leaves the schema
    // intact. Two doors into the same state machine must have the same side effects; a caller
    // who wants to remove the schema can issue an explicit DELETE /schemas/{name} afterwards.
    if (failed.isEmpty()) {
      response.setMessage("Dropped " + dropped.size() + " table(s).");
      LOGGER.info("DDL dropped tables {}", dropped);
      return response;
    }
    // At least one target failed. Surface a structured error that names both what succeeded
    // and what failed so the operator knows which variant needs manual cleanup. Preserve the
    // first failure's status code so client-actionable failures (e.g. active tasks → 400)
    // remain visible to the caller; only fall back to 500 for genuinely unexpected failures.
    String causeDesc = firstFailure.getMessage() != null
        ? firstFailure.getMessage() : firstFailure.getClass().getSimpleName();
    String partialPrefix = dropped.isEmpty() ? "" : "Partial DROP TABLE: dropped " + dropped + ", ";
    String msg = partialPrefix + "failed to drop " + failed + ": " + causeDesc
        + (dropped.isEmpty() ? "" : ". The successfully-dropped variant must be re-created if "
            + "the original DROP was unintended.");
    throw new ControllerApplicationException(LOGGER, msg,
        firstFailureStatus == null ? Response.Status.INTERNAL_SERVER_ERROR : firstFailureStatus,
        firstFailure);
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
    String fullyQualifiedRaw = DatabaseUtils.translateTableName(dottedRaw, headers);

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
      // Authorize BOTH candidates before probing existence so the caller cannot infer which
      // variant exists from a 403 vs 404 response, and so an auth failure on the realtime
      // variant is not masked by a 404 produced before the check runs.
      String off = TableNameBuilder.OFFLINE.tableNameWithType(fullyQualifiedRaw);
      String rt = TableNameBuilder.REALTIME.tableNameWithType(fullyQualifiedRaw);
      ResourceUtils.checkPermissionAndAccess(off, httpRequest, headers,
          AccessType.READ, Actions.Table.GET_TABLE_CONFIG, _accessControlFactory, LOGGER);
      ResourceUtils.checkPermissionAndAccess(rt, httpRequest, headers,
          AccessType.READ, Actions.Table.GET_TABLE_CONFIG, _accessControlFactory, LOGGER);
      boolean offExists = _pinotHelixResourceManager.hasTable(off);
      boolean rtExists = _pinotHelixResourceManager.hasTable(rt);
      if (offExists && rtExists) {
        // Both variants exist; silently picking one would return DDL for the wrong variant.
        // Require the caller to disambiguate with an explicit TYPE clause.
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
            "Table not found: " + fullyQualifiedRaw, Response.Status.NOT_FOUND);
      }
    }

    org.apache.pinot.spi.config.table.TableConfig tableConfig =
        _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      // Should not happen — hasTable just succeeded — but treat as not-found rather than 500.
      throw new ControllerApplicationException(LOGGER,
          "Table " + tableNameWithType + " disappeared during SHOW CREATE.",
          Response.Status.NOT_FOUND);
    }
    org.apache.pinot.spi.data.Schema schema =
        _pinotHelixResourceManager.getTableSchema(tableNameWithType);
    if (schema == null) {
      throw new ControllerApplicationException(LOGGER,
          "Schema not found for " + tableNameWithType
              + "; SHOW CREATE TABLE requires both schema and config to exist.",
          Response.Status.INTERNAL_SERVER_ERROR);
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

  /**
   * Returns the database name to use for this request. Precedence: explicit {@code db.name} in
   * the SQL statement → {@code Database} HTTP header → {@code null} (default database).
   */
  private static String resolveDatabase(String fromSql, HttpHeaders headers) {
    if (fromSql != null) {
      return fromSql;
    }
    if (headers == null) {
      return null;
    }
    return headers.getHeaderString(DATABASE);
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
}
