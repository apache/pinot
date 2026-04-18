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
import java.util.List;
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
import org.apache.pinot.spi.config.table.TableConfigValidatorRegistry;
import org.apache.pinot.spi.config.table.TableType;
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
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Bad request (parse/semantic error)"),
      @ApiResponse(code = 409, message = "Table already exists"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public DdlExecutionResponse executeDdl(
      @ApiParam(value = "DDL request body with 'sql' field", required = true)
          DdlExecutionRequest request,
      @ApiParam(value = "When true, compile and validate but do not persist.")
          @QueryParam("dryRun") @DefaultValue("false") boolean dryRun,
      @Context HttpHeaders httpHeaders, @Context Request httpRequest) {
    if (request == null || StringUtils.isBlank(request.getSql())) {
      throw badRequest("Request body must include a non-empty 'sql' field.");
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
        return executeDrop((CompiledDropTable) compiled, effectiveDatabase, dryRun,
            httpHeaders, httpRequest);
      case SHOW_TABLES:
        return executeShow(effectiveDatabase, httpHeaders, httpRequest);
      case SHOW_CREATE_TABLE:
        return executeShowCreate((CompiledShowCreateTable) compiled, effectiveDatabase,
            httpHeaders, httpRequest);
      default:
        throw new ControllerApplicationException(LOGGER, "Unhandled DDL operation: " + op,
            Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  // -------------------------------------------------------------------------------------------
  // CREATE
  // -------------------------------------------------------------------------------------------

  private DdlExecutionResponse executeCreate(CompiledCreateTable create, String database,
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

    // Run the full schema/table validation stack that the existing /tables and /tableConfigs APIs
    // apply before any ZK write. This catches invalid combinations (upsert without primary keys,
    // field configs referencing non-existent columns, task configs with bad column references,
    // etc.) that the compiler alone cannot detect. Runs for both dry-run and live create.
    // Validation runs BEFORE the existence check so that IF NOT EXISTS is a no-op only for
    // semantically valid statements; invalid DDL is always rejected regardless of IF NOT EXISTS.
    validateTableConfig(create.getSchema(), create.getTableConfig());

    // Check existence for both live and dry-run paths: dry-run must faithfully predict conflicts
    // so callers can use it for pre-deployment validation ("would this DDL succeed?").
    if (_pinotHelixResourceManager.hasTable(tableNameWithType)) {
      if (create.isIfNotExists()) {
        response.setMessage("Table " + tableNameWithType + " already exists; CREATE IF NOT EXISTS is a no-op.");
        return response;
      }
      throw new ControllerApplicationException(LOGGER,
          "Table " + tableNameWithType + " already exists.", Response.Status.CONFLICT);
    }

    if (dryRun) {
      response.setMessage("Dry run: validated CREATE TABLE without persisting.");
      return response;
    }

    // When a schema for this raw table name already exists (the common case when adding the
    // second physical variant of a hybrid pair), verify the DDL column list is equivalent to
    // the stored schema. Silently accepting a mismatched column list would create a table whose
    // runtime schema differs from what the DDL described.
    Schema storedSchema = _pinotHelixResourceManager.getSchema(schemaName);
    boolean schemaPreexisted = storedSchema != null;
    if (schemaPreexisted) {
      try {
        String storedJson = JsonUtils.objectToString(storedSchema);
        String compiledJson = JsonUtils.objectToString(create.getSchema());
        if (!storedJson.equals(compiledJson)) {
          throw new ControllerApplicationException(LOGGER,
              "Schema '" + schemaName + "' already exists and does not match the column list in the DDL. "
                  + "Either omit the column list to reuse the existing schema, or drop and recreate the table pair "
                  + "if the schema has genuinely changed.",
              Response.Status.CONFLICT);
        }
      } catch (ControllerApplicationException e) {
        throw e;
      } catch (Exception e) {
        throw new ControllerApplicationException(LOGGER,
            "Failed to compare schemas for '" + schemaName + "': " + e.getMessage(),
            Response.Status.INTERNAL_SERVER_ERROR, e);
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
      return response;
    } catch (TableAlreadyExistsException e) {
      // Race: another caller added the table between our hasTable check and addTable.
      // Only roll back the schema we created here if no table for this raw name now exists —
      // if another caller won the race and its table is live, removing the schema would
      // orphan that table. Re-check existence with the winner's table still present before
      // deciding to clean up.
      if (schemaCreatedHere && !_pinotHelixResourceManager.hasTable(tableNameWithType)) {
        rollbackSchemaIfCreated(schemaName, true);
      }
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
    } catch (SchemaAlreadyExistsException e) {
      // The override=false addSchema call lost a race with another schema writer. Surface 409.
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.CONFLICT, e);
    } catch (Exception e) {
      // Only roll back the schema if no table for this raw name now exists. A concurrent request
      // may have added the sibling physical variant (OFFLINE vs REALTIME) between our addSchema()
      // and this addTable() failure; deleting the schema would orphan that live table. Check both
      // typed variants before deciding to remove the shared schema.
      if (schemaCreatedHere) {
        boolean offlineNowExists = _pinotHelixResourceManager.hasOfflineTable(schemaName);
        boolean realtimeNowExists = _pinotHelixResourceManager.hasRealtimeTable(schemaName);
        rollbackSchemaIfCreated(schemaName, !offlineNowExists && !realtimeNowExists);
      }
      // ControllerApplicationException(LOGGER, ...) logs the exception, so don't double-log here.
      throw new ControllerApplicationException(LOGGER,
          "Failed to create table " + tableNameWithType + ": " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  private void rollbackSchemaIfCreated(String schemaName, boolean schemaCreatedHere) {
    if (!schemaCreatedHere) {
      return;
    }
    try {
      _pinotHelixResourceManager.deleteSchema(schemaName);
      LOGGER.info("Rolled back schema {} after failed table create", schemaName);
    } catch (Exception rollbackEx) {
      LOGGER.error("Failed to roll back schema {} after failed table create; manual cleanup "
          + "may be required", schemaName, rollbackEx);
    }
  }

  /**
   * Runs the same schema/table validation stack that {@code /tables} and {@code /tableConfigs}
   * apply before any ZK write, so DDL-created configs are subject to the same rules as
   * JSON-API-created configs (upsert/dedup primary-key requirements, field config column
   * references, task config validation, registry-level semantic checks, etc.).
   */
  private void validateTableConfig(Schema schema,
      org.apache.pinot.spi.config.table.TableConfig tableConfig) {
    try {
      TableConfigUtils.validateTableName(tableConfig);
      TableConfigUtils.validate(tableConfig, schema, null);
      _pinotHelixResourceManager.validateTableTenantConfig(tableConfig);
      _pinotHelixResourceManager.validateTableTaskMinionInstanceTagConfig(tableConfig);
      TaskConfigUtils.validateTaskConfigs(tableConfig, schema, _pinotTaskManager, null);
      TableConfigValidatorRegistry.validate(tableConfig, schema);
    } catch (ControllerApplicationException e) {
      throw e;
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Table config validation failed: " + e.getMessage(), Response.Status.BAD_REQUEST, e);
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
    for (String target : targets) {
      try {
        // Remove task schedules before deletion so tasks are not triggered during the drop.
        PinotTableRestletResource.tableTasksCleanup(target, false,
            _pinotHelixResourceManager, _pinotHelixTaskResourceManager);
        TableType type = TableNameBuilder.getTableTypeFromTableName(target);
        _pinotHelixResourceManager.deleteTable(fullyQualifiedRaw, type, null);
        dropped.add(target);
        LOGGER.info("DDL dropped table {}", target);
      } catch (Exception e) {
        LOGGER.error("Failed to drop table {} during DDL DROP TABLE", target, e);
        failed.add(target);
        if (firstFailure == null) {
          firstFailure = e;
        }
      }
    }
    // Delete the shared schema when the last physical variant has been removed. A schema
    // without any table leaves stale metadata that blocks future CREATE TABLE for the same
    // raw name with a different column list. Only attempt if all targets were deleted.
    if (failed.isEmpty()) {
      boolean offlineExists = _pinotHelixResourceManager.hasOfflineTable(fullyQualifiedRaw);
      boolean realtimeExists = _pinotHelixResourceManager.hasRealtimeTable(fullyQualifiedRaw);
      if (!offlineExists && !realtimeExists) {
        try {
          _pinotHelixResourceManager.deleteSchema(fullyQualifiedRaw);
          LOGGER.info("DDL deleted schema {} after dropping last table variant", fullyQualifiedRaw);
        } catch (Exception schemaEx) {
          LOGGER.warn("Failed to delete schema {} after DROP TABLE; manual cleanup may be required",
              fullyQualifiedRaw, schemaEx);
        }
      }
      response.setMessage("Dropped " + dropped.size() + " table(s).");
      LOGGER.info("DDL dropped tables {}", dropped);
      return response;
    }
    // At least one target failed. Surface a structured error that names both what succeeded
    // and what failed so the operator knows which variant needs manual cleanup.
    String causeDesc = firstFailure.getMessage() != null
        ? firstFailure.getMessage() : firstFailure.getClass().getSimpleName();
    String msg = "Partial DROP TABLE: dropped " + dropped + ", failed to drop " + failed
        + ": " + causeDesc;
    throw new ControllerApplicationException(LOGGER, msg,
        Response.Status.INTERNAL_SERVER_ERROR, firstFailure);
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
      if (_pinotHelixResourceManager.hasTable(off)) {
        tableNameWithType = off;
        resolvedType = TableType.OFFLINE;
      } else if (_pinotHelixResourceManager.hasTable(rt)) {
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
      // SchemaEmitter.validateEmittable() throws IllegalArgumentException for unsupported column
      // types (MAP, LIST, STRUCT, UNKNOWN) that the current DDL grammar cannot represent. Return
      // 400 so the caller sees a deterministic client error rather than a 500.
      throw new ControllerApplicationException(LOGGER,
          "SHOW CREATE TABLE is not supported for table " + tableNameWithType
              + ": " + e.getMessage(),
          Response.Status.BAD_REQUEST, e);
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
