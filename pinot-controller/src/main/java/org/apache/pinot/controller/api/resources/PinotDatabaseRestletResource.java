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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.DatabaseConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.DATABASE_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```"),
    @ApiKeyAuthDefinition(name = CommonConstants.DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = CommonConstants.DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")}))
@Path("/")
public class PinotDatabaseRestletResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotDatabaseRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/databases")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_DATABASE)
  @ApiOperation(value = "List all database names", notes = "Lists all database names")
  public List<String> listDatabaseNames() {
    return _pinotHelixResourceManager.getDatabaseNames();
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/databases/{databaseName}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.DELETE_DATABASE)
  @ApiOperation(value = "Delete all tables in given database name", notes = "Delete all tables in given database name")
  public DeleteDatabaseResponse deleteTablesInDatabase(
      @ApiParam(value = "Database name", required = true) @PathParam("databaseName") String databaseName,
      @ApiParam(value = "Run in dryRun mode initially to know the list of tables that will be deleted in actual run. "
          + "No tables will be deleted when dryRun=true", required = true, defaultValue = "true")
      @QueryParam("dryRun") boolean dryRun) {
    List<String> tablesInDatabase = _pinotHelixResourceManager.getAllTables(databaseName);
    List<String> deletedTables = new ArrayList<>(tablesInDatabase.size());
    List<DeletionFailureWrapper> failedTables = new ArrayList<>(tablesInDatabase.size());
    if (dryRun) {
      deletedTables.addAll(tablesInDatabase);
    } else {
      for (String table : tablesInDatabase) {
        boolean isSchemaDeleted = false;
        try {
          TableType tableType = TableNameBuilder.getTableTypeFromTableName(table);
          String rawTableName = TableNameBuilder.extractRawTableName(table);
          _pinotHelixResourceManager.deleteSchema(rawTableName);
          LOGGER.info("Deleted schema: {}", rawTableName);
          isSchemaDeleted = true;
          _pinotHelixResourceManager.deleteTable(table, tableType, null);
          LOGGER.info("Deleted table: {}", table);
          deletedTables.add(table);
        } catch (Exception e) {
          if (isSchemaDeleted) {
            LOGGER.error("Failed to delete table {}", table);
          } else {
            LOGGER.error("Failed to delete table and schema for {}", table);
          }
          failedTables.add(new DeletionFailureWrapper(table, e.getMessage()));
        }
      }
    }
    return new DeleteDatabaseResponse(deletedTables, failedTables, dryRun);
  }

  /**
   * API to update the quota configs for database
   * If database config is not present it will be created implicitly
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/databases/{databaseName}/quotas")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.UPDATE_DATABASE_QUOTA)
  @ApiOperation(value = "Update database quotas", notes = "Update database quotas")
  public SuccessResponse setDatabaseQuota(
      @PathParam("databaseName") String databaseName, @QueryParam("maxQueriesPerSecond") String queryQuota,
      @Context HttpHeaders httpHeaders) {
    if (!databaseName.equals(DatabaseUtils.extractDatabaseFromHttpHeaders(httpHeaders))) {
      throw new ControllerApplicationException(LOGGER, "Database config name and request context does not match",
          Response.Status.BAD_REQUEST);
    }
    try {
      DatabaseConfig databaseConfig = _pinotHelixResourceManager.getDatabaseConfig(databaseName);
      QuotaConfig quotaConfig = new QuotaConfig(null, queryQuota);
      if (databaseConfig == null) {
         databaseConfig = new DatabaseConfig(databaseName, quotaConfig);
        _pinotHelixResourceManager.addDatabaseConfig(databaseConfig);
      } else {
        databaseConfig.setQuotaConfig(quotaConfig);
        _pinotHelixResourceManager.updateDatabaseConfig(databaseConfig);
      }
      return new SuccessResponse("Database quotas for database config " + databaseName + " successfully updated");
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  /**
   * API to get database quota configs.
   * Will return null if database config is not defined or database quotas are not defined
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/databases/{databaseName}/quotas")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_DATABASE_QUOTA)
  @ApiOperation(value = "Get database quota configs", notes = "Get database quota configs")
  public QuotaConfig getDatabaseQuota(
      @PathParam("databaseName") String databaseName, @Context HttpHeaders httpHeaders) {
    if (!databaseName.equals(DatabaseUtils.extractDatabaseFromHttpHeaders(httpHeaders))) {
      throw new ControllerApplicationException(LOGGER, "Database config name and request context does not match",
          Response.Status.BAD_REQUEST);
    }
    DatabaseConfig databaseConfig = _pinotHelixResourceManager.getDatabaseConfig(databaseName);
    if (databaseConfig != null) {
      return databaseConfig.getQuotaConfig();
    }
    HelixAdmin helixAdmin = _pinotHelixResourceManager.getHelixAdmin();
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
        .forCluster(_pinotHelixResourceManager.getHelixClusterName()).build();
    String defaultQueryQuota = helixAdmin.getConfig(configScope,
            Collections.singletonList(CommonConstants.Helix.DATABASE_MAX_QUERIES_PER_SECOND))
            .getOrDefault(CommonConstants.Helix.DATABASE_MAX_QUERIES_PER_SECOND, null);
    return new QuotaConfig(null, defaultQueryQuota);
  }
}

class DeleteDatabaseResponse {
  private final List<String> _deletedTables;
  private final List<DeletionFailureWrapper> _failedTables;
  private final boolean _dryRun;

  public DeleteDatabaseResponse(List<String> deletedTables, List<DeletionFailureWrapper> failedTables, boolean dryRun) {
    _deletedTables = deletedTables;
    _failedTables = failedTables;
    _dryRun = dryRun;
  }

  @JsonProperty("deletedTables")
  public List<String> getDeletedTables() {
    return _deletedTables;
  }

  @JsonProperty("failedTables")
  public List<DeletionFailureWrapper> getFailedTables() {
    return _failedTables;
  }

  @JsonProperty("dryRun")
  public boolean isDryRun() {
    return _dryRun;
  }
}

class DeletionFailureWrapper {
  private final String _tableName;
  private final String _errorMessage;

  public DeletionFailureWrapper(String tableName, String errorMessage) {
    _tableName = tableName;
    _errorMessage = errorMessage;
  }

  @JsonProperty("tableName")
  public String getTableName() {
    return _tableName;
  }

  @JsonProperty("errorMessage")
  public String getErrorMessage() {
    return _errorMessage;
  }
}
