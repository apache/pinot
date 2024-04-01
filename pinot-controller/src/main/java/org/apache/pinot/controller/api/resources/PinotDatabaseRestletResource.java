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
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.DATABASE_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
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
          + "No tables will be deleted when dryRun=true", required = true)
      @QueryParam("dryRun") boolean dryRun) {
    List<String> tablesInDatabase = _pinotHelixResourceManager.getAllTables(databaseName);
    List<String> deletedTables = new ArrayList<>(tablesInDatabase.size());
    if (dryRun) {
      deletedTables = tablesInDatabase;
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
        }
      }
    }
    return new DeleteDatabaseResponse(tablesInDatabase, deletedTables, dryRun);
  }
}

class DeleteDatabaseResponse {
  private final List<String> _tablesInDatabase;
  private final List<String> _deletedTables;
  private final boolean _partiallyDeleted;
  private final boolean _dryRun;

  public DeleteDatabaseResponse(List<String> tablesInDatabase, List<String> deletedTables, boolean dryRun) {
    _tablesInDatabase = tablesInDatabase;
    _deletedTables = deletedTables;
    _dryRun = dryRun;
    _partiallyDeleted = tablesInDatabase.size() != deletedTables.size();
  }

  @JsonProperty("tablesInDatabase")
  public List<String> getTablesInDatabase() {
    return _tablesInDatabase;
  }

  @JsonProperty("deletedTables")
  public List<String> getDeletedTables() {
    return _deletedTables;
  }

  public boolean isDryRun() {
    return _dryRun;
  }

  @JsonProperty("partiallyDeleted")
  public boolean isPartiallyDeleted() {
    return _partiallyDeleted;
  }
}
