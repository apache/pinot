/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.api.resources;

import com.linkedin.pinot.common.config.CombinedConfig;
import com.linkedin.pinot.common.config.Deserializer;
import com.linkedin.pinot.common.config.Serializer;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import io.swagger.annotations.ApiOperation;
import java.io.IOException;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/")
public class PinotTableConfigRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableConfigRestletResource.class);

  @Inject
  private PinotHelixResourceManager _resourceManager;

  @GET
  @Produces("application/hocon")
  @Path("/v2/tables/{tableName}")
  @ApiOperation("Displays the configuration of a table")
  public Response readTableConfiguration(
      @PathParam("tableName") String tableName
  ) {
    TableConfig offlineTableConfig =
        _resourceManager.getTableConfig(tableName, CommonConstants.Helix.TableType.OFFLINE);
    TableConfig realtimeTableConfig =
        _resourceManager.getTableConfig(tableName, CommonConstants.Helix.TableType.REALTIME);
    Schema tableSchema = _resourceManager.getTableSchema(tableName);

    if (offlineTableConfig == null && realtimeTableConfig == null) {
      return Response
          .status(Response.Status.NOT_FOUND)
          .build();
    }

    CombinedConfig combinedConfig = new CombinedConfig(offlineTableConfig, realtimeTableConfig, tableSchema);
    String serializedConfig = Serializer.serializeToString(combinedConfig);

    return Response
        .ok(serializedConfig)
        .header("Content-Disposition", "inline")
        .build();
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/v2/tables")
  public Response createNewTable(String tableConfiguration) {
    CombinedConfig config = null;

    try {
      config = Deserializer.deserializeFromString(CombinedConfig.class, tableConfiguration);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while deserializing the table configuration", e);
      return Response
          .serverError()
          .entity(e.getMessage())
          .type(MediaType.TEXT_PLAIN_TYPE)
          .build();
    }

    if (config == null) {
      LOGGER.warn("Failed to deserialize the table configuration: {}", tableConfiguration);
      return Response
          .serverError()
          .entity("Failed to deserialize the table configuration")
          .type(MediaType.TEXT_PLAIN_TYPE)
          .build();
    }

    if (config.getSchema() != null) {
      _resourceManager.addOrUpdateSchema(config.getSchema());
    }

    if (config.getOfflineTableConfig() != null) {
      _resourceManager.addTable(config.getOfflineTableConfig());
    }

    if (config.getRealtimeTableConfig() != null) {
      _resourceManager.addTable(config.getRealtimeTableConfig());
    }

    return Response
        .ok()
        .build();
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/v2/tables/{tableName}")
  public Response updateTable(String tableConfiguration) {
    CombinedConfig config = null;

    try {
      config = Deserializer.deserializeFromString(CombinedConfig.class, tableConfiguration);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while deserializing the table configuration", e);
      return Response
          .serverError()
          .entity(e.getMessage())
          .type(MediaType.TEXT_PLAIN_TYPE)
          .build();
    }

    if (config == null) {
      LOGGER.warn("Failed to deserialize the table configuration: {}", tableConfiguration);
      return Response
          .serverError()
          .entity("Failed to deserialize the table configuration")
          .type(MediaType.TEXT_PLAIN_TYPE)
          .build();
    }

    if (config.getSchema() != null) {
      _resourceManager.addOrUpdateSchema(config.getSchema());
    }

    if (config.getOfflineTableConfig() != null) {
      if (_resourceManager.getAllTables().contains(config.getOfflineTableConfig().getTableName())) {
        try {
          _resourceManager.setExistingTableConfig(config.getOfflineTableConfig(), config.getOfflineTableConfig().getTableName(),
              CommonConstants.Helix.TableType.OFFLINE);
        } catch (IOException e) {
          LOGGER.warn("Failed to update the offline table configuration for table {}", e, config.getOfflineTableConfig().getTableName());
          return Response
              .serverError()
              .entity("Failed to update the offline table configuration")
              .type(MediaType.TEXT_PLAIN_TYPE)
              .build();
        }
      } else {
        _resourceManager.addTable(config.getOfflineTableConfig());
      }
    }

    if (config.getRealtimeTableConfig() != null) {
      if (_resourceManager.getAllTables().contains(config.getRealtimeTableConfig().getTableName())) {
        try {
          _resourceManager.setExistingTableConfig(config.getRealtimeTableConfig(), config.getRealtimeTableConfig().getTableName(),
              CommonConstants.Helix.TableType.REALTIME);
        } catch (IOException e) {
          LOGGER.warn("Failed to update the realtime table configuration for table {}", e, config.getRealtimeTableConfig().getTableName());
          return Response
              .serverError()
              .entity("Failed to update the realtime table configuration")
              .type(MediaType.TEXT_PLAIN_TYPE)
              .build();
        }
      } else {
        _resourceManager.addTable(config.getRealtimeTableConfig());
      }
    }

    return Response
        .ok()
        .build();
  }
}
