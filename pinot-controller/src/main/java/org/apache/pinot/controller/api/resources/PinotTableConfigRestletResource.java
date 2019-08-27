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

import io.swagger.annotations.ApiOperation;
import java.io.IOException;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Variant;
import org.apache.pinot.common.config.CombinedConfig;
import org.apache.pinot.common.config.Deserializer;
import org.apache.pinot.common.config.Serializer;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/")
public class PinotTableConfigRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableConfigRestletResource.class);
  public static final MediaType APPLICATION_HOCON = new MediaType("application", "hocon");
  public static final MediaType TEXT_JAVA_PROPERTIES = new MediaType("text", "x-java-properties");

  @Inject
  private PinotHelixResourceManager _resourceManager;

  @GET
  @Produces({"application/hocon", "text/x-java-properties", "text/plain"})
  @Path("/v2/tables/{tableName}")
  @ApiOperation("Displays the configuration of a table")
  public Response readTableConfiguration(@PathParam("tableName") String tableName, @Context Request request) {
    TableConfig offlineTableConfig =
        _resourceManager.getTableConfig(tableName, CommonConstants.Helix.TableType.OFFLINE);
    TableConfig realtimeTableConfig =
        _resourceManager.getTableConfig(tableName, CommonConstants.Helix.TableType.REALTIME);
    Schema tableSchema = _resourceManager.getTableSchema(tableName);

    if (offlineTableConfig == null && realtimeTableConfig == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    CombinedConfig combinedConfig = new CombinedConfig(offlineTableConfig, realtimeTableConfig, tableSchema);
    String serializedConfig;

    List<Variant> variants =
        Variant.mediaTypes(APPLICATION_HOCON, TEXT_JAVA_PROPERTIES, MediaType.TEXT_PLAIN_TYPE).build();

    Variant variant = request.selectVariant(variants);

    if (variant == null) {
      return Response.notAcceptable(variants).build();
    } else if (APPLICATION_HOCON.equals(variant.getMediaType()) || MediaType.TEXT_PLAIN_TYPE
        .equals(variant.getMediaType())) {
      serializedConfig = Serializer.serializeToString(combinedConfig);
    } else if (TEXT_JAVA_PROPERTIES.equals(variant.getMediaType())) {
      serializedConfig = Serializer.serializeToPropertiesString(combinedConfig);
    } else {
      return Response.notAcceptable(variants).build();
    }

    return Response.ok(serializedConfig, variant).header("Content-Disposition", "inline").build();
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/v2/tables")
  public Response createNewTable(String tableConfiguration) {
    try {
      CombinedConfig config;

      try {
        config = Deserializer.deserializeFromString(CombinedConfig.class, tableConfiguration);
      } catch (Exception e) {
        LOGGER.warn("Caught exception while deserializing the table configuration", e);
        return Response.serverError().entity(e.getMessage()).type(MediaType.TEXT_PLAIN_TYPE).build();
      }

      if (config == null) {
        LOGGER.warn("Failed to deserialize the table configuration: {}", tableConfiguration);
        return Response.serverError().entity("Failed to deserialize the table configuration")
            .type(MediaType.TEXT_PLAIN_TYPE).build();
      }

      // TODO: Fix the bug - when schema is not configured, after deserialization, CombinedConfig will have a non-null
      //       schema with null schema name
      if (config.getSchema() != null) {
        _resourceManager.addOrUpdateSchema(config.getSchema());
      }

      if (config.getOfflineTableConfig() != null) {
        _resourceManager.addTable(config.getOfflineTableConfig());
      }

      if (config.getRealtimeTableConfig() != null) {
        _resourceManager.addTable(config.getRealtimeTableConfig());
      }

      return Response.ok().build();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/v2/tables/{tableName}")
  public Response updateTable(String tableConfiguration) {
    try {
      CombinedConfig config;

      try {
        config = Deserializer.deserializeFromString(CombinedConfig.class, tableConfiguration);
      } catch (Exception e) {
        LOGGER.warn("Caught exception while deserializing the table configuration", e);
        return Response.serverError().entity(e.getMessage()).type(MediaType.TEXT_PLAIN_TYPE).build();
      }

      if (config == null) {
        LOGGER.warn("Failed to deserialize the table configuration: {}", tableConfiguration);
        return Response.serverError().entity("Failed to deserialize the table configuration")
            .type(MediaType.TEXT_PLAIN_TYPE).build();
      }

      Schema schema = config.getSchema();
      if (schema != null) {
        _resourceManager.addOrUpdateSchema(schema);
      }

      TableConfig offlineTableConfig = config.getOfflineTableConfig();
      if (offlineTableConfig != null) {
        String offlineTableName = offlineTableConfig.getTableName();
        if (_resourceManager.hasTable(offlineTableName)) {
          try {
            _resourceManager.setExistingTableConfig(offlineTableConfig);
          } catch (IOException e) {
            LOGGER.warn("Failed to update the table config for table: {}", offlineTableName, e);
            return Response.serverError().entity("Failed to update the offline table configuration")
                .type(MediaType.TEXT_PLAIN_TYPE).build();
          }
        } else {
          _resourceManager.addTable(offlineTableConfig);
        }
      }

      TableConfig realtimeTableConfig = config.getRealtimeTableConfig();
      if (realtimeTableConfig != null) {
        String realtimeTableName = realtimeTableConfig.getTableName();
        if (_resourceManager.hasTable(realtimeTableName)) {
          try {
            _resourceManager.setExistingTableConfig(realtimeTableConfig);
          } catch (IOException e) {
            LOGGER.warn("Failed to update the table config for table: {}", realtimeTableName, e);
            return Response.serverError().entity("Failed to update the realtime table configuration")
                .type(MediaType.TEXT_PLAIN_TYPE).build();
          }
        } else {
          _resourceManager.addTable(realtimeTableConfig);
        }
      }

      return Response.ok().build();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }
}
