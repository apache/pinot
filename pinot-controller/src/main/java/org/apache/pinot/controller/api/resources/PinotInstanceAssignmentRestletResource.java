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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.collections.CollectionUtils;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.assignment.instance.InstanceAssignmentDriver;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.*;


@Api(tags = Constants.TABLE_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotInstanceAssignmentRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotInstanceAssignmentRestletResource.class);

  @Inject
  PinotHelixResourceManager _resourceManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/instancePartitions")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_INSTANCE_PARTITIONS)
  @ApiOperation(value = "Get the instance partitions")
  public Map<String, InstancePartitions> getInstancePartitions(
      @ApiParam(value = "Name of the table") @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|CONSUMING|COMPLETED|tier name") @QueryParam("type") @Nullable String type) {
    Map<String, InstancePartitions> instancePartitionsMap = new TreeMap<>();

    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType != TableType.REALTIME) {
      if (InstancePartitionsType.OFFLINE.toString().equals(type) || type == null) {
        InstancePartitions offlineInstancePartitions =
            InstancePartitionsUtils.fetchInstancePartitions(_resourceManager.getPropertyStore(),
                InstancePartitionsType.OFFLINE.getInstancePartitionsName(rawTableName));
        if (offlineInstancePartitions != null) {
          instancePartitionsMap.put(InstancePartitionsType.OFFLINE.toString(), offlineInstancePartitions);
        }
      }
    }
    if (tableType != TableType.OFFLINE) {
      if (InstancePartitionsType.CONSUMING.toString().equals(type) || type == null) {
        InstancePartitions consumingInstancePartitions =
            InstancePartitionsUtils.fetchInstancePartitions(_resourceManager.getPropertyStore(),
                InstancePartitionsType.CONSUMING.getInstancePartitionsName(rawTableName));
        if (consumingInstancePartitions != null) {
          instancePartitionsMap.put(InstancePartitionsType.CONSUMING.toString(), consumingInstancePartitions);
        }
      }
      if (InstancePartitionsType.COMPLETED.toString().equals(type) || type == null) {
        InstancePartitions completedInstancePartitions =
            InstancePartitionsUtils.fetchInstancePartitions(_resourceManager.getPropertyStore(),
                InstancePartitionsType.COMPLETED.getInstancePartitionsName(rawTableName));
        if (completedInstancePartitions != null) {
          instancePartitionsMap.put(InstancePartitionsType.COMPLETED.toString(), completedInstancePartitions);
        }
      }
    }

    List<TableConfig> tableConfigs = Arrays.asList(_resourceManager.getRealtimeTableConfig(tableName),
        _resourceManager.getOfflineTableConfig(tableName));

    for (TableConfig tableConfig : tableConfigs) {
      if (tableConfig != null && CollectionUtils.isNotEmpty(tableConfig.getTierConfigsList())) {
        for (TierConfig tierConfig : tableConfig.getTierConfigsList()) {
          if (type == null || type.equals(tierConfig.getName())) {
            InstancePartitions instancePartitions =
                InstancePartitionsUtils.fetchInstancePartitions(_resourceManager.getPropertyStore(),
                    InstancePartitionsUtils.getInstancePartitionsNameForTier(tableConfig.getTableName(),
                        tierConfig.getName()));
            if (instancePartitions != null) {
              instancePartitionsMap.put(tierConfig.getName(), instancePartitions);
            }
          }
        }
      }
    }

    if (instancePartitionsMap.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Failed to find the instance partitions",
          Response.Status.NOT_FOUND);
    } else {
      return instancePartitionsMap;
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/assignInstances")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.CREATE_INSTANCE_PARTITIONS)
  @Authenticate(AccessType.CREATE)
  @ApiOperation(value = "Assign server instances to a table")
  public Map<String, InstancePartitions> assignInstances(
      @ApiParam(value = "Name of the table") @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|CONSUMING|COMPLETED|tier name") @QueryParam("type") @Nullable String type,
      @ApiParam(value = "Whether to do dry-run") @DefaultValue("false") @QueryParam("dryRun") boolean dryRun) {
    Map<String, InstancePartitions> instancePartitionsMap = new TreeMap<>();
    List<InstanceConfig> instanceConfigs = _resourceManager.getAllHelixInstanceConfigs();

    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType != TableType.REALTIME && (InstancePartitionsType.OFFLINE.toString().equals(type) || type == null)) {
      TableConfig offlineTableConfig = _resourceManager.getOfflineTableConfig(tableName);
      if (offlineTableConfig != null) {
        try {
          if (InstanceAssignmentConfigUtils.allowInstanceAssignment(offlineTableConfig,
              InstancePartitionsType.OFFLINE)) {
            assignInstancesForInstancePartitionsType(instancePartitionsMap, offlineTableConfig, instanceConfigs,
                InstancePartitionsType.OFFLINE);
          }
        } catch (IllegalStateException e) {
          throw new ControllerApplicationException(LOGGER, "Caught IllegalStateException", Response.Status.BAD_REQUEST,
              e);
        } catch (Exception e) {
          throw new ControllerApplicationException(LOGGER, "Caught exception while calculating the instance partitions",
              Response.Status.INTERNAL_SERVER_ERROR, e);
        }
      }
    }
    if (tableType != TableType.OFFLINE && !InstancePartitionsType.OFFLINE.toString().equals(type)) {
      TableConfig realtimeTableConfig = _resourceManager.getRealtimeTableConfig(tableName);
      if (realtimeTableConfig != null) {
        try {
          if (InstancePartitionsType.CONSUMING.toString().equals(type) || type == null) {
            if (InstanceAssignmentConfigUtils.allowInstanceAssignment(realtimeTableConfig,
                InstancePartitionsType.CONSUMING)) {
              assignInstancesForInstancePartitionsType(instancePartitionsMap, realtimeTableConfig, instanceConfigs,
                  InstancePartitionsType.CONSUMING);
            }
          }
          if (InstancePartitionsType.COMPLETED.toString().equals(type) || type == null) {
            if (InstanceAssignmentConfigUtils.allowInstanceAssignment(realtimeTableConfig,
                InstancePartitionsType.COMPLETED)) {
              assignInstancesForInstancePartitionsType(instancePartitionsMap, realtimeTableConfig, instanceConfigs,
                  InstancePartitionsType.COMPLETED);
            }
          }
        } catch (IllegalStateException e) {
          throw new ControllerApplicationException(LOGGER, "Caught IllegalStateException", Response.Status.BAD_REQUEST,
              e);
        } catch (Exception e) {
          throw new ControllerApplicationException(LOGGER, "Caught exception while calculating the instance partitions",
              Response.Status.INTERNAL_SERVER_ERROR, e);
        }
      }
    }

    TableConfig realtimeTableConfig = _resourceManager.getRealtimeTableConfig(tableName);
    if (realtimeTableConfig != null) {
      assignInstancesForTier(instancePartitionsMap, realtimeTableConfig, instanceConfigs, type);
    }

    TableConfig offlineTableConfig = _resourceManager.getOfflineTableConfig(tableName);
    if (offlineTableConfig != null) {
      assignInstancesForTier(instancePartitionsMap, offlineTableConfig, instanceConfigs, type);
    }

    if (instancePartitionsMap.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Failed to find the instance assignment config",
          Response.Status.NOT_FOUND);
    }

    if (!dryRun) {
      for (InstancePartitions instancePartitions : instancePartitionsMap.values()) {
        persistInstancePartitionsHelper(instancePartitions);
      }
    }

    return instancePartitionsMap;
  }

  /**
   * Assign instances given the type of instancePartitions.
   * @param instancePartitionsMap the empty map to be filled.
   * @param tableConfig table config
   * @param instanceConfigs list of instance configs
   * @param instancePartitionsType type of instancePartitions
   */
  private void assignInstancesForInstancePartitionsType(Map<String, InstancePartitions> instancePartitionsMap,
      TableConfig tableConfig, List<InstanceConfig> instanceConfigs, InstancePartitionsType instancePartitionsType) {
    String tableNameWithType = tableConfig.getTableName();
    if (!TableConfigUtils.hasPreConfiguredInstancePartitions(tableConfig, instancePartitionsType)) {
      InstancePartitions existingInstancePartitions =
          InstancePartitionsUtils.fetchInstancePartitions(_resourceManager.getHelixZkManager().getHelixPropertyStore(),
              InstancePartitionsUtils.getInstancePartitionsName(tableNameWithType, instancePartitionsType.toString()));
      instancePartitionsMap.put(instancePartitionsType.toString(),
          new InstanceAssignmentDriver(tableConfig).assignInstances(instancePartitionsType, instanceConfigs,
              existingInstancePartitions));
    } else if (InstanceAssignmentConfigUtils.isPreConfigurationBasedAssignment(tableConfig, instancePartitionsType)) {
      // fetch the existing instance partitions, if the table, this is referenced in the new instance partitions
      // generation for minimum difference
      InstancePartitions existingInstancePartitions =
          InstancePartitionsUtils.fetchInstancePartitions(_resourceManager.getHelixZkManager().getHelixPropertyStore(),
              InstancePartitionsUtils.getInstancePartitionsName(tableNameWithType, instancePartitionsType.toString()));
      String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
      // fetch the pre-configured instance partitions, the renaming part is irrelevant as we are not really
      // preserving this preConfigured, but only using it as a reference to generate the new instance partitions
      InstancePartitions preConfigured =
          InstancePartitionsUtils.fetchInstancePartitionsWithRename(_resourceManager.getPropertyStore(),
              tableConfig.getInstancePartitionsMap().get(instancePartitionsType),
              instancePartitionsType.getInstancePartitionsName(rawTableName));
      instancePartitionsMap.put(instancePartitionsType.toString(),
          new InstanceAssignmentDriver(tableConfig).assignInstances(instancePartitionsType, instanceConfigs,
              existingInstancePartitions, preConfigured));
    } else {
      String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
      instancePartitionsMap.put(instancePartitionsType.toString(),
          InstancePartitionsUtils.fetchInstancePartitionsWithRename(_resourceManager.getPropertyStore(),
              tableConfig.getInstancePartitionsMap().get(instancePartitionsType),
              instancePartitionsType.getInstancePartitionsName(rawTableName)));
    }
  }

  private void assignInstancesForTier(Map<String, InstancePartitions> instancePartitionsMap, TableConfig tableConfig,
      List<InstanceConfig> instanceConfigs, String tierName) {
    if (CollectionUtils.isNotEmpty(tableConfig.getTierConfigsList())
        && tableConfig.getInstanceAssignmentConfigMap() != null) {
      for (TierConfig tierConfig : tableConfig.getTierConfigsList()) {
        if ((tierConfig.getName().equals(tierName) || tierName == null)
            && tableConfig.getInstanceAssignmentConfigMap().get(tierConfig.getName()) != null) {
          InstancePartitions existingInstancePartitions = InstancePartitionsUtils.fetchInstancePartitions(
              _resourceManager.getHelixZkManager().getHelixPropertyStore(),
              InstancePartitionsUtils.getInstancePartitionsNameForTier(tableConfig.getTableName(),
                  tierConfig.getName()));

          instancePartitionsMap.put(tierConfig.getName(),
              new InstanceAssignmentDriver(tableConfig).assignInstances(tierConfig.getName(), instanceConfigs,
                  existingInstancePartitions, tableConfig.getInstanceAssignmentConfigMap().get(tierConfig.getName())));
        }
      }
    }
  }

  private void persistInstancePartitionsHelper(InstancePartitions instancePartitions) {
    try {
      LOGGER.info("Persisting instance partitions: {}", instancePartitions);
      InstancePartitionsUtils.persistInstancePartitions(_resourceManager.getPropertyStore(), instancePartitions);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Caught Exception while persisting the instance partitions",
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/instancePartitions")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.UPDATE_INSTANCE_PARTITIONS)
  @Authenticate(AccessType.UPDATE)
  @ApiOperation(value = "Create/update the instance partitions")
  public Map<String, InstancePartitions> setInstancePartitions(
      @ApiParam(value = "Name of the table") @PathParam("tableName") String tableName, String instancePartitionsStr) {
    InstancePartitions instancePartitions;
    try {
      instancePartitions = JsonUtils.stringToObject(instancePartitionsStr, InstancePartitions.class);
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, "Failed to deserialize the instance partitions",
          Response.Status.BAD_REQUEST);
    }

    String instancePartitionsName = instancePartitions.getInstancePartitionsName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType != TableType.REALTIME) {
      if (InstancePartitionsType.OFFLINE.getInstancePartitionsName(rawTableName).equals(instancePartitionsName)) {
        persistInstancePartitionsHelper(instancePartitions);
        return Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(), instancePartitions);
      }
    }
    if (tableType != TableType.OFFLINE) {
      if (InstancePartitionsType.CONSUMING.getInstancePartitionsName(rawTableName).equals(instancePartitionsName)) {
        persistInstancePartitionsHelper(instancePartitions);
        return Collections.singletonMap(InstancePartitionsType.CONSUMING.toString(), instancePartitions);
      }
      if (InstancePartitionsType.COMPLETED.getInstancePartitionsName(rawTableName).equals(instancePartitionsName)) {
        persistInstancePartitionsHelper(instancePartitions);
        return Collections.singletonMap(InstancePartitionsType.COMPLETED.toString(), instancePartitions);
      }
    }

    List<TableConfig> tableConfigs = Arrays.asList(_resourceManager.getRealtimeTableConfig(tableName),
        _resourceManager.getOfflineTableConfig(tableName));

    for (TableConfig tableConfig : tableConfigs) {
      if (tableConfig != null && CollectionUtils.isNotEmpty(tableConfig.getTierConfigsList())) {
        for (TierConfig tierConfig : tableConfig.getTierConfigsList()) {
          if (InstancePartitionsUtils.getInstancePartitionsNameForTier(tableConfig.getTableName(), tierConfig.getName())
              .equals(instancePartitionsName)) {
            persistInstancePartitionsHelper(instancePartitions);
            return Collections.singletonMap(tierConfig.getName(), instancePartitions);
          }
        }
      }
    }

    throw new ControllerApplicationException(LOGGER, "Instance partitions cannot be applied to the table",
        Response.Status.BAD_REQUEST);
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/instancePartitions")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.DELETE_INSTANCE_PARTITIONS)
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Remove the instance partitions")
  public SuccessResponse removeInstancePartitions(
      @ApiParam(value = "Name of the table") @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|CONSUMING|COMPLETED|tier name") @QueryParam("type") @Nullable
          String instancePartitionsType) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType != TableType.REALTIME && (InstancePartitionsType.OFFLINE.toString().equals(instancePartitionsType)
        || instancePartitionsType == null)) {
      removeInstancePartitionsHelper(InstancePartitionsType.OFFLINE.getInstancePartitionsName(rawTableName));
    }
    if (tableType != TableType.OFFLINE) {
      if (InstancePartitionsType.CONSUMING.toString().equals(instancePartitionsType)
          || instancePartitionsType == null) {
        removeInstancePartitionsHelper(InstancePartitionsType.CONSUMING.getInstancePartitionsName(rawTableName));
      }
      if (InstancePartitionsType.COMPLETED.toString().equals(instancePartitionsType)
          || instancePartitionsType == null) {
        removeInstancePartitionsHelper(InstancePartitionsType.COMPLETED.getInstancePartitionsName(rawTableName));
      }
    }

    List<TableConfig> tableConfigs = Arrays.asList(_resourceManager.getRealtimeTableConfig(tableName),
        _resourceManager.getOfflineTableConfig(tableName));

    for (TableConfig tableConfig : tableConfigs) {
      if (tableConfig != null && CollectionUtils.isNotEmpty(tableConfig.getTierConfigsList())) {
        for (TierConfig tierConfig : tableConfig.getTierConfigsList()) {
          if (instancePartitionsType == null || instancePartitionsType.equals(tierConfig.getName())) {
            removeInstancePartitionsHelper(
                InstancePartitionsUtils.getInstancePartitionsNameForTier(tableConfig.getTableName(),
                    tierConfig.getName()));
          }
        }
      }
    }
    return new SuccessResponse("Instance partitions removed");
  }

  private void removeInstancePartitionsHelper(String instancePartitionsName) {
    try {
      LOGGER.info("Removing instance partitions: {}", instancePartitionsName);
      InstancePartitionsUtils.removeInstancePartitions(_resourceManager.getPropertyStore(), instancePartitionsName);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Caught Exception while removing the instance partitions",
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/replaceInstance")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.UPDATE_INSTANCE_PARTITIONS)
  @Authenticate(AccessType.CREATE)
  @ApiOperation(value = "Replace an instance in the instance partitions")
  public Map<String, InstancePartitions> replaceInstance(
      @ApiParam(value = "Name of the table") @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|CONSUMING|COMPLETED|tier name") @QueryParam("type") @Nullable
          String type,
      @ApiParam(value = "Old instance to be replaced", required = true) @QueryParam("oldInstanceId")
          String oldInstanceId,
      @ApiParam(value = "New instance to replace with", required = true) @QueryParam("newInstanceId")
          String newInstanceId) {
    Map<String, InstancePartitions> instancePartitionsMap =
        getInstancePartitions(tableName, type);
    Iterator<InstancePartitions> iterator = instancePartitionsMap.values().iterator();
    while (iterator.hasNext()) {
      InstancePartitions instancePartitions = iterator.next();
      boolean oldInstanceFound = false;
      Map<String, List<String>> partitionToInstancesMap = instancePartitions.getPartitionToInstancesMap();
      for (List<String> instances : partitionToInstancesMap.values()) {
        oldInstanceFound |= Collections.replaceAll(instances, oldInstanceId, newInstanceId);
      }
      if (oldInstanceFound) {
        persistInstancePartitionsHelper(instancePartitions);
      } else {
        iterator.remove();
      }
    }
    if (instancePartitionsMap.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Failed to find the old instance", Response.Status.NOT_FOUND);
    } else {
      return instancePartitionsMap;
    }
  }
}
