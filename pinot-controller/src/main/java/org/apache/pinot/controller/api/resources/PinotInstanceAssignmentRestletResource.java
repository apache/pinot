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
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.io.IOException;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.assignment.instance.InstanceAssignmentDriver;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class PinotInstanceAssignmentRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotInstanceAssignmentRestletResource.class);

  @Inject
  PinotHelixResourceManager _resourceManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/instancePartitions")
  @ApiOperation(value = "Get the instance partitions")
  public Map<InstancePartitionsType, InstancePartitions> getInstancePartitions(
      @ApiParam(value = "Name of the table") @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|CONSUMING|COMPLETED") @QueryParam("type") @Nullable InstancePartitionsType instancePartitionsType) {
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap = new TreeMap<>();

    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType != TableType.REALTIME) {
      if (instancePartitionsType == InstancePartitionsType.OFFLINE || instancePartitionsType == null) {
        InstancePartitions offlineInstancePartitions = InstancePartitionsUtils
            .fetchInstancePartitions(_resourceManager.getPropertyStore(),
                InstancePartitionsType.OFFLINE.getInstancePartitionsName(rawTableName));
        if (offlineInstancePartitions != null) {
          instancePartitionsMap.put(InstancePartitionsType.OFFLINE, offlineInstancePartitions);
        }
      }
    }
    if (tableType != TableType.OFFLINE) {
      if (instancePartitionsType == InstancePartitionsType.CONSUMING || instancePartitionsType == null) {
        InstancePartitions consumingInstancePartitions = InstancePartitionsUtils
            .fetchInstancePartitions(_resourceManager.getPropertyStore(),
                InstancePartitionsType.CONSUMING.getInstancePartitionsName(rawTableName));
        if (consumingInstancePartitions != null) {
          instancePartitionsMap.put(InstancePartitionsType.CONSUMING, consumingInstancePartitions);
        }
      }
      if (instancePartitionsType == InstancePartitionsType.COMPLETED || instancePartitionsType == null) {
        InstancePartitions completedInstancePartitions = InstancePartitionsUtils
            .fetchInstancePartitions(_resourceManager.getPropertyStore(),
                InstancePartitionsType.COMPLETED.getInstancePartitionsName(rawTableName));
        if (completedInstancePartitions != null) {
          instancePartitionsMap.put(InstancePartitionsType.COMPLETED, completedInstancePartitions);
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
  @Authenticate(AccessType.CREATE)
  @ApiOperation(value = "Assign server instances to a table")
  public Map<InstancePartitionsType, InstancePartitions> assignInstances(
      @ApiParam(value = "Name of the table") @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|CONSUMING|COMPLETED") @QueryParam("type") @Nullable InstancePartitionsType instancePartitionsType,
      @ApiParam(value = "Whether to do dry-run") @DefaultValue("false") @QueryParam("dryRun") boolean dryRun) {
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap = new TreeMap<>();
    List<InstanceConfig> instanceConfigs = _resourceManager.getAllHelixInstanceConfigs();

    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType != TableType.REALTIME && (instancePartitionsType == InstancePartitionsType.OFFLINE
        || instancePartitionsType == null)) {
      TableConfig offlineTableConfig = _resourceManager.getOfflineTableConfig(tableName);
      if (offlineTableConfig != null) {
        try {
          if (InstanceAssignmentConfigUtils
              .allowInstanceAssignment(offlineTableConfig, InstancePartitionsType.OFFLINE)) {
            instancePartitionsMap.put(InstancePartitionsType.OFFLINE, new InstanceAssignmentDriver(offlineTableConfig)
                .assignInstances(InstancePartitionsType.OFFLINE, instanceConfigs));
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
    if (tableType != TableType.OFFLINE && instancePartitionsType != InstancePartitionsType.OFFLINE) {
      TableConfig realtimeTableConfig = _resourceManager.getRealtimeTableConfig(tableName);
      if (realtimeTableConfig != null) {
        try {
          InstanceAssignmentDriver instanceAssignmentDriver = new InstanceAssignmentDriver(realtimeTableConfig);
          if (instancePartitionsType == InstancePartitionsType.CONSUMING || instancePartitionsType == null) {
            if (InstanceAssignmentConfigUtils
                .allowInstanceAssignment(realtimeTableConfig, InstancePartitionsType.CONSUMING)) {
              instancePartitionsMap.put(InstancePartitionsType.CONSUMING,
                  instanceAssignmentDriver.assignInstances(InstancePartitionsType.CONSUMING, instanceConfigs));
            }
          }
          if (instancePartitionsType == InstancePartitionsType.COMPLETED || instancePartitionsType == null) {
            if (InstanceAssignmentConfigUtils
                .allowInstanceAssignment(realtimeTableConfig, InstancePartitionsType.COMPLETED)) {
              instancePartitionsMap.put(InstancePartitionsType.COMPLETED,
                  instanceAssignmentDriver.assignInstances(InstancePartitionsType.COMPLETED, instanceConfigs));
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
  @Authenticate(AccessType.UPDATE)
  @ApiOperation(value = "Create/update the instance partitions")
  public Map<InstancePartitionsType, InstancePartitions> setInstancePartitions(
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
        return Collections.singletonMap(InstancePartitionsType.OFFLINE, instancePartitions);
      }
    }
    if (tableType != TableType.OFFLINE) {
      if (InstancePartitionsType.CONSUMING.getInstancePartitionsName(rawTableName).equals(instancePartitionsName)) {
        persistInstancePartitionsHelper(instancePartitions);
        return Collections.singletonMap(InstancePartitionsType.CONSUMING, instancePartitions);
      }
      if (InstancePartitionsType.COMPLETED.getInstancePartitionsName(rawTableName).equals(instancePartitionsName)) {
        persistInstancePartitionsHelper(instancePartitions);
        return Collections.singletonMap(InstancePartitionsType.COMPLETED, instancePartitions);
      }
    }

    throw new ControllerApplicationException(LOGGER, "Instance partitions cannot be applied to the table",
        Response.Status.BAD_REQUEST);
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/instancePartitions")
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Remove the instance partitions")
  public SuccessResponse removeInstancePartitions(
      @ApiParam(value = "Name of the table") @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|CONSUMING|COMPLETED") @QueryParam("type") @Nullable InstancePartitionsType instancePartitionsType) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType != TableType.REALTIME && (instancePartitionsType == InstancePartitionsType.OFFLINE
        || instancePartitionsType == null)) {
      removeInstancePartitionsHelper(InstancePartitionsType.OFFLINE.getInstancePartitionsName(rawTableName));
    }
    if (tableType != TableType.OFFLINE) {
      if (instancePartitionsType == InstancePartitionsType.CONSUMING || instancePartitionsType == null) {
        removeInstancePartitionsHelper(InstancePartitionsType.CONSUMING.getInstancePartitionsName(rawTableName));
      }
      if (instancePartitionsType == InstancePartitionsType.COMPLETED || instancePartitionsType == null) {
        removeInstancePartitionsHelper(InstancePartitionsType.COMPLETED.getInstancePartitionsName(rawTableName));
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
  @Authenticate(AccessType.CREATE)
  @ApiOperation(value = "Replace an instance in the instance partitions")
  public Map<InstancePartitionsType, InstancePartitions> replaceInstance(
      @ApiParam(value = "Name of the table") @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|CONSUMING|COMPLETED") @QueryParam("type") @Nullable InstancePartitionsType instancePartitionsType,
      @ApiParam(value = "Old instance to be replaced", required = true) @QueryParam("oldInstanceId") String oldInstanceId,
      @ApiParam(value = "New instance to replace with", required = true) @QueryParam("newInstanceId") String newInstanceId) {
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap =
        getInstancePartitions(tableName, instancePartitionsType);
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
