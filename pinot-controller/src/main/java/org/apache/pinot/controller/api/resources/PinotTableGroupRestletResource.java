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
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.config.TableGroupConfigUtils;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.assignment.instance.InstanceAssignmentDriver;
import org.apache.pinot.spi.config.table.TableGroupConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(tags = Constants.TABLE_GROUP_TAG)
@Path("/")
public class PinotTableGroupRestletResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotTableGroupRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/groups")
  @ApiOperation(value = "Gets list of all table-groups")
  public String listTableGroups() {
    List<String> groups = ZKMetadataProvider.getAllGroups(_pinotHelixResourceManager.getPropertyStore());
    return JsonUtils.newObjectNode().set("groups", JsonUtils.objectToJsonNode(groups)).toString();
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/groups")
  @ApiOperation(value = "Creates a new table group")
  public SuccessResponse createTableGroup(String groupConfigStr) {
    try {
      TableGroupConfig tableGroupConfig = JsonUtils.stringToObject(groupConfigStr, TableGroupConfig.class);
      String groupName = tableGroupConfig.getGroupName();
      HelixDataAccessor helixDataAccessor = _pinotHelixResourceManager.getHelixZkManager().getHelixDataAccessor();
      InstancePartitions instancePartitions = InstanceAssignmentDriver.assignInstancesToGroup(groupName,
          helixDataAccessor.getChildValues(helixDataAccessor.keyBuilder().instanceConfigs(), true),
          tableGroupConfig.getInstanceAssignmentConfig());
      _pinotHelixResourceManager.addTableGroup(groupName, tableGroupConfig);
      InstancePartitionsUtils.persistGroupInstancePartitions(_pinotHelixResourceManager.getPropertyStore(),
          groupName, instancePartitions);
      return new SuccessResponse(String.format("Group %s successfully created", groupName));
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/groups/{groupName}")
  @ApiOperation(value = "Gets config for a given table-group")
  public String getTableGroup(@ApiParam(value = "name of group") @PathParam("groupName") String groupName) {
    try {
      TableGroupConfig tableGroupConfig = getTableGroupConfig(groupName);
      return JsonUtils.objectToString(tableGroupConfig);
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/groups/{groupName}")
  @ApiOperation(value = "Updates the configuration for a table-group")
  public SuccessResponse updateTableGroup(String groupConfigStr,
      @ApiParam(value = "name of group") @PathParam("groupName") String groupName) {
    try {
      TableGroupConfig tableGroupConfig = JsonUtils.stringToObject(groupConfigStr, TableGroupConfig.class);
      if (!tableGroupConfig.getGroupName().equals(groupName)) {
        throw new ControllerApplicationException(LOGGER, "You cannot change group name", Response.Status.BAD_REQUEST);
      }
      ZNRecord znRecord = TableGroupConfigUtils.toZNRecord(tableGroupConfig);
      if (!_pinotHelixResourceManager.getPropertyStore().set(
          ZKMetadataProvider.constructPropertyStorePathForTableGroup(groupName), znRecord, AccessOption.PERSISTENT)) {
        throw new ControllerApplicationException(LOGGER, "Error updating table group config",
            Response.Status.BAD_REQUEST);
      }
      return new SuccessResponse("Updated group config successfully");
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
    }
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/groups/{groupName}/recompute")
  @ApiOperation(value = "Re-computes instance-partitions for the group")
  public SuccessResponse recomputeInstanceAssignmentForGroup(
      @ApiParam(value = "name of group") @PathParam("groupName") String groupName) {
    TableGroupConfig tableGroupConfig = null;
    try {
      tableGroupConfig = getTableGroupConfig(groupName);
      HelixDataAccessor helixDataAccessor = _pinotHelixResourceManager.getHelixZkManager().getHelixDataAccessor();
      InstancePartitions instancePartitions = InstanceAssignmentDriver.assignInstancesToGroup(groupName,
          helixDataAccessor.getChildValues(helixDataAccessor.keyBuilder().instanceConfigs(), true),
          tableGroupConfig.getInstanceAssignmentConfig());
      InstancePartitionsUtils.persistGroupInstancePartitions(_pinotHelixResourceManager.getPropertyStore(),
          groupName, instancePartitions);
      return new SuccessResponse(String.format("Group %s successfully created", groupName));
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/groups/{groupName}/instancePartitions")
  @ApiOperation(value = "Gets the current instance-partitions for a table-group")
  public String getTableGroupInstancePartitions(
      @ApiParam(value = "name of group") @PathParam("groupName") String groupName) {
    try {
      InstancePartitions instancePartitions = InstancePartitionsUtils.fetchGroupInstancePartitions(
          _pinotHelixResourceManager.getPropertyStore(), groupName);
      return JsonUtils.objectToString(instancePartitions);
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  private TableGroupConfig getTableGroupConfig(String groupName)
      throws IOException {
    ZNRecord znRecord = _pinotHelixResourceManager.getPropertyStore().get(
        ZKMetadataProvider.constructPropertyStorePathForTableGroup(groupName), null, AccessOption.PERSISTENT);
    LOGGER.info(znRecord.getMapFields().toString());
    return TableGroupConfigUtils.fromZNRecord(znRecord);
  }
}
