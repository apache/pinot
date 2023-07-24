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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.LEAD_CONTROLLER_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/leader")
public class PinotLeadControllerRestletResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotLeadControllerRestletResource.class);

  @Inject
  private PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TABLE_LEADER)
  @ApiOperation(value = "Gets leaders for all tables", notes = "Gets leaders for all tables")
  public LeadControllerResponse getLeadersForAllTables() {
    Map<String, LeadControllerEntry> leadControllerEntryMap = new LinkedHashMap<>();
    HelixManager helixManager = _pinotHelixResourceManager.getHelixZkManager();
    boolean isLeadControllerResourceEnabled;
    try {
      isLeadControllerResourceEnabled = LeadControllerUtils.isLeadControllerResourceEnabled(helixManager);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Exception when checking whether lead controller resource is enabled or not.",
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }

    // Returns empty map if lead controller resource is disabled.
    if (!isLeadControllerResourceEnabled) {
      return new LeadControllerResponse(false, leadControllerEntryMap);
    }

    ExternalView leadControllerResourceExternalView = getLeadControllerResourceExternalView(helixManager);
    for (int partitionId = 0; partitionId < Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE; partitionId++) {
      String partitionName = LeadControllerUtils.generatePartitionName(partitionId);
      String participantInstanceId =
          getParticipantInstanceIdFromExternalView(leadControllerResourceExternalView, partitionName);
      leadControllerEntryMap
          .putIfAbsent(partitionName, new LeadControllerEntry(participantInstanceId, new ArrayList<>()));
    }

    // Assigns all the tables to the relevant partitions.
    List<String> tableNames = _pinotHelixResourceManager.getAllTables();
    for (String tableName : tableNames) {
      String rawTableName = TableNameBuilder.extractRawTableName(tableName);
      int partitionId = LeadControllerUtils.getPartitionIdForTable(rawTableName);
      String partitionName = LeadControllerUtils.generatePartitionName(partitionId);
      LeadControllerEntry leadControllerEntry = leadControllerEntryMap.get(partitionName);
      leadControllerEntry.getTableNames().add(tableName);
    }
    return new LeadControllerResponse(true, leadControllerEntryMap);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_TABLE_LEADER)
  @ApiOperation(value = "Gets leader for a given table", notes = "Gets leader for a given table")
  public LeadControllerResponse getLeaderForTable(
      @ApiParam(value = "Table name", required = true) @PathParam("tableName") String tableName) {
    Map<String, LeadControllerEntry> leadControllerEntryMap = new HashMap<>();
    HelixManager helixManager = _pinotHelixResourceManager.getHelixZkManager();
    boolean isLeadControllerResourceEnabled;
    try {
      isLeadControllerResourceEnabled = LeadControllerUtils.isLeadControllerResourceEnabled(helixManager);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Exception when checking whether lead controller resource is enabled or not.",
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    // Returns controller Id from lead controller resource is enabled, otherwise returns empty map.
    if (!isLeadControllerResourceEnabled) {
      return new LeadControllerResponse(false, leadControllerEntryMap);
    }

    ExternalView leadControllerResourceExternalView = getLeadControllerResourceExternalView(helixManager);
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    int partitionId = LeadControllerUtils.getPartitionIdForTable(rawTableName);
    String partitionName = LeadControllerUtils.generatePartitionName(partitionId);

    String leadControllerId =
        getParticipantInstanceIdFromExternalView(leadControllerResourceExternalView, partitionName);
    LeadControllerEntry leadControllerEntry =
        new LeadControllerEntry(leadControllerId, Collections.singletonList(tableName));
    leadControllerEntryMap.put(partitionName, leadControllerEntry);
    return new LeadControllerResponse(true, leadControllerEntryMap);
  }

  private ExternalView getLeadControllerResourceExternalView(HelixManager helixManager) {
    return helixManager.getClusterManagmentTool()
        .getResourceExternalView(helixManager.getClusterName(), Helix.LEAD_CONTROLLER_RESOURCE_NAME);
  }

  private String getParticipantInstanceIdFromExternalView(ExternalView leadControllerResourceExternalView,
      String partitionName) {
    if (leadControllerResourceExternalView == null) {
      LOGGER.warn("External view of lead controller resource is null!");
      return null;
    }
    Map<String, String> partitionStateMap = leadControllerResourceExternalView.getStateMap(partitionName);

    // Gets master host from partition map. Returns null if no master found.
    for (Map.Entry<String, String> entry : partitionStateMap.entrySet()) {
      if (MasterSlaveSMD.States.MASTER.name().equals(entry.getValue())) {
        // Found the controller in master state.
        // Converts participant id (with Prefix "Controller_") to controller id and assigns it as the leader,
        // since realtime segment completion protocol doesn't need the prefix in controller instance id.
        return entry.getKey();
      }
    }
    return null;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class LeadControllerEntry {

    @JsonProperty("leadControllerId")
    private String _leadControllerId;

    @JsonProperty("tableNames")
    private List<String> _tableNames;

    @JsonCreator
    public LeadControllerEntry(String leadControllerId, List<String> tableNames) {
      _leadControllerId = leadControllerId;
      _tableNames = tableNames;
    }

    @JsonProperty
    public String getLeadControllerId() {
      return _leadControllerId;
    }

    @JsonProperty
    public List<String> getTableNames() {
      return _tableNames;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class LeadControllerResponse {
    private boolean _isLeadControllerResourceEnabled;
    private Map<String, LeadControllerEntry> _leadControllerEntryMap;

    @JsonCreator
    public LeadControllerResponse(boolean isLeadControllerResourceEnabled,
        Map<String, LeadControllerEntry> leadControllerEntryMap) {
      _isLeadControllerResourceEnabled = isLeadControllerResourceEnabled;
      _leadControllerEntryMap = leadControllerEntryMap;
    }

    public boolean isLeadControllerResourceEnabled() {
      return _isLeadControllerResourceEnabled;
    }

    public Map<String, LeadControllerEntry> getLeadControllerEntryMap() {
      return _leadControllerEntryMap;
    }
  }
}
