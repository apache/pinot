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
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
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
import javax.ws.rs.core.MediaType;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.LEAD_CONTROLLER_TAG)
@Path("/leader")
public class PinotLeadControllerRestletResource {
  public static Logger LOGGER = LoggerFactory.getLogger(PinotLeadControllerRestletResource.class);

  @Inject
  private PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables")
  @ApiOperation(value = "Gets leaders for all tables", notes = "Gets leaders for all tables")
  public Map<String, LeadControllerResponse> getLeadersForAllTables() {
    Map<String, LeadControllerResponse> leadControllerResponses = new LinkedHashMap<>();
    HelixManager helixManager = _pinotHelixResourceManager.getHelixZkManager();
    // returns an empty map if lead controller resource is disabled.
    if (!LeadControllerUtils.isLeadControllerResourceEnabled(helixManager)) {
      return leadControllerResponses;
    }

    ExternalView leadControllerResourceExternalView = getLeadControllerResourceExternalView(helixManager);
    for (int partitionId = 0; partitionId < Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE; partitionId++) {
      String partitionName = LeadControllerUtils.generatePartitionName(partitionId);
      String participantInstanceId =
          getParticipantInstanceIdFromExternalView(leadControllerResourceExternalView, partitionName);
      if (participantInstanceId == null) {
        continue;
      }
      leadControllerResponses
          .putIfAbsent(partitionName, new LeadControllerResponse(participantInstanceId, new ArrayList<>()));
    }

    // Assign all the tables to the relevant partitions.
    List<String> tableNames = _pinotHelixResourceManager.getAllTables();
    for (String tableName : tableNames) {
      String rawTableName = TableNameBuilder.extractRawTableName(tableName);
      int partitionId = LeadControllerUtils.getPartitionIdForTable(rawTableName);
      String partitionName = LeadControllerUtils.generatePartitionName(partitionId);
      LeadControllerResponse leadControllerResponse = leadControllerResponses.get(partitionName);
      leadControllerResponse.getTableNames().add(tableName);
    }
    return leadControllerResponses;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}")
  @ApiOperation(value = "Gets leaders for all tables", notes = "Gets leaders for all tables")
  public Map<String, LeadControllerResponse> getLeadersForTable(
      @ApiParam(value = "Table name", required = true) @PathParam("tableName") String tableName) {
    Map<String, LeadControllerResponse> leadControllerResponses = new HashMap<>();
    HelixManager helixManager = _pinotHelixResourceManager.getHelixZkManager();
    // returns an empty map if lead controller resource is disabled.
    if (!LeadControllerUtils.isLeadControllerResourceEnabled(helixManager)) {
      return leadControllerResponses;
    }
    ExternalView leadControllerResourceExternalView = getLeadControllerResourceExternalView(helixManager);
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    int partitionId = LeadControllerUtils.getPartitionIdForTable(rawTableName);
    String partitionName = LeadControllerUtils.generatePartitionName(partitionId);
    String participantInstanceId =
        getParticipantInstanceIdFromExternalView(leadControllerResourceExternalView, partitionName);

    LeadControllerResponse leadControllerResponse =
        new LeadControllerResponse(participantInstanceId, Collections.singletonList(tableName));
    leadControllerResponses.put(partitionName, leadControllerResponse);
    return leadControllerResponses;
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

    // Get master host from partition map. Return null if no master found.
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
  public static class LeadControllerResponse {
    private String _leadControllerId;
    private List<String> _tableNames;

    @JsonCreator
    public LeadControllerResponse(String leadControllerId, List<String> tableNames) {
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
}
