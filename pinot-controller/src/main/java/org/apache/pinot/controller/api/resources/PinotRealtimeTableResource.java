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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.Criteria;
import org.apache.helix.InstanceType;
import org.apache.pinot.common.messages.RunPeriodicTaskMessage;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.validation.RealtimeSegmentValidationManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class PinotRealtimeTableResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotRealtimeTableResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @POST
  @Path("/tables/{tableName}/resumeConsumption")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Resume the consumption of a realtime table",
      notes = "Resume the consumption of a realtime table")
  public String resumeConsumption(
      @ApiParam(value = "Name of the table", required = true)
      @PathParam("tableName") String tableName) throws JsonProcessingException {
    // TODO: Add util method for invoking periodic tasks
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    // Generate an id for this request by taking first eight characters of a randomly generated UUID. This request id
    // is returned to the user and also appended to log messages so that user can locate all log messages associated
    // with this PeriodicTask's execution.
    String periodicTaskRequestId = "api-" + UUID.randomUUID().toString().substring(0, 8);

    // Create and send message to send to all controllers (including this one)
    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setSessionSpecific(true);
    recipientCriteria.setResource(CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME);
    recipientCriteria.setSelfExcluded(false);
    Map<String, String> taskProperties = new HashMap<>();
    taskProperties.put(RealtimeSegmentValidationManager.RECREATE_DELETED_CONSUMING_SEGMENT_KEY, "true");

    RunPeriodicTaskMessage runPeriodicTaskMessage =
        new RunPeriodicTaskMessage(periodicTaskRequestId, Constants.REALTIME_SEGMENT_VALIDATION_MANAGER,
            tableNameWithType, taskProperties);

    LOGGER.info("[TaskRequestId: {}] Sending periodic task message to all controllers for running task {} against {},"
            + " with properties {}.", periodicTaskRequestId, Constants.REALTIME_SEGMENT_VALIDATION_MANAGER,
        " table '" + tableNameWithType + "'", taskProperties);

    ClusterMessagingService clusterMessagingService =
        _pinotHelixResourceManager.getHelixZkManager().getMessagingService();
    int messageCount = clusterMessagingService.send(recipientCriteria, runPeriodicTaskMessage, null, -1);
    LOGGER.info("[TaskRequestId: {}] Periodic task execution message sent to {} controllers.", periodicTaskRequestId,
        messageCount);

    return "{\"Log Request Id\": \"" + periodicTaskRequestId + "\",\"Controllers notified\":" + (messageCount > 0)
        + "}";
  }
}
