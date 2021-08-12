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
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.Criteria;
import org.apache.helix.InstanceType;
import org.apache.pinot.common.messages.RunPeriodicTaskMessage;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.periodictask.PeriodicTaskScheduler;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.PERIODIC_TASK_TAG)
@Path("/periodictask")
public class PinotControllerPeriodicTaskRestletResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotControllerPeriodicTaskRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  PeriodicTaskScheduler _periodicTaskScheduler;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/run")
  @ApiOperation(value = "Run a periodic task against specified table. If no table name is specified, task will run against all tables.")
  public boolean runPeriodicTask(
      @ApiParam(value = "Periodic task name", required = true) @QueryParam("taskname") String periodicTaskName,
      @ApiParam(value = "Table name", required = false) @QueryParam("tablename") String tableName,
      @ApiParam(value = "Table type suffix", required = false, example = "OFFLINE | REALTIME", defaultValue = "OFFLINE") @QueryParam("tabletype") String tableType) {
    if (!_periodicTaskScheduler.hasTask(periodicTaskName)) {
      throw new WebApplicationException("Periodic task '" + periodicTaskName + "' not found.",
          Response.Status.NOT_FOUND);
    }

    if (!_pinotHelixResourceManager.getAllRawTables().contains(tableName)) {
      throw new WebApplicationException("Table '" + tableName + "' not found.", Response.Status.NOT_FOUND);
    }

    LOGGER.info("Sending periodic task execution message to all controllers for running task {} against {}.",
        periodicTaskName, tableName != null ? " table '" + tableName + "'" : "all tables");

    // Create and send message to send to all controllers (including this one)
    Criteria recipientCriteria = new Criteria();
    recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    recipientCriteria.setInstanceName("%");
    recipientCriteria.setSessionSpecific(true);
    recipientCriteria.setResource(CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME);
    recipientCriteria.setSelfExcluded(false);
    RunPeriodicTaskMessage runPeriodicTaskMessage = new RunPeriodicTaskMessage(periodicTaskName,
        (tableName != null && tableName.length() > 0) ? tableName + "_" + tableType : null);

    ClusterMessagingService clusterMessagingService =
        _pinotHelixResourceManager.getHelixZkManager().getMessagingService();
    int messageCount = clusterMessagingService.send(recipientCriteria, runPeriodicTaskMessage, null, -1);
    LOGGER.info("Periodic task execution message sent to {} controllers.", messageCount);
    return messageCount > 0;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/names")
  @ApiOperation(value = "Get comma-delimited list of all available periodic task names.")
  public List<String> getPeriodicTaskNames() {
    return _periodicTaskScheduler.getTaskNameList();
  }
}
