/*
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

package org.apache.pinot.thirdeye.api.application;

import com.google.inject.Inject;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.thirdeye.datalayer.bao.ApplicationManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.ApplicationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.detection.yaml.translator.SubscriptionConfigTranslator.*;


@Produces(MediaType.APPLICATION_JSON)
public class ApplicationResource {
  protected static final Logger LOG = LoggerFactory.getLogger(ApplicationResource.class);

  private final ApplicationManager appDAO;
  private final MergedAnomalyResultManager anomalyDAO;
  private final DetectionConfigManager detectionDAO;
  private final DetectionAlertConfigManager detectionAlertDAO;

  @Inject
  public ApplicationResource(ApplicationManager appDAO,
      MergedAnomalyResultManager anomalyDAO,
      DetectionConfigManager detectionDAO,
      DetectionAlertConfigManager detectionAlertDAO) {
    this.appDAO = appDAO;
    this.anomalyDAO = anomalyDAO;
    this.detectionDAO = detectionDAO;
    this.detectionAlertDAO = detectionAlertDAO;
  }

  @DELETE
  @Path("/delete/{application}")
  @ApiOperation(value = "Delete an application along with associated subscriptions, functions and anomalies")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response deleteApplication(
      @ApiParam(value = "Name of the application to delete")
      @PathParam("application") String application) throws Exception {
    Map<String, String> responseMessage = new HashMap<>();
    LOG.info("[APPLICATION] deleting application " + application);

    try {
      List<ApplicationDTO> appDTO = appDAO.findByPredicate(Predicate.EQ("application", application));
      if (appDTO == null || appDTO.isEmpty()) {
        responseMessage.put("message", "No application with name " + application + " was found.");
        return Response.status(Response.Status.BAD_REQUEST).entity(responseMessage).build();
      }

      List<DetectionAlertConfigDTO> subsGroupList = detectionAlertDAO.findByPredicate(Predicate.EQ("application", application));
      LOG.info("[APPLICATION] Found " + subsGroupList.size() + " subscription groups under application " + application);
      for (DetectionAlertConfigDTO subsGroup : subsGroupList) {
        for (long id : ConfigUtils.getLongs(subsGroup.getProperties().get(PROP_DETECTION_CONFIG_IDS))) {
          DetectionConfigDTO function = this.detectionDAO.findById(id);
          if (function != null) {
            List<MergedAnomalyResultDTO> anomalies =
                new ArrayList<>(anomalyDAO.findByPredicate(Predicate.EQ("detectionConfigId", function.getId())));
            for (MergedAnomalyResultDTO anomaly : anomalies) {
              anomalyDAO.delete(anomaly);
            }
            detectionDAO.delete(function);
            LOG.info("[APPLICATION] detection function " + function.getName() + " and all anomalies have been deleted.");
          }
        }
        detectionAlertDAO.delete(subsGroup);
        LOG.info("[APPLICATION] subscription group " + subsGroup.getName() + " deleted.");
      }
      appDAO.delete(appDTO.get(0));
    } catch (Exception e) {
      LOG.error("[APPLICATION] Error while deleting application " + application, e);
      responseMessage.put("message", "Error while deleting application. Error = " + e.getMessage());
      return Response.serverError().entity(responseMessage).build();
    }

    LOG.info("[APPLICATION] Successfully deleted application " + application);
    responseMessage.put("message", "Successfully deleted application.");
    return Response.ok().entity(responseMessage).build();
  }
}
