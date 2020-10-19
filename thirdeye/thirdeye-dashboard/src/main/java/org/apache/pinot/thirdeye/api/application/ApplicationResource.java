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
import com.google.inject.Singleton;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
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

import static org.apache.pinot.thirdeye.dashboard.resources.ResourceUtils.ensureExists;
import static org.apache.pinot.thirdeye.detection.yaml.translator.SubscriptionConfigTranslator.*;


@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class ApplicationResource {

  protected static final Logger LOG = LoggerFactory.getLogger(ApplicationResource.class);

  private final ApplicationManager applicationManager;
  private final MergedAnomalyResultManager mergedAnomalyResultManager;
  private final DetectionConfigManager detectionConfigManager;
  private final DetectionAlertConfigManager detectionAlertConfigManager;

  @Inject
  public ApplicationResource(
      ApplicationManager applicationManager,
      MergedAnomalyResultManager mergedAnomalyResultManager,
      DetectionConfigManager detectionConfigManager,
      DetectionAlertConfigManager detectionAlertConfigManager) {
    this.applicationManager = applicationManager;
    this.mergedAnomalyResultManager = mergedAnomalyResultManager;
    this.detectionConfigManager = detectionConfigManager;
    this.detectionAlertConfigManager = detectionAlertConfigManager;
  }

  /**
   * This API helps reset an application. The reset operation cleans all objects related to the
   * application including alerts and anomalies providing a clean slate to play with.
   */
  @POST
  @Path("reset/{name}")
  public Response reset(
      @ApiParam(value = "Name of the application") @PathParam("name") String name
  ) {
    // Call also ensures that application exists and throws an error if it doesn't
    final ApplicationDTO application = getByName(name);

    deleteDependents(name);
    return Response.ok().build();
  }

  @DELETE
  @Path("/delete/{name}")
  @ApiOperation(value = "Delete an application along with associated subscriptions, functions and anomalies")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response deleteApplication(
      @ApiParam(value = "Name of the application") @PathParam("name") final String name
  ) {
    final ApplicationDTO application = getByName(name);
    LOG.debug("[APPLICATION] deleting application " + name);

    deleteDependents(name);
    applicationManager.delete(application);

    LOG.info("[APPLICATION] Successfully deleted application " + name);

    Map<String, String> responseMessage = new HashMap<>();
    responseMessage.put("message", "Successfully deleted application.");
    return Response.ok().entity(responseMessage).build();
  }

  /**
   * Get the first applicationDTO object by name. Throws error if it doesn't find any.
   *
   * @param name name of the application
   * @return an {@link ApplicationDTO} object
   */
  private ApplicationDTO getByName(final String name) {
    final List<ApplicationDTO> list = applicationManager
        .findByPredicate(Predicate.EQ("application", name));

    return ensureExists(Optional.of(list)
        .map(Collection::stream)
        .flatMap(Stream::findFirst)
        .orElse(null), "Application not found: " + name);
  }

  private void deleteDependents(final String application) {
    List<DetectionAlertConfigDTO> subsGroupList = detectionAlertConfigManager.findByPredicate(
        Predicate.EQ("application", application));
    LOG.debug(String.format("[APPLICATION] Found %d subscription groups under application %s",
        subsGroupList.size(), application));

    for (DetectionAlertConfigDTO subsGroup : subsGroupList) {
      for (long id : ConfigUtils
          .getLongs(subsGroup.getProperties().get(PROP_DETECTION_CONFIG_IDS))) {
        DetectionConfigDTO function = this.detectionConfigManager.findById(id);
        if (function != null) {
          List<MergedAnomalyResultDTO> anomalies =
              new ArrayList<>(
                  mergedAnomalyResultManager
                      .findByPredicate(Predicate.EQ("detectionConfigId", function.getId())));
          anomalies.forEach(mergedAnomalyResultManager::delete);
          detectionConfigManager.delete(function);
          LOG.debug("[APPLICATION] detection function " + function.getName()
              + " and all anomalies have been deleted.");
        }
      }
      detectionAlertConfigManager.delete(subsGroup);
      LOG.debug("[APPLICATION] subscription group " + subsGroup.getName() + " deleted.");
    }
  }
}
