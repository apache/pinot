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

package org.apache.pinot.thirdeye.dashboard.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Singleton;
import org.apache.pinot.thirdeye.datalayer.bao.OnboardDatasetMetricManager;
import org.apache.pinot.thirdeye.datalayer.dto.OnboardDatasetMetricDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;

/**
 * Endpoints for adding datasets and metrics to be read by data sources
 */
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class OnboardDatasetMetricResource {

  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final OnboardDatasetMetricManager onboardDatasetMetricDAO = DAO_REGISTRY.getOnboardDatasetMetricDAO();


  public OnboardDatasetMetricResource() {
  }


  @GET
  @Path("/view/dataSource/{dataSource}/onboarded/{onboarded}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<OnboardDatasetMetricDTO> viewOnboardConfigsByDatasourceAndOnboarded(
      @PathParam("dataSource") String dataSource, @PathParam("onboarded") boolean onboarded) {
    List<OnboardDatasetMetricDTO> dtos = onboardDatasetMetricDAO.findByDataSourceAndOnboarded(dataSource, onboarded);
    return dtos;
  }


  /**
   * Create this by providing json payload as follows:
   *
   *    curl -H "Content-Type: application/json" -X POST -d <payload> <url>
   *    Eg: curl -H "Content-Type: application/json" -X POST -d
   *            '{"datasetName":"xyz","metricName":"xyz", "dataSource":"PinotThirdeyeDataSource", "properties": { "prop1":"1", "prop2":"2"}}'
   *                http://localhost:8080/onboard/create
   * @param payload
   */
  @POST
  @Path("/create")
  public Response createOnboardConfig(String payload) {
    OnboardDatasetMetricDTO onboardConfig = null;
    Response response = null;
    try {
      onboardConfig = OBJECT_MAPPER.readValue(payload, OnboardDatasetMetricDTO.class);
      Long id = onboardDatasetMetricDAO.save(onboardConfig);
      response = Response.status(Status.OK).entity(String.format("Created config with id %d", id)).build();
    } catch (Exception e) {
      response = Response.status(Status.INTERNAL_SERVER_ERROR)
          .entity(String.format("Invalid payload %s %s",  payload, e)).build();
    }
    return response;
  }


  @DELETE
  @Path("/delete/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteOnboardConfig(@PathParam("id") Long id) {
    Response response = Response.status(Status.NOT_FOUND).build();
    OnboardDatasetMetricDTO dto = onboardDatasetMetricDAO.findById(id);
    if (dto != null) {
      onboardDatasetMetricDAO.delete(dto);
      response = Response.ok().build();
    }
    return response;
  }

  @POST
  @Path("update/{id}/{onboarded}")
  public void toggleOnboarded(@PathParam("id") Long id, @PathParam("onboarded") boolean onboarded) {
    OnboardDatasetMetricDTO onboardConfig = onboardDatasetMetricDAO.findById(id);
    onboardConfig.setOnboarded(onboarded);
    onboardDatasetMetricDAO.update(onboardConfig);
  }

}
