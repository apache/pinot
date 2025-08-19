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

import io.swagger.annotations.ApiOperation;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
public class PinotControllerTimeseriesResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotControllerTimeseriesResource.class);

  @Inject
  ControllerConf _controllerConf;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/timeseries/languages")
  @ApiOperation(value = "Get timeseries languages from controller configuration",
      notes = "Get timeseries languages from controller configuration")
  public List<String> getBrokerTimeSeriesLanguages(@Context HttpHeaders headers) {
    try {
      return _controllerConf.getTimeseriesLanguages();
    } catch (Exception e) {
      LOGGER.error("Error fetching timeseries languages from controller configuration", e);
      throw new ControllerApplicationException(LOGGER,
          "Error fetching timeseries languages from controller configuration: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
