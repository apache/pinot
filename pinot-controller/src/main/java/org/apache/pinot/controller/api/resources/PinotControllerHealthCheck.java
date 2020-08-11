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
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.controller.ControllerConf;


@Api(tags = Constants.HEALTH_TAG)
@Path("/")
public class PinotControllerHealthCheck {

  @Inject
  ControllerConf controllerConf;

  @GET
  @Path("pinot-controller/admin")
  @ApiOperation(value = "Check controller health")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Good")})
  @Produces(MediaType.TEXT_PLAIN)
  public String checkHealthLegacy() {
    if (StringUtils.isNotBlank(controllerConf.generateVipUrl())) {
      return "GOOD";
    }
    return "";
  }

  @GET
  @Path("health")
  @ApiOperation(value = "Check controller health")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Good")})
  @Produces(MediaType.TEXT_PLAIN)
  public String checkHealth() {
    if (StringUtils.isNotBlank(controllerConf.generateVipUrl())) {
      return "GOOD";
    }
    return "";
  }
}
