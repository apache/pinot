/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.api.resources;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang.StringUtils;
import com.linkedin.pinot.controller.ControllerConf;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Api(tags = Constants.HEALTH_TAG)
@Path("/pinot-controller/admin")
public class PinotControllerHealthCheck {

  @Inject
  ControllerConf controllerConf;

  @GET
  @Path("/")
  @ApiOperation(value = "Check controller health")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Good")})
  @Produces(MediaType.TEXT_PLAIN)
  public String checkHealth(
  ) {
    if (StringUtils.isNotBlank(controllerConf.generateVipUrl())) {
      return "GOOD";
    }
    return "";
  }
}
