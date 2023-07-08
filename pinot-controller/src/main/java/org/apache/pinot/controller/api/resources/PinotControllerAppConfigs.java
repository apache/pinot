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
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.common.utils.PinotAppConfigs;
import org.apache.pinot.controller.api.ControllerAdminApiApplication;
import org.apache.pinot.core.auth.RBACAuthorization;
import org.apache.pinot.spi.env.PinotConfiguration;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * Resource to get the application configs {@link PinotAppConfigs}
 * for the pinot controller.
 */
@Api(tags = Constants.APP_CONFIGS, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotControllerAppConfigs {

  @Context
  private Application _application;

  @GET
  @Path("/appconfigs")
  @RBACAuthorization(targetType = "cluster", permission = "GetAppConfigs")
  @Produces(MediaType.APPLICATION_JSON)
  public String getAppConfigs() {
    PinotConfiguration pinotConfiguration =
        (PinotConfiguration) _application.getProperties().get(ControllerAdminApiApplication.PINOT_CONFIGURATION);
    PinotAppConfigs pinotAppConfigs = new PinotAppConfigs(pinotConfiguration);
    return pinotAppConfigs.toJSONString();
  }
}
