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
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessType;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = "Auth", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotControllerAuthResource {

  @Inject
  private AccessControlFactory _accessControlFactory;

  @Context
  HttpHeaders _httpHeaders;

  /**
   * Verify a token is both authenticated and authorized to perform an operation.
   *
   * @param tableName table name (optional)
   * @param accessType access type (optional)
   * @param endpointUrl endpoint url (optional)
   *
   * @return {@code true} if authenticated and authorized, {@code false} otherwise
   */
  @GET
  @Path("auth/verify")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Check whether authentication is enabled")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Verification result provided"),
      @ApiResponse(code = 500, message = "Verification error")
  })
  public boolean verify(@ApiParam(value = "Table name without type") @QueryParam("tableName") String tableName,
      @ApiParam(value = "API access type") @DefaultValue("READ") @QueryParam("accessType") AccessType accessType,
      @ApiParam(value = "Endpoint URL") @QueryParam("endpointUrl") String endpointUrl) {
    AccessControl accessControl = _accessControlFactory.create();
    return accessControl.hasAccess(tableName, accessType, _httpHeaders, endpointUrl);
  }

  /**
   * Provide the auth workflow configuration for the Pinot UI to perform user authentication. Currently supports NONE
   * (no auth) and BASIC (basic auth with username and password)
   *
   * @return auth workflow info/configuration
   */
  @GET
  @Path("auth/info")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Retrieve auth workflow info")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Auth workflow info provided")})
  public AccessControl.AuthWorkflowInfo info() {
    return _accessControlFactory.create().getAuthWorkflowInfo();
  }
}
