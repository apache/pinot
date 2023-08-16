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
package org.apache.pinot.broker.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.broker.BrokerAdminApiApplication;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * This resource API can be used to retrieve instance level information like instance tags.
 */
@Api(tags = "Instance", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class InstanceResource {

  @Inject
  @Named(BrokerAdminApiApplication.BROKER_INSTANCE_ID)
  private String _instanceId;

  @Inject
  private HelixManager _helixManager;

  @GET
  @Path("/instance/tags")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_INSTANCE)
  @ApiOperation(value = "Tenant tags for current instance")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  @Produces(MediaType.APPLICATION_JSON)
  public List<String> getInstanceTags() {
    InstanceConfig config = HelixHelper.getInstanceConfig(_helixManager, _instanceId);
    if (config != null && config.getTags() != null) {
      return config.getTags();
    }
    return Collections.emptyList();
  }
}
