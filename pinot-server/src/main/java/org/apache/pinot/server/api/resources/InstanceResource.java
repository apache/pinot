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

package org.apache.pinot.server.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.restlet.resources.DiskUsageInfo;
import org.apache.pinot.common.restlet.resources.ResourceUtils;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.server.api.AdminApiApplication;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * This resource API can be used to retrieve instance level information like instance tags.
 */
@Api(description = "Metadata for this instance (like tenant tags)", tags = "instance", authorizations =
    {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY,
    description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```")))
@Path("instance")
public class InstanceResource {
  @Inject
  @Named(AdminApiApplication.SERVER_INSTANCE_ID)
  private String _instanceId;
  @Inject
  private HelixManager _helixManager;

  @GET
  @Path("tags")
  @ApiOperation(value = "Tenant tags for current instance")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error")
  })
  @Produces(MediaType.APPLICATION_JSON)
  public List<String> getInstanceTags() {
    InstanceConfig config = HelixHelper.getInstanceConfig(_helixManager, _instanceId);
    if (config != null && config.getTags() != null) {
      return config.getTags();
    }
    return Collections.emptyList();
  }

  /**
   * Retrieve instance pools in the Helix InstanceConfig:
   * https://docs.pinot.apache.org/operators/operating-pinot/instance-assignment#pool-based-instance-assignment.
   * Returns an empty Map if poolBased config is not enabled or the instance is not assigned to any pool.
   */
  @GET
  @Path("pools")
  @ApiOperation(value = "Tenant pools for current instance")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error")
  })
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, String> getInstancePools() {
    InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_helixManager, _instanceId);
    if (instanceConfig == null || instanceConfig.getRecord() == null) {
      return Collections.emptyMap();
    }
    Map<String, String> pools = instanceConfig.getRecord().getMapField(InstanceUtils.POOL_KEY);
    return pools == null ? Collections.emptyMap() : pools;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/diskUtilization")
  @ApiOperation(value = "Show disk utilization", notes = "Disk capacity and usage shown in bytes")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Bad Request – Invalid disk utilization path in header")
  })
  public String getDiskUsageInfo(@Context HttpHeaders headers)
      throws WebApplicationException, IOException {
    String pathStr = headers.getHeaderString("diskUtilizationPath");
    if (StringUtils.isEmpty(pathStr)) {
      throw new WebApplicationException("Invalid disk utilization path in header", 400);
    }
    DiskUsageInfo diskUsageInfo = DiskUtilization.computeDiskUsage(_instanceId, pathStr);
    return ResourceUtils.convertToJsonString(diskUsageInfo);
  }
}
