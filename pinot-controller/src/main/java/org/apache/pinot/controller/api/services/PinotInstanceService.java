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
package org.apache.pinot.controller.api.services;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.model.Instances;
import org.apache.pinot.controller.api.resources.SuccessResponse;
import org.apache.pinot.spi.config.instance.Instance;


@Path("/")
public interface PinotInstanceService {

  @GET
  @Path("/instances")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List all instances")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  Instances getAllInstances();

  @GET
  @Path("/instances/{instanceName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get instance information", produces = MediaType.APPLICATION_JSON)
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Instance not found"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  String getInstance(
      @ApiParam(value = "Instance name", required = true, example = "Server_a.b.com_20000 | Broker_my.broker.com_30000")
      @PathParam("instanceName") String instanceName);

  @POST
  @Path("/instances")
  @Authenticate(AccessType.CREATE)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Create a new instance", consumes = MediaType.APPLICATION_JSON,
      notes = "Creates a new instance with given instance config")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 409, message = "Instance already exists"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  SuccessResponse addInstance(
      @ApiParam("Whether to update broker resource for broker instance") @QueryParam("updateBrokerResource")
      @DefaultValue("false") boolean updateBrokerResource, Instance instance);

  @POST
  @Path("/instances/{instanceName}/state")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Enable/disable/drop an instance", notes = "Enable/disable/drop an instance")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Bad Request"),
      @ApiResponse(code = 404, message = "Instance not found"),
      @ApiResponse(code = 409, message = "Instance cannot be dropped"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  SuccessResponse toggleInstanceState(
      @ApiParam(value = "Instance name", required = true, example = "Server_a.b.com_20000 | Broker_my.broker.com_30000")
      @PathParam("instanceName") String instanceName, String state);

  @DELETE
  @Path("/instances/{instanceName}")
  @Authenticate(AccessType.DELETE)
  @Consumes(MediaType.TEXT_PLAIN)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Drop an instance", notes = "Drop an instance")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Instance not found"),
      @ApiResponse(code = 409, message = "Instance cannot be dropped"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  SuccessResponse dropInstance(
      @ApiParam(value = "Instance name", required = true, example = "Server_a.b.com_20000 | Broker_my.broker.com_30000")
      @PathParam("instanceName") String instanceName);

  @PUT
  @Path("/instances/{instanceName}")
  @Authenticate(AccessType.UPDATE)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update the specified instance", consumes = MediaType.APPLICATION_JSON,
      notes = "Update specified instance with given instance config")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  SuccessResponse updateInstance(
      @ApiParam(value = "Instance name", required = true, example = "Server_a.b.com_20000 | Broker_my.broker.com_30000")
      @PathParam("instanceName") String instanceName,
      @ApiParam("Whether to update broker resource for broker instance") @QueryParam("updateBrokerResource")
      @DefaultValue("false") boolean updateBrokerResource, Instance instance);

  @PUT
  @Path("/instances/{instanceName}/updateTags")
  @Authenticate(AccessType.UPDATE)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update the tags of the specified instance", consumes = MediaType.APPLICATION_JSON,
      notes = "Update the tags of the specified instance")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Bad Request"),
      @ApiResponse(code = 404, message = "Instance not found"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  SuccessResponse updateInstanceTags(
      @ApiParam(value = "Instance name", required = true, example = "Server_a.b.com_20000 | Broker_my.broker.com_30000")
      @PathParam("instanceName") String instanceName,
      @ApiParam(value = "Comma separated tags list", required = true) @QueryParam("tags") String tags,
      @ApiParam("Whether to update broker resource for broker instance") @QueryParam("updateBrokerResource")
      @DefaultValue("false") boolean updateBrokerResource);

  @POST
  @Path("/instances/{instanceName}/updateBrokerResource")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update the tables served by the specified broker instance in the broker resource", notes =
      "Broker resource should be updated when a new broker instance is added, or the tags for an existing broker are "
          + "changed. Updating broker resource requires reading all the table configs, which can be costly for large "
          + "cluster. Consider updating broker resource for each table individually.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Bad Request"),
      @ApiResponse(code = 404, message = "Instance not found"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  SuccessResponse updateBrokerResource(
      @ApiParam(value = "Instance name", required = true, example = "Broker_my.broker.com_30000")
      @PathParam("instanceName") String instanceName);
}
