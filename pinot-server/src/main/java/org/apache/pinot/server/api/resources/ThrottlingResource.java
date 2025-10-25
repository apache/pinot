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
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.core.query.scheduler.ThrottlingRuntime;


@Singleton
@Path("/throttling")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "throttling")
public class ThrottlingResource {

  @GET
  @Path("/state")
  @ApiOperation(value = "Get current throttling state", notes = "Returns current concurrency limit and config flags")
  @ApiResponses({@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Error")})
  public Response getState() {
    Map<String, Object> state = new HashMap<>();
    state.put("pauseOnAlarm", ThrottlingRuntime.isPauseOnAlarm());
    state.put("alarmMaxConcurrent", ThrottlingRuntime.getAlarmMaxConcurrent());
    state.put("normalMaxConcurrent", ThrottlingRuntime.getNormalMaxConcurrent());
    state.put("defaultConcurrency", ThrottlingRuntime.getDefaultConcurrency());
    state.put("currentConcurrencyLimit", ThrottlingRuntime.getCurrentLimit());
    return Response.ok(state).build();
  }

  public static class ConcurrencyUpdate {
    public int _limit;
  }

  @POST
  @Path("/setLimit")
  @ApiOperation(value = "Set current concurrency limit", notes = "Sets the current scheduler concurrency limit")
  @ApiResponses({@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 400, message = "Bad request")})
  public Response setLimit(ConcurrencyUpdate update) {
    if (update == null || update._limit <= 0) {
      return Response.status(Response.Status.BAD_REQUEST).entity("limit must be > 0").build();
    }
    ThrottlingRuntime.setCurrentLimit(update._limit);
    Map<String, Object> result = new HashMap<>();
    result.put("currentConcurrencyLimit", ThrottlingRuntime.getCurrentLimit());
    return Response.ok(result).build();
  }
}
