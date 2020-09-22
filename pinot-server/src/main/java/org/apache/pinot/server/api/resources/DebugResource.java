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
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.core.upsert.UpsertMetadataTableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = "Debug")
@Path("/serverDebug")
public class DebugResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(DebugResource.class);

  @POST
  @Produces(MediaType.TEXT_PLAIN)
  @Path("/upsert/{enableQuery}")
  @ApiOperation(value = "Debugging upsert query", notes = "")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success", response = String.class),
      @ApiResponse(code = 500, message = "Internal server error"),
  })
  public void getUpsertDataAtOffset(
      @ApiParam(value = "enableQuery", required = true, example = "true") @PathParam("enableQuery") boolean enableQuery
  ) {
    UpsertMetadataTableManager.enableUpsertInQuery(enableQuery);
  }
}
