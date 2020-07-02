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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;


@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class PinotTableInstances {

  @Inject
  PinotHelixResourceManager pinotHelixResourceManager;

  @GET
  @Path("/tables/{tableName}/instances")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List table instances", notes = "List instances of the given table")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 404, message = "Table not found"), @ApiResponse(code = 500, message = "Internal server error")})
  public String getTableInstances(
      @ApiParam(value = "Table name without type", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Instance type", example = "broker", allowableValues = "BROKER, SERVER") @DefaultValue("") @QueryParam("type") String type) {
    ObjectNode ret = JsonUtils.newObjectNode();
    ret.put("tableName", tableName);
    ArrayNode brokers = JsonUtils.newArrayNode();
    ArrayNode servers = JsonUtils.newArrayNode();

    if (type == null || type.isEmpty() || type.toLowerCase().equals("broker")) {
      if (pinotHelixResourceManager.hasOfflineTable(tableName)) {
        ObjectNode e = JsonUtils.newObjectNode();
        e.put("tableType", "offline");
        ArrayNode a = JsonUtils.newArrayNode();
        for (String ins : pinotHelixResourceManager.getBrokerInstancesForTable(tableName, TableType.OFFLINE)) {
          a.add(ins);
        }
        e.set("instances", a);
        brokers.add(e);
      }
      if (pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        ObjectNode e = JsonUtils.newObjectNode();
        e.put("tableType", "realtime");
        ArrayNode a = JsonUtils.newArrayNode();
        for (String ins : pinotHelixResourceManager.getBrokerInstancesForTable(tableName, TableType.REALTIME)) {
          a.add(ins);
        }
        e.set("instances", a);
        brokers.add(e);
      }
    }

    if (type == null || type.isEmpty() || type.toLowerCase().equals("server")) {
      if (pinotHelixResourceManager.hasOfflineTable(tableName)) {
        ObjectNode e = JsonUtils.newObjectNode();
        e.put("tableType", "offline");
        ArrayNode a = JsonUtils.newArrayNode();
        for (String ins : pinotHelixResourceManager.getServerInstancesForTable(tableName, TableType.OFFLINE)) {
          a.add(ins);
        }
        e.set("instances", a);
        servers.add(e);
      }

      if (pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        ObjectNode e = JsonUtils.newObjectNode();
        e.put("tableType", "realtime");
        ArrayNode a = JsonUtils.newArrayNode();
        for (String ins : pinotHelixResourceManager.getServerInstancesForTable(tableName, TableType.REALTIME)) {
          a.add(ins);
        }
        e.set("instances", a);
        servers.add(e);
      }
    }
    ret.set("brokers", brokers);
    ret.set("server", servers);   // Keeping compatibility with previous API, so "server" and "brokers"
    return ret.toString();
  }
}
