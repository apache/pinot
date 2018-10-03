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

import com.alibaba.fastjson.JSONArray;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class PinotTableInstances {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableInstances.class);

  @Inject
  PinotHelixResourceManager pinotHelixResourceManager;

  @GET
  @Path("/tables/{tableName}/instances")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List table instances", notes = "List instances of the given table")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Table not found"),
      @ApiResponse(code = 500, message = "Internal server error")})
  public String getTableInstances(
      @ApiParam(value = "Table name without type", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Instance type", required = false, example = "broker", allowableValues = "BROKER, SERVER")
          @DefaultValue("") @QueryParam("type") String type
  ) {
    try {
      JSONObject ret = new JSONObject();
      ret.put("tableName", tableName);
      JSONArray brokers = new JSONArray();
      JSONArray servers = new JSONArray();

      if (type == null || type.isEmpty() || type.toLowerCase().equals("broker")) {
        if (pinotHelixResourceManager.hasOfflineTable(tableName)) {
          JSONObject e = new JSONObject();
          e.put("tableType", "offline");
          JSONArray a = new JSONArray();
          for (String ins : pinotHelixResourceManager.getBrokerInstancesForTable(tableName, TableType.OFFLINE)) {
            a.add(ins);
          }
          e.put("instances", a);
          brokers.add(e);
        }
        if (pinotHelixResourceManager.hasRealtimeTable(tableName)) {
          JSONObject e = new JSONObject();
          e.put("tableType", "realtime");
          JSONArray a = new JSONArray();
          for (String ins : pinotHelixResourceManager.getBrokerInstancesForTable(tableName, TableType.REALTIME)) {
            a.add(ins);
          }
          e.put("instances", a);
          brokers.add(e);
        }
      }

      if (type == null || type.isEmpty() || type.toLowerCase().equals("server")) {
        if (pinotHelixResourceManager.hasOfflineTable(tableName)) {
          JSONObject e = new JSONObject();
          e.put("tableType", "offline");
          JSONArray a = new JSONArray();
          for (String ins : pinotHelixResourceManager.getServerInstancesForTable(tableName, TableType.OFFLINE)) {
            a.add(ins);
          }
          e.put("instances", a);
          servers.add(e);
        }

        if (pinotHelixResourceManager.hasRealtimeTable(tableName)) {
          JSONObject e = new JSONObject();
          e.put("tableType", "realtime");
          JSONArray a = new JSONArray();
          for (String ins : pinotHelixResourceManager.getServerInstancesForTable(tableName, TableType.REALTIME)) {
            a.add(ins);
          }
          e.put("instances", a);
          servers.add(e);
        }
      }
      ret.put("brokers", brokers);
      ret.put("server", servers);   // Keeping compatibility with previous API, so "server" and "brokers"
      return ret.toString();
    } catch (JSONException e) {
      String errStr = "Error listing all table instances for table: " + tableName;
      throw new ControllerApplicationException(LOGGER, errStr, Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }
    private JSONObject getInstances(List<String> instanceList, TableType tableType)
      throws JSONException {
    JSONObject e = new JSONObject();
    // not sure how using enum toString will impact clients
    String typeStr = tableType==TableType.REALTIME ? "realtime" : "offline";
    e.put("tableType", typeStr);
    JSONArray a = new JSONArray();
    for (String ins : instanceList) {
      a.add(ins);
    }
    e.put("instances", a);
    return e;
  }
}
