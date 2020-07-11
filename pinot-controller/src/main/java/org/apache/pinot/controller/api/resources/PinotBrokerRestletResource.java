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

import com.google.common.collect.ImmutableList;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.BROKER_TAG)
@Path("/")
public class PinotBrokerRestletResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotBrokerRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/brokers")
  @ApiOperation(value = "List tenants and tables to brokers mappings", notes = "List tenants and tables to brokers mappings")
  public Map<String, Map<String, List<String>>> listBrokersMapping(
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state) {
    Map<String, Map<String, List<String>>> resultMap = new HashMap<>();
    resultMap.put("tenants", getTenantsToBrokersMapping(state));
    resultMap.put("tables", getTablesToBrokersMapping(state));
    return resultMap;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/brokers/tenants")
  @ApiOperation(value = "List tenants to brokers mappings", notes = "List tenants to brokers mappings")
  public Map<String, List<String>> getTenantsToBrokersMapping(
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state) {
    Map<String, List<String>> resultMap = new HashMap<>();
    _pinotHelixResourceManager.getAllBrokerTenantNames().stream()
        .forEach(tenant -> resultMap.put(tenant, getBrokersForTenant(tenant, state)));
    return resultMap;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/brokers/tenants/{tenantName}")
  @ApiOperation(value = "List brokers for a given tenant", notes = "List brokers for a given tenant")
  public List<String> getBrokersForTenant(
      @ApiParam(value = "Name of the tenant", required = true) @PathParam("tenantName") String tenantName,
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state) {
    Set<String> tenantBrokers = new HashSet<>();
    tenantBrokers.addAll(_pinotHelixResourceManager.getAllInstancesForBrokerTenant(tenantName));
    applyStateChanges(tenantBrokers, state);
    return ImmutableList.copyOf(tenantBrokers);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/brokers/tables")
  @ApiOperation(value = "List tables to brokers mappings", notes = "List tables to brokers mappings")
  public Map<String, List<String>> getTablesToBrokersMapping(
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state) {
    Map<String, List<String>> resultMap = new HashMap<>();
    _pinotHelixResourceManager.getAllTables().stream()
        .forEach(table -> resultMap.put(table, getBrokersForTable(table, state)));
    return resultMap;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/brokers/tables/{tableName}")
  @ApiOperation(value = "List brokers for a given table", notes = "List brokers for a given table")
  public List<String> getBrokersForTable(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "ONLINE|OFFLINE") @QueryParam("state") String state) {
    String actualTableName = _pinotHelixResourceManager.getActualTableName(tableName);
    Set<String> tableBrokers = new HashSet<>();
    if (_pinotHelixResourceManager.hasOfflineTable(actualTableName)) {
      tableBrokers.addAll(_pinotHelixResourceManager.getBrokerInstancesForTable(actualTableName, TableType.OFFLINE));
    }
    if (_pinotHelixResourceManager.hasRealtimeTable(actualTableName)) {
      tableBrokers.addAll(_pinotHelixResourceManager.getBrokerInstancesForTable(actualTableName, TableType.REALTIME));
    }
    applyStateChanges(tableBrokers, state);
    return ImmutableList.copyOf(tableBrokers);
  }

  private void applyStateChanges(Set<String> brokers, String state) {
    if (state == null) {
      return;
    }
    switch (state) {
      case "ONLINE":
        brokers.retainAll(_pinotHelixResourceManager.getOnlineInstanceList());
        break;
      case "OFFLINE":
        brokers.removeAll(_pinotHelixResourceManager.getOnlineInstanceList());
        break;
    }
  }
}
