/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import java.io.IOException;
import org.codehaus.jackson.map.ObjectMapper;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableSize extends BasePinotControllerRestletResource {
  private static Logger LOGGER = LoggerFactory.getLogger(TableSize.class);

  @Get
  @Override
  public Representation get() {
    final String tableName = (String) getRequest().getAttributes().get("tableName");
    final String detailedVal = (String) getRequest().getAttributes().get("detailed");
    final boolean detailed = ! "false".equalsIgnoreCase(detailedVal);

    if (tableName == null) {
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      StringRepresentation resp = new StringRepresentation("{\"error\" : \"Table name is required\"}");
      resp.setMediaType(MediaType.APPLICATION_JSON);
      return resp;
    }
    return getTableSizes(tableName, detailed);
  }

  @HttpVerb("get")
  @Summary("Get table storage size across all replicas")
  @Tags({"table"})
  @Paths({"/tables/{tableName}/size"})
  private Representation getTableSizes(
      @Parameter(name="tableName", in = "path", description = "Table name")
      String tableName,
      @Parameter(name="detailed", in="query",
      description = "true=Provide detailed size information; false=Only aggregated size",
      required = false)
      boolean detailed) {
    TableSizeReader tableSizeReader = new TableSizeReader(_executor, _connectionManager, _pinotHelixResourceManager);


    TableSizeReader.TableSizeDetails tableSizeDetails = null;
    try {
      tableSizeDetails = tableSizeReader.getTableSizeDetails(tableName,
          _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
    } catch (Throwable t) {
      LOGGER.error("Failed to read table size for: {}", tableName, t);
      return responseRepresentation(Status.SERVER_ERROR_INTERNAL,
          "{\"error\" : \"Error reading size information for "
          + tableName + "\"}");
    }

    if (tableSizeDetails == null) {
      return responseRepresentation(Status.CLIENT_ERROR_NOT_FOUND,
          "{\"error\" : \"Table " + tableName + " does not exist\" }");
    }

    try {
      return responseRepresentation(Status.SUCCESS_OK,
          new ObjectMapper().writeValueAsString(tableSizeDetails));
    } catch (IOException e) {
      LOGGER.error("Failed to convert table size to json for table: {}", tableName, e);
      return responseRepresentation(Status.SERVER_ERROR_INTERNAL,
          "{\"error\" : \"Error formatting table size\"}");
    }
  }

}
