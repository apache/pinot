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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Charsets;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.ZOOKEEPER)
@Path("/")
public class ZookeeperResource {

  public static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ZookeeperResource.class);

  @Inject
  PinotHelixResourceManager pinotHelixResourceManager;

  ZNRecordSerializer _znRecordSerializer = new ZNRecordSerializer();

  @GET
  @Path("/zk/get")
  @Produces(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Get content of the znode")
  @ApiResponses(value = { //
      @ApiResponse(code = 200, message = "Success"), //
      @ApiResponse(code = 404, message = "ZK Path not found"), //
      @ApiResponse(code = 204, message = "No Content"), //
      @ApiResponse(code = 500, message = "Internal server error")})
  public String getData(
      @ApiParam(value = "Zookeeper Path, must start with /", required = true, defaultValue = "/") @QueryParam("path") @DefaultValue("") String path) {

    path = validateAndNormalizeZKPath(path);

    ZNRecord znRecord = pinotHelixResourceManager.readZKData(path);
    if (znRecord != null) {
      return new String(_znRecordSerializer.serialize(znRecord), StandardCharsets.UTF_8);
    }
    return null;
  }

  @PUT
  @Path("/zk/put")
  @Produces(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Get content of the znode")
  @ApiResponses(value = { //
      @ApiResponse(code = 200, message = "Success"), //
      @ApiResponse(code = 404, message = "ZK Path not found"), //
      @ApiResponse(code = 204, message = "No Content"), //
      @ApiResponse(code = 500, message = "Internal server error")})
  public SuccessResponse putData(
      @ApiParam(value = "Zookeeper Path, must start with /", required = true, defaultValue = "/") @QueryParam("path") @DefaultValue("") String path,
      @ApiParam(value = "Content", required = true) @QueryParam("data") @DefaultValue("") String content,
      @ApiParam(value = "expectedVersion", required = true, defaultValue = "-1") @QueryParam("expectedVersion") @DefaultValue("-1") String expectedVersion,
      @ApiParam(value = "accessOption", required = true, defaultValue = "1") @QueryParam("accessOption") @DefaultValue("1") String accessOption) {
    path = validateAndNormalizeZKPath(path);
    ZNRecord record = null;
    if (content != null) {
      record = (ZNRecord) _znRecordSerializer.deserialize(content.getBytes(Charsets.UTF_8));
    }
    try {
      boolean result = pinotHelixResourceManager
          .setZKData(path, record, Integer.parseInt(expectedVersion), Integer.parseInt(accessOption));
      if (result) {
        return new SuccessResponse("Successfully Updated path: " + path);
      } else {
        throw new ControllerApplicationException(LOGGER, "Failed to update path: " + path,
            Response.Status.INTERNAL_SERVER_ERROR);
      }
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Failed to update path: " + path,
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @GET
  @Path("/zk/ls")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List the child znodes")
  @ApiResponses(value = { //
      @ApiResponse(code = 200, message = "Success"), //
      @ApiResponse(code = 404, message = "ZK Path not found"), //
      @ApiResponse(code = 500, message = "Internal server error")})
  public String ls(
      @ApiParam(value = "Zookeeper Path, must start with /", required = true, defaultValue = "/") @QueryParam("path") @DefaultValue("") String path) {

    path = validateAndNormalizeZKPath(path);

    List<String> children = pinotHelixResourceManager.getZKChildren(path);
    try {
      return JsonUtils.objectToString(children);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @GET
  @Path("/zk/lsl")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List the child znodes along with Stats")
  @ApiResponses(value = { //
      @ApiResponse(code = 200, message = "Success"), //
      @ApiResponse(code = 404, message = "ZK Path not found"), //
      @ApiResponse(code = 500, message = "Internal server error")})
  public String lsl(
      @ApiParam(value = "Zookeeper Path, must start with /", required = true, defaultValue = "/") @QueryParam("path") @DefaultValue("") String path) {

    path = validateAndNormalizeZKPath(path);

    Map<String, Stat> childrenStats = pinotHelixResourceManager.getZKChildrenStats(path);

    try {
      return JsonUtils.objectToString(childrenStats);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @GET
  @Path("/zk/stat")
  @Produces(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Get the stat", notes = " Use this api to fetch additional details of a znode such as creation time, modified time, numChildren etc ")
  @ApiResponses(value = { //
      @ApiResponse(code = 200, message = "Success"), //
      @ApiResponse(code = 404, message = "Table not found"), //
      @ApiResponse(code = 500, message = "Internal server error")})
  public String stat(
      @ApiParam(value = "Zookeeper Path, must start with /", required = true, defaultValue = "/") @QueryParam("path") @DefaultValue("") String path) {

    path = validateAndNormalizeZKPath(path);

    Stat stat = pinotHelixResourceManager.getZKStat(path);
    try {
      return JsonUtils.objectToString(stat);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private String validateAndNormalizeZKPath(
      @DefaultValue("") @QueryParam("path") @ApiParam(value = "Zookeeper Path, must start with /", required = false, defaultValue = "/") String path) {

    if (path == null || path.trim().isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "ZKPath " + path + " cannot be null or empty",
          Response.Status.BAD_REQUEST);
    }
    path = path.trim();
    if (!path.startsWith("/")) {
      throw new ControllerApplicationException(LOGGER, "ZKPath " + path + " must start with /",
          Response.Status.BAD_REQUEST);
    }
    if (!path.equals("/") && path.endsWith("/")) {
      throw new ControllerApplicationException(LOGGER, "ZKPath " + path + " cannot end with /",
          Response.Status.BAD_REQUEST);
    }

    if (!pinotHelixResourceManager.getHelixZkManager().getHelixDataAccessor().getBaseDataAccessor().exists(path, -1)) {
      throw new ControllerApplicationException(LOGGER, "ZKPath " + path + " does not exist:",
          Response.Status.NOT_FOUND);
    }
    return path;
  }
}
