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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.introspect.CodehausJacksonIntrospector;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.ZOOKEEPER, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class ZookeeperResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperResource.class);

  // Helix uses codehaus.jackson.map.ObjectMapper, hence we can't use pinot JsonUtils here.
  @VisibleForTesting
  static final ObjectMapper MAPPER = (new ObjectMapper()).setAnnotationIntrospector(new CodehausJacksonIntrospector());

  static {
    // Configuration should be identical to org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer.

    MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
    MAPPER.enable(new MapperFeature[]{MapperFeature.AUTO_DETECT_FIELDS});
    MAPPER.enable(new MapperFeature[]{MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS});
    MAPPER.enable(new MapperFeature[]{MapperFeature.AUTO_DETECT_SETTERS});
    MAPPER.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
  }

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Path("/zk/get")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_ZNODE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get content of the znode")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "ZK Path not found"),
      @ApiResponse(code = 204, message = "No Content"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public String getData(
      @ApiParam(value = "Zookeeper Path, must start with /", required = true) @QueryParam("path") String path) {

    path = validateAndNormalizeZKPath(path, true);

    ZNRecord znRecord = _pinotHelixResourceManager.readZKData(path);
    if (znRecord != null) {
      try {
        return MAPPER.writeValueAsString(znRecord);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      // TODO: should throw Not found exception, but need to fix how UI interpret the error
      return null;
    }
  }

  @DELETE
  @Path("/zk/delete")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.DELETE_ZNODE)
  @Authenticate(AccessType.DELETE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Delete the znode at this path")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "ZK Path not found"),
      @ApiResponse(code = 204, message = "No Content"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public SuccessResponse delete(
      @ApiParam(value = "Zookeeper Path, must start with /", required = true) @QueryParam("path") String path) {

    path = validateAndNormalizeZKPath(path, true);

    boolean success = _pinotHelixResourceManager.deleteZKPath(path);
    if (success) {
      return new SuccessResponse("Successfully deleted path: " + path);
    } else {
      throw new ControllerApplicationException(LOGGER, "Failed to delete path: " + path,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  @PUT
  @Path("/zk/putChildren")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.UPDATE_ZNODE)
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update the content of multiple znRecord node under the same path")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "ZK Path not found"),
      @ApiResponse(code = 204, message = "No Content"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public SuccessResponse putChildren(
      @ApiParam(value = "Zookeeper path of parent, must start with /", required = true) @QueryParam("path") String path,
      @ApiParam(value = "Content") @QueryParam("data") @Nullable String data,
      @ApiParam(value = "expectedVersion", defaultValue = "-1") @QueryParam("expectedVersion") @DefaultValue("-1")
          int expectedVersion,
      @ApiParam(value = "accessOption", defaultValue = "1") @QueryParam("accessOption") @DefaultValue("1")
          int accessOption,
      @Nullable String payload) {

    path = validateAndNormalizeZKPath(path, false);

    if (StringUtils.isEmpty(data)) {
      data = payload;
    }
    if (StringUtils.isEmpty(data)) {
      throw new ControllerApplicationException(LOGGER, "Must provide data through query parameter or payload",
          Response.Status.BAD_REQUEST);
    }
    List<ZNRecord> znRecords;
    try {
      znRecords = MAPPER.readValue(data, new TypeReference<List<ZNRecord>>() { });
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Failed to deserialize the data", Response.Status.BAD_REQUEST,
          e);
    }

    for (ZNRecord znRecord : znRecords) {
      String childPath = path + "/" + znRecord.getId();
      try {
        boolean result = _pinotHelixResourceManager.setZKData(childPath, znRecord, expectedVersion, accessOption);
        if (!result) {
          throw new ControllerApplicationException(LOGGER, "Failed to update path: " + childPath,
              Response.Status.INTERNAL_SERVER_ERROR);
        }
      } catch (Exception e) {
        throw new ControllerApplicationException(LOGGER, "Failed to update path: " + childPath,
            Response.Status.INTERNAL_SERVER_ERROR, e);
      }
    }
    return new SuccessResponse("Successfully updated " + znRecords.size() + " ZnRecords under path: " + path);
  }

  @PUT
  @Path("/zk/put")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.UPDATE_ZNODE)
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update the content of the node")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "ZK Path not found"),
      @ApiResponse(code = 204, message = "No Content"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public SuccessResponse putData(
      @ApiParam(value = "Zookeeper Path, must start with /", required = true) @QueryParam("path") String path,
      @ApiParam(value = "Content") @QueryParam("data") @Nullable String data,
      @ApiParam(value = "expectedVersion", defaultValue = "-1") @QueryParam("expectedVersion") @DefaultValue("-1")
          int expectedVersion,
      @ApiParam(value = "accessOption", defaultValue = "1") @QueryParam("accessOption") @DefaultValue("1")
          int accessOption,
      @Nullable String payload) {

    path = validateAndNormalizeZKPath(path, false);

    if (StringUtils.isEmpty(data)) {
      data = payload;
    }
    if (StringUtils.isEmpty(data)) {
      throw new ControllerApplicationException(LOGGER, "Must provide data through query parameter or payload",
          Response.Status.BAD_REQUEST);
    }
    ZNRecord znRecord;
    try {
      znRecord = MAPPER.readValue(data, ZNRecord.class);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Failed to deserialize the data", Response.Status.BAD_REQUEST,
          e);
    }

    try {
      boolean result = _pinotHelixResourceManager.setZKData(path, znRecord, expectedVersion, accessOption);
      if (result) {
        return new SuccessResponse("Successfully updated path: " + path);
      } else {
        throw new ControllerApplicationException(LOGGER, "Failed to update path: " + path,
            Response.Status.INTERNAL_SERVER_ERROR);
      }
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Failed to update path: " + path,
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @POST
  @Path("/zk/create")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.UPDATE_ZNODE)
  @Authenticate(AccessType.CREATE)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Create a node at a given path")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 204, message = "No Content"),
      @ApiResponse(code = 400, message = "Bad Request"), @ApiResponse(code = 500, message = "Internal server error")
  })
  public SuccessResponse createNode(
      @ApiParam(value = "Zookeeper Path, must start with /", required = true) @QueryParam("path") String path,
      @ApiParam(value = "Content") @QueryParam("data") @Nullable String data,
      @ApiParam(value = "ttl", defaultValue = "-1") @QueryParam("ttl") @DefaultValue("-1") int ttl,
      @ApiParam(value = "accessOption", defaultValue = "1") @QueryParam("accessOption") @DefaultValue("1")
      int accessOption, @Nullable String payload) {

    path = validateAndNormalizeZKPath(path, false);

    if (StringUtils.isEmpty(data)) {
      data = payload;
    }
    if (StringUtils.isEmpty(data)) {
      throw new ControllerApplicationException(LOGGER, "Must provide data through query parameter or payload",
          Response.Status.BAD_REQUEST);
    }
    ZNRecord znRecord;
    try {
      znRecord = MAPPER.readValue(data, ZNRecord.class);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Failed to deserialize the data", Response.Status.BAD_REQUEST,
          e);
    }

    boolean result;
    try {
      result = _pinotHelixResourceManager.createZKNode(path, znRecord, accessOption, ttl);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, "Failed to create znode at path: " + path,
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    if (result) {
      return new SuccessResponse("Successfully updated path: " + path);
    } else {
      throw new ControllerApplicationException(LOGGER, "ZNode already exists at path: " + path,
          Response.Status.BAD_REQUEST);
    }
  }

  @GET
  @Path("/zk/ls")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_ZNODE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List the child znodes")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "ZK Path not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public String ls(
      @ApiParam(value = "Zookeeper Path, must start with /", required = true) @QueryParam("path") String path) {

    path = validateAndNormalizeZKPath(path, true);

    List<String> childNames = _pinotHelixResourceManager.getZKChildNames(path);
    try {
      return JsonUtils.objectToString(childNames);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @GET
  @Path("/zk/getChildren")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_ZNODE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get all child znodes")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "ZK Path not found"),
      @ApiResponse(code = 204, message = "No Content"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public String getChildren(
      @ApiParam(value = "Zookeeper Path, must start with /", required = true) @QueryParam("path") String path) {

    path = validateAndNormalizeZKPath(path, true);

    List<ZNRecord> znRecords = _pinotHelixResourceManager.getZKChildren(path);
    if (znRecords != null) {
      List<ZNRecord> nonNullRecords = new ArrayList<>(znRecords.size());
      for (ZNRecord znRecord : znRecords) {
        if (znRecord != null) {
          nonNullRecords.add(znRecord);
        }
      }
      try {
        return MAPPER.writeValueAsString(nonNullRecords);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new ControllerApplicationException(LOGGER, String.format("ZNRecord children %s not found", path),
          Response.Status.NOT_FOUND);
    }
  }

  @GET
  @Path("/zk/lsl")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_ZNODE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List the child znodes along with Stats")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "ZK Path not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public String lsl(
      @ApiParam(value = "Zookeeper Path, must start with /", required = true) @QueryParam("path") String path) {

    path = validateAndNormalizeZKPath(path, true);

    Map<String, Stat> childrenStats = _pinotHelixResourceManager.getZKChildrenStats(path);

    try {
      return JsonUtils.objectToString(childrenStats);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @GET
  @Path("/zk/stat")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_ZNODE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the stat",
      notes = " Use this api to fetch additional details of a znode such as creation time, modified time, numChildren"
          + " etc ")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Table not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public String stat(
      @ApiParam(value = "Zookeeper Path, must start with /", required = true) @QueryParam("path") String path) {

    path = validateAndNormalizeZKPath(path, true);

    Stat stat = _pinotHelixResourceManager.getZKStat(path);
    try {
      return JsonUtils.objectToString(stat);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private String validateAndNormalizeZKPath(String path, boolean shouldExist) {
    if (path == null) {
      throw new ControllerApplicationException(LOGGER, "ZKPath cannot be null", Response.Status.BAD_REQUEST);
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
    if (shouldExist && _pinotHelixResourceManager.getZKStat(path) == null) {
      throw new ControllerApplicationException(LOGGER, "ZKPath " + path + " does not exist", Response.Status.NOT_FOUND);
    }
    return path;
  }
}
