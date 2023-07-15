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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.LoggerUtils;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.common.utils.log.DummyLogFileServer;
import org.apache.pinot.common.utils.log.LogFileServer;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * Logger resource.
 */
@Api(tags = "Logger", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotControllerLogger {

  private final FileUploadDownloadClient _fileUploadDownloadClient = new FileUploadDownloadClient();

  @Inject
  private LogFileServer _logFileServer;

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Path("/loggers")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_LOGGERS)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get all the loggers", notes = "Return all the logger names")
  public List<String> getLoggers() {
    return LoggerUtils.getAllLoggers();
  }

  @GET
  @Path("/loggers/{loggerName}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_LOGGER)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get logger configs", notes = "Return logger info")
  public Map<String, String> getLogger(
      @ApiParam(value = "Logger name", required = true) @PathParam("loggerName") String loggerName) {
    Map<String, String> loggerInfo = LoggerUtils.getLoggerInfo(loggerName);
    if (loggerInfo == null) {
      throw new WebApplicationException(String.format("Logger %s not found", loggerName), Response.Status.NOT_FOUND);
    }
    return loggerInfo;
  }

  @PUT
  @Path("/loggers/{loggerName}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.SET_LOGGER)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Set logger level", notes = "Set logger level for a given logger")
  public Map<String, String> setLoggerLevel(@ApiParam(value = "Logger name") @PathParam("loggerName") String loggerName,
      @ApiParam(value = "Logger level") @QueryParam("level") String level) {
    return LoggerUtils.setLoggerLevel(loggerName, level);
  }

  @GET
  @Path("/loggers/files")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_LOG_FILES)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get all local log files")
  public Set<String> getLocalLogFiles() {
    try {
      if (_logFileServer == null || _logFileServer instanceof DummyLogFileServer) {
        throw new WebApplicationException("Root log directory doesn't exist", Response.Status.INTERNAL_SERVER_ERROR);
      }
      return _logFileServer.getAllLogFilePaths();
    } catch (IOException e) {
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/loggers/download")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_LOG_FILE)
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Download a log file")
  public Response downloadLogFile(
      @ApiParam(value = "Log file path", required = true) @QueryParam("filePath") String filePath) {
    if (_logFileServer == null || _logFileServer instanceof DummyLogFileServer) {
      throw new WebApplicationException("Root log directory is not configured",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return _logFileServer.downloadLogFile(filePath);
  }

  @GET
  @Path("/loggers/instances")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_LOG_FILES)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Collect log files from all the instances")
  public Map<String, Set<String>> getLogFilesFromAllInstances(
      @HeaderParam(HttpHeaders.AUTHORIZATION) String authorization) {
    if (_logFileServer == null || _logFileServer instanceof DummyLogFileServer) {
      throw new WebApplicationException("Root directory doesn't exist", Response.Status.INTERNAL_SERVER_ERROR);
    }
    Map<String, Set<String>> instancesToLogFilesMap = new HashMap<>();
    List<String> onlineInstanceList = _pinotHelixResourceManager.getOnlineInstanceList();
    onlineInstanceList.forEach(
        instance -> {
          try {
            instancesToLogFilesMap.put(instance, getLogFilesFromInstance(authorization, instance));
          } catch (Exception e) {
            // Skip the instance for any exception.
          }
        });
    return instancesToLogFilesMap;
  }

  @GET
  @Path("/loggers/instances/{instanceName}")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_LOG_FILES)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Collect log files from a given instance")
  public Set<String> getLogFilesFromInstance(
      @HeaderParam(HttpHeaders.AUTHORIZATION) String authorization,
      @ApiParam(value = "Instance Name", required = true) @PathParam("instanceName") String instanceName) {
    try {
      URI uri = new URI(getInstanceBaseUri(instanceName) + "/loggers/files");
      Map<String, String> headers = new HashMap<>();
      if (authorization != null) {
        headers.put(HttpHeaders.AUTHORIZATION, authorization);
      }
      SimpleHttpResponse simpleHttpResponse = _fileUploadDownloadClient.getHttpClient().sendGetRequest(uri, headers);
      if (simpleHttpResponse.getStatusCode() >= 400) {
        throw new WebApplicationException("Failed to fetch logs from instance name: " + instanceName,
            Response.Status.fromStatusCode(simpleHttpResponse.getStatusCode()));
      }
      String responseString = simpleHttpResponse.getResponse();
      responseString = responseString.substring(1, responseString.length() - 1).replace("\"", "");
      return new HashSet<>(Arrays.asList(responseString.split(",")));
    } catch (IOException | URISyntaxException e) {
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/loggers/instances/{instanceName}/download")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_LOG_FILE)
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Authenticate(AccessType.DELETE)
  @ApiOperation(value = "Download a log file from a given instance")
  public Response downloadLogFileFromInstance(
      @HeaderParam(HttpHeaders.AUTHORIZATION) String authorization,
      @ApiParam(value = "Instance Name", required = true) @PathParam("instanceName") String instanceName,
      @ApiParam(value = "Log file path", required = true) @QueryParam("filePath") String filePath,
      @Context Map<String, String> headers) {
    try {
      URI uri = UriBuilder.fromUri(getInstanceBaseUri(instanceName)).path("/loggers/download")
          .queryParam("filePath", filePath).build();
      RequestBuilder requestBuilder = RequestBuilder.get(uri).setVersion(HttpVersion.HTTP_1_1);
      if (MapUtils.isNotEmpty(headers)) {
        for (Map.Entry<String, String> header : headers.entrySet()) {
          requestBuilder.addHeader(header.getKey(), header.getValue());
        }
      }
      if (authorization != null) {
        requestBuilder.addHeader(HttpHeaders.AUTHORIZATION, authorization);
      }
      CloseableHttpResponse httpResponse = _fileUploadDownloadClient.getHttpClient().execute(requestBuilder.build());
      if (httpResponse.getStatusLine().getStatusCode() >= 400) {
        throw new WebApplicationException(IOUtils.toString(httpResponse.getEntity().getContent(), "UTF-8"),
            Response.Status.fromStatusCode(httpResponse.getStatusLine().getStatusCode()));
      }
      Response.ResponseBuilder builder = Response.ok();
      builder.entity(httpResponse.getEntity().getContent());
      builder.contentLocation(uri);
      builder.header(HttpHeaders.CONTENT_LENGTH, httpResponse.getEntity().getContentLength());
      return builder.build();
    } catch (IOException e) {
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private String getInstanceBaseUri(String instanceName) {
    return InstanceUtils.getInstanceBaseUri(_pinotHelixResourceManager.getHelixInstanceConfig(instanceName));
  }
}
