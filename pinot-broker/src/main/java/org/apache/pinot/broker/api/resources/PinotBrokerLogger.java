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
package org.apache.pinot.broker.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.utils.LoggerUtils;
import org.apache.pinot.common.utils.log.DummyLogFileServer;
import org.apache.pinot.common.utils.log.LogFileServer;
import org.apache.pinot.core.auth.RBACAuthorization;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * Logger resource.
 */
@Api(tags = "Logger", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotBrokerLogger {

  @Inject
  private LogFileServer _logFileServer;

  @GET
  @Path("/loggers")
  @RBACAuthorization(targetType = "cluster", permission = "GetLoggers")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get all the loggers", notes = "Return all the logger names")
  public List<String> getLoggers() {
    return LoggerUtils.getAllLoggers();
  }

  @GET
  @Path("/loggers/{loggerName}")
  @RBACAuthorization(targetType = "cluster", permission = "GetLogger")
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
  @RBACAuthorization(targetType = "cluster", permission = "PutLogger")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Set logger level", notes = "Set logger level for a given logger")
  public Map<String, String> setLoggerLevel(@ApiParam(value = "Logger name") @PathParam("loggerName") String loggerName,
      @ApiParam(value = "Logger level") @QueryParam("level") String level) {
    return LoggerUtils.setLoggerLevel(loggerName, level);
  }

  @GET
  @Path("/loggers/files")
  @RBACAuthorization(targetType = "cluster", permission = "GetLogFiles")
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
  @RBACAuthorization(targetType = "cluster", permission = "GetLogFile")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @ApiOperation(value = "Download a log file")
  public Response downloadLogFile(
      @ApiParam(value = "Log file path", required = true) @QueryParam("filePath") String filePath) {
    if (_logFileServer == null || _logFileServer instanceof DummyLogFileServer) {
      throw new WebApplicationException("Root log directory is not configured",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return _logFileServer.downloadLogFile(filePath);
  }
}
