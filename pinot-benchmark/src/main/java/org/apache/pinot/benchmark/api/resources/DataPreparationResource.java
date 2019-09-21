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
package org.apache.pinot.benchmark.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.benchmark.api.data.DataPreparationManager;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = "Data Preparation")
@Path("/")
public class DataPreparationResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(DataPreparationManager.class);

  @Inject
  private DataPreparationManager _dataPreparationManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/data/tables/{tableName}/segments")
  public String listSegments(@ApiParam(value = "Table name", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "perf|prod") @QueryParam("cluster") String clusterType) {
    return _dataPreparationManager.listSegments(clusterType, tableName);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/data/segments")
  public String uploadSegments(FormDataMultiPart multiPart) {
    return _dataPreparationManager.uploadSegment(multiPart);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/data/tables/{tableName}/segments")
  public String convertAndUploadSegments(
      @ApiParam(value = "Table name", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "upload|convert,upload") @QueryParam("actions") String actions, FormDataMultiPart multiPart) {
    return _dataPreparationManager.convertAndUploadSegment(tableName, multiPart, actions);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/data/mirror")
  public SuccessResponse copyConvertAndUploadSegments(
      @ApiParam(value = "originTableName") @QueryParam("from") String originTableName,
      @ApiParam(value = "targetTableName") @QueryParam("to") String targetTableName,
      @ApiParam(value = "diffOnly") @QueryParam("diffOnly") @DefaultValue("false") boolean diffOnly) {
    return _dataPreparationManager.copyConvertAndUploadSegments(originTableName, targetTableName, diffOnly);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/data/queries")
  public SuccessResponse uploadQueries(@ApiParam(value = "originTableName") @QueryParam("from") String originTableName,
      @ApiParam(value = "targetTableName") @QueryParam("to") String targetTableName, FormDataMultiPart multiPart) {
    return _dataPreparationManager.uploadQueries(originTableName, targetTableName, multiPart);
  }
}
