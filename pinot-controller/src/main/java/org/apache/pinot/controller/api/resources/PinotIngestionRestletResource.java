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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.net.URI;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.FileIngestionHelper;
import org.apache.pinot.controller.util.FileIngestionHelper.DataPayload;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * APIs related to ingestion
 */
@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class PinotIngestionRestletResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotIngestionRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  ControllerConf _controllerConf;

  /**
   * API to upload a file and ingest it into a Pinot table
   * @param tableName Name of the table to upload to
   * @param batchConfigMapStr Batch config Map as a string. Provide the
   *                          input format (inputFormat)
   *                          record reader configs (recordReader.prop.<property>),
   *                          fs class name (input.fs.className)
   *                          fs configs (input.fs.prop.<property>)
   * @param fileUpload file to upload as a multipart
   */
  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/ingestFromFile")
  @ApiOperation(value = "Ingest a file", notes = "Creates a segment using given file and pushes it to Pinot")
  public void ingestFromFile(
      @ApiParam(value = "Name of the table to upload the file to", required = true) @QueryParam("tableName") String tableName,
      @ApiParam(value = "Batch config map as string", required = true) @QueryParam("batchConfigMapStr") String batchConfigMapStr,
      FormDataMultiPart fileUpload,
      @Suspended final AsyncResponse asyncResponse) {
    try {
      asyncResponse.resume(ingestData(tableName, batchConfigMapStr, new DataPayload(fileUpload)));
    } catch (Exception e) {
      asyncResponse.resume(new ControllerApplicationException(LOGGER,
          String.format("Caught exception when ingesting file into table: %s. %s", tableName, e.getMessage()),
          Response.Status.INTERNAL_SERVER_ERROR, e));
    }
  }

  /**
   * API to ingest a file into Pinot from a URI
   * @param tableName Name of the table to upload to
   * @param batchConfigMapStr Batch config Map as a string. Provide the
   *                          input format (inputFormat)
   *                          record reader configs (recordReader.prop.<property>),
   *                          fs class name (input.fs.className)
   *                          fs configs (input.fs.prop.<property>)
   * @param sourceURIStr URI for input file to ingest
   */
  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/ingestFromURI")
  @ApiOperation(value = "Ingest from the given URI", notes = "Creates a segment using file at the given URI and pushes it to Pinot")
  public void ingestFromURI(
      @ApiParam(value = "Name of the table to upload the file to", required = true) @QueryParam("tableName") String tableName,
      @ApiParam(value = "Batch config map as string", required = true) @QueryParam("batchConfigMapStr") String batchConfigMapStr,
      @ApiParam(value = "URI", required = true) @QueryParam("sourceURIStr") String sourceURIStr,
      @Suspended final AsyncResponse asyncResponse) {
    try {
      asyncResponse.resume(ingestData(tableName, batchConfigMapStr, new DataPayload(new URI(sourceURIStr))));
    } catch (Exception e) {
      asyncResponse.resume(new ControllerApplicationException(LOGGER,
          String.format("Caught exception when ingesting file into table: %s. %s", tableName, e.getMessage()),
          Response.Status.INTERNAL_SERVER_ERROR, e));
    }
  }

  private SuccessResponse ingestData(String tableName, String batchConfigMapStr, DataPayload payload)
      throws Exception {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    Preconditions
        .checkState(TableType.REALTIME != tableType, "Cannot ingest file into REALTIME table: %s", tableName);
    String tableNameWithType = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(tableName);
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Table: %s not found", tableNameWithType);
    Map<String, String> batchConfigMap =
        JsonUtils.stringToObject(batchConfigMapStr, new TypeReference<Map<String, String>>() {});
    BatchConfig batchConfig = new BatchConfig(tableNameWithType, batchConfigMap);
    Schema schema = _pinotHelixResourceManager.getTableSchema(tableNameWithType);

    FileIngestionHelper fileIngestionHelper = new FileIngestionHelper(tableConfig, schema, batchConfig, _controllerConf);
    return fileIngestionHelper.buildSegmentAndPush(payload);
  }
}
