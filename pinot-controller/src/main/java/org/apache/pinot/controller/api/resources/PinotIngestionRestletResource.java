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
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.FileIngestionHelper;
import org.apache.pinot.controller.util.FileIngestionHelper.DataPayload;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * APIs related to ingestion
 *
 * Ingest data into the tableNameWithType using the form multipart file
 * /ingestFromFile?tableNameWithType=foo_OFFLINE
 * &batchConfigMapStr={
 *   "inputFormat":"csv",
 *   "recordReader.prop.delimiter":"|"
 * }
 *
 * Ingest data into the tableNameWithType using the source file URI
 * /ingestFromURI?tableNameWithType=foo_OFFLINE
 * &batchConfigMapStr={
 *   "inputFormat":"json",
 *   "input.fs.className":"org.apache.pinot.plugin.filesystem.S3PinotFS",
 *   "input.fs.prop.region":"us-central",
 *   "input.fs.prop.accessKey":"foo",
 *   "input.fs.prop.secretKey":"bar"
 * }
 * &sourceURIStr=s3://test.bucket/path/to/json/data/data.json
 *
 */
@Api(tags = Constants.TABLE_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")}))
@Path("/")
public class PinotIngestionRestletResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotIngestionRestletResource.class);
  // directory to use under the controller local temp dir. Controller config can be added for this later if needed.
  private static final String INGESTION_DIR = "ingestion_dir";
  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  ControllerConf _controllerConf;

  /**
   * API to upload a file and ingest it into a Pinot table.
   * This call will copy the file locally, create a segment and push the segment to Pinot.
   * A response will be returned after the completion of all of the above steps.
   * All steps happen on the controller. This API is NOT meant for production environments/large input files.
   * For Production setup, use the minion batch ingestion mechanism
   *
   * @param tableNameWithType Name of the table to upload to, with type suffix
   * @param batchConfigMapStr Batch config Map as a string. Provide the
   *                          input format (inputFormat)
   *                          record reader configs (recordReader.prop.<property>),
   *                          fs class name (input.fs.className)
   *                          fs configs (input.fs.prop.<property>)
   * @param fileUpload file to upload as a multipart
   * @param asyncResponse injected async response to return result
   */
  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/ingestFromFile")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.INGEST_FILE)
  @Authenticate(AccessType.CREATE)
  @ApiOperation(value = "Ingest a file", notes = "Creates a segment using given file and pushes it to Pinot. "
      + "\n All steps happen on the controller. This API is NOT meant for production environments/large input files. "
      + "\n Example usage (query params need encoding):" + "\n```"
      + "\ncurl -X POST -F file=@data.json -H \"Content-Type: multipart/form-data\" "
      + "\"http://localhost:9000/ingestFromFile?tableNameWithType=foo_OFFLINE&" + "\nbatchConfigMapStr={"
      + "\n  \"inputFormat\":\"csv\"," + "\n  \"recordReader.prop.delimiter\":\"|\"" + "\n}\" " + "\n```")
  public void ingestFromFile(
      @ApiParam(value = "Name of the table to upload the file to", required = true) @QueryParam("tableNameWithType")
          String tableNameWithType, @ApiParam(value =
      "Batch config Map as json string. Must pass inputFormat, and optionally record reader properties. e.g. "
          + "{\"inputFormat\":\"json\"}", required = true) @QueryParam("batchConfigMapStr") String batchConfigMapStr,
      FormDataMultiPart fileUpload, @Suspended final AsyncResponse asyncResponse, @Context HttpHeaders headers) {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, headers);
    try {
      asyncResponse.resume(ingestData(tableNameWithType, batchConfigMapStr, new DataPayload(fileUpload)));
    } catch (IllegalArgumentException e) {
      asyncResponse.resume(new ControllerApplicationException(LOGGER, String
          .format("Got illegal argument when ingesting file into table: %s. %s", tableNameWithType, e.getMessage()),
          Response.Status.BAD_REQUEST, e));
    } catch (Exception e) {
      asyncResponse.resume(new ControllerApplicationException(LOGGER,
          String.format("Caught exception when ingesting file into table: %s. %s", tableNameWithType, e.getMessage()),
          Response.Status.INTERNAL_SERVER_ERROR, e));
    }
  }

  /**
   * API to ingest a file into Pinot from a URI.
   * This call will copy the file locally, create a segment and push the segment to Pinot.
   * A response will be returned after the completion of all of the above steps.
   *
   * @param tableNameWithType Name of the table to upload to, with type suffix
   * @param batchConfigMapStr Batch config Map as a string. Provide the
   *                          input format (inputFormat)
   *                          record reader configs (recordReader.prop.<property>),
   *                          fs class name (input.fs.className)
   *                          fs configs (input.fs.prop.<property>)
   * @param sourceURIStr URI for input file to ingest
   * @param asyncResponse injected async response to return result
   */
  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/ingestFromURI")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.INGEST_FILE)
  @Authenticate(AccessType.CREATE)
  @ApiOperation(value = "Ingest from the given URI", notes =
      "Creates a segment using file at the given URI and pushes it to Pinot. "
          + "\n All steps happen on the controller. This API is NOT meant for production environments/large input "
          + "files. " + "\nExample usage (query params need encoding):" + "\n```"
          + "\ncurl -X POST \"http://localhost:9000/ingestFromURI?tableNameWithType=foo_OFFLINE"
          + "\n&batchConfigMapStr={" + "\n  \"inputFormat\":\"json\","
          + "\n  \"input.fs.className\":\"org.apache.pinot.plugin.filesystem.S3PinotFS\","
          + "\n  \"input.fs.prop.region\":\"us-central\"," + "\n  \"input.fs.prop.accessKey\":\"foo\","
          + "\n  \"input.fs.prop.secretKey\":\"bar\"" + "\n}"
          + "\n&sourceURIStr=s3://test.bucket/path/to/json/data/data.json\"" + "\n```")
  public void ingestFromURI(
      @ApiParam(value = "Name of the table to upload the file to", required = true) @QueryParam("tableNameWithType")
          String tableNameWithType, @ApiParam(value =
      "Batch config Map as json string. Must pass inputFormat, and optionally input FS properties. e.g. "
          + "{\"inputFormat\":\"json\"}", required = true) @QueryParam("batchConfigMapStr") String batchConfigMapStr,
      @ApiParam(value = "URI of file to upload", required = true) @QueryParam("sourceURIStr") String sourceURIStr,
      @Suspended final AsyncResponse asyncResponse, @Context HttpHeaders headers) {
    tableNameWithType = DatabaseUtils.translateTableName(tableNameWithType, headers);
    try {
      asyncResponse.resume(ingestData(tableNameWithType, batchConfigMapStr, new DataPayload(new URI(sourceURIStr))));
    } catch (IllegalArgumentException e) {
      asyncResponse.resume(new ControllerApplicationException(LOGGER, String
          .format("Got illegal argument when ingesting file into table: %s. %s", tableNameWithType, e.getMessage()),
          Response.Status.BAD_REQUEST, e));
    } catch (Exception e) {
      asyncResponse.resume(new ControllerApplicationException(LOGGER,
          String.format("Caught exception when ingesting file into table: %s. %s", tableNameWithType, e.getMessage()),
          Response.Status.INTERNAL_SERVER_ERROR, e));
    }
  }

  private SuccessResponse ingestData(String tableNameWithType, String batchConfigMapStr, DataPayload payload)
      throws Exception {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    Preconditions
        .checkState(tableType != null, "Must provide table name with type suffix for table: %s", tableNameWithType);
    Preconditions
        .checkState(TableType.REALTIME != tableType, "Cannot ingest file into REALTIME table: %s", tableNameWithType);
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Table: %s not found", tableNameWithType);
    Map<String, String> batchConfigMap =
        JsonUtils.stringToObject(batchConfigMapStr, new TypeReference<Map<String, String>>() {
        });
    Schema schema = _pinotHelixResourceManager.getTableSchema(tableNameWithType);

    AuthProvider authProvider = AuthProviderUtils.extractAuthProvider(_controllerConf,
        CommonConstants.Controller.PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY + ".auth");

    FileIngestionHelper fileIngestionHelper =
        new FileIngestionHelper(tableConfig, schema, batchConfigMap, getControllerUri(),
            new File(_controllerConf.getLocalTempDir(), INGESTION_DIR), authProvider);
    return fileIngestionHelper.buildSegmentAndPush(payload);
  }

  private URI getControllerUri() {
    try {
      return new URI(_controllerConf.generateVipUrl());
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Controller VIP uri is invalid", e);
    }
  }
}
