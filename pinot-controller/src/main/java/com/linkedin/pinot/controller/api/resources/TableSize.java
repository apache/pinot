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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.upload.batch.SegmentsInBatch;
import com.linkedin.pinot.controller.api.upload.batch.SuccessBatchIdGenerationResponse;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.util.TableSizeReader;
import com.linkedin.pinot.controller.validation.StorageQuotaChecker;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.UUID;
import java.util.concurrent.Executor;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class TableSize {
  private static Logger LOGGER = LoggerFactory.getLogger(TableSize.class);

  @Inject
  ControllerConf _controllerConf;
  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;
  @Inject
  Executor _executor;
  @Inject
  HttpConnectionManager _connectionManager;

  @Inject ControllerMetrics _controllerMetrics;

  @GET
  @Path("/tables/{tableName}/size")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Read table sizes",
      notes = "Get table size details. Table size is the size of untarred segments including replication")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Table not found"),
      @ApiResponse(code = 500, message = "Internal server error")})
  public TableSizeReader.TableSizeDetails getTableSize(
      @ApiParam(value = "Table name without type", required = true, example = "myTable | myTable_OFFLINE")
          @PathParam("tableName") String tableName,
      @ApiParam(value = "Get detailed information", required = false) @DefaultValue("true")
          @QueryParam("detailed") boolean detailed
  ) {
    TableSizeReader
        tableSizeReader = new TableSizeReader(_executor, _connectionManager,
        _controllerMetrics, _pinotHelixResourceManager);
    TableSizeReader.TableSizeDetails tableSizeDetails = null;
    try {
      tableSizeDetails = tableSizeReader.getTableSizeDetails(tableName,
          _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
    } catch (Throwable t) {
      throw new ControllerApplicationException(LOGGER, String.format("Failed to read table size for %s", tableName),
          Response.Status.INTERNAL_SERVER_ERROR, t);
    }

    if (tableSizeDetails == null) {
      throw new ControllerApplicationException(LOGGER, "Table " + tableName + " not found",
          Response.Status.NOT_FOUND);
    }
    return tableSizeDetails;
  }

  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/batch")
  @ApiOperation(value = "Create batch id to upload segments if segments upload is within storage quota", notes = "Given a list of segment names and sizes, create batch id for table to upload segments if segments upload is within storage quota.")
  public void createBatchForTable(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "List of segment names to be uploaded and their sizes") SegmentsInBatch segments,
      @Context HttpHeaders headers, @Context Request request, @Suspended final AsyncResponse asyncResponse) {
    try {
      asyncResponse.resume(generateBatchIdForTable(tableName, segments));
    } catch (Throwable t) {
      asyncResponse.resume(t);
    }
  }

  private SuccessBatchIdGenerationResponse generateBatchIdForTable(String tableName, SegmentsInBatch segments) {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkArgument(segments != null && !segments.getSegmentEntries().isEmpty());
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);

    TableConfig offlineTableConfig =
        ZKMetadataProvider.getOfflineTableConfig(_pinotHelixResourceManager.getPropertyStore(), offlineTableName);

    LOGGER.info("Start checking quota config for table: {}", rawTableName);
    StorageQuotaChecker.QuotaCheckerResponse quotaCheckerResponse;
    try {
      TableSizeReader tableSizeReader =
          new TableSizeReader(_executor, _connectionManager, _controllerMetrics, _pinotHelixResourceManager);
      StorageQuotaChecker quotaChecker =
          new StorageQuotaChecker(offlineTableConfig, tableSizeReader, _controllerMetrics, _pinotHelixResourceManager);

      quotaCheckerResponse = quotaChecker.isBatchSegmentSizeWithinQuota(offlineTableName, segments.getSegmentEntries(),
          _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
    } catch (InvalidConfigException ie) {
      throw new ControllerApplicationException(LOGGER,
          "Quota check failed for batch upload segments of table: " + offlineTableName + ", reason: " + ie.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    if (!quotaCheckerResponse.isSegmentWithinQuota) {
      throw new ControllerApplicationException(LOGGER,
          "Quota check failed for batch upload segments of table: " + offlineTableName + ", reason: "
              + quotaCheckerResponse.reason, Response.Status.FORBIDDEN);
    }

    // Batch upload is within storage quota.
    try {
      // Generate batchId.
      String batchId = UUID.randomUUID().toString() + "-" + segments.getSegmentEntries().size();

      return new SuccessBatchIdGenerationResponse("Segment upload is within quota. Successfully generated batch id for table: " + rawTableName,
          batchId, quotaCheckerResponse.currentSizeAcrossReplicas, quotaCheckerResponse.storageQuotaAcrossReplicas);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Unable to initialize batch upload of table: " + rawTableName + ", reason: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
