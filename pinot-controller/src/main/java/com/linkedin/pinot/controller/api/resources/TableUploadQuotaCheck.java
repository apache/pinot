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

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.utils.DataSize;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.util.TableSizeReader;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.concurrent.Executor;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * API that allows for callers to check if storage quota will be exceeded if an upload of certain
 * size is attempted.
 *
 * The size provided as a parameter to the API should be UNCOMPRESSED size in bytes of all segments
 * that will be uploaded. The API will take into account whether the segments to be uploaded will
 * have indices generated or not. If no indices are present in the segment files, the API will
 * do a best effort estimate of the indices. Currently, this is set to about 50% of the data size
 * which is what we typically see in most use cases.This could result in overestimation and
 * rejection of the upload thus. This should get better overtime as we gather more stats specific
 * to tables about the index ratios.
 */
@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class TableUploadQuotaCheck {
  private static Logger LOGGER = LoggerFactory.getLogger(TableUploadQuotaCheck.class);
  // empirical value for percentage of table storage used by indices
  // TODO: replace with table specific stats eventually
  private static final float INDEX_RATIO = 0.5f;

  @Inject
  ControllerConf _controllerConf;
  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;
  @Inject
  Executor _executor;
  @Inject
  HttpConnectionManager _connectionManager;

  @GET
  @Path("/tables/{tableName}/checkQuotaForUpload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Check if an upload of given size can succeed",
      notes = "Gets current table size and checks if the upload of given size (refresh or append) can succeed")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "size in bytes invalid"),
      @ApiResponse(code = 404, message = "Table not found"),
      @ApiResponse(code = 413, message = "Proposed upload size too large"),
      @ApiResponse(code = 500, message = "Internal server error")})
  public SuccessResponse checkQuotaForUpload(
      @ApiParam(value = "Table name without type", required = true, example = "myTable")
          @PathParam("tableName") String tableName,
      @ApiParam(value = "Uncompressed size in bytes", required = true)
          @QueryParam("size") long size) {

    if (size < 0) {
      throw new ControllerApplicationException(LOGGER,
          "size in bytes: " + size + " invalid", Response.Status.BAD_REQUEST);
    }

    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    TableConfig offlineTableConfig =
        ZKMetadataProvider.getOfflineTableConfig(_pinotHelixResourceManager.getPropertyStore(), offlineTableName);
    if (offlineTableConfig == null) {
      throw new ControllerApplicationException(LOGGER,
          "Table: " + tableName + "not found", Response.Status.NOT_FOUND);
    }

    if (!offlineTableConfig.getIndexingConfig().isCreateInvertedIndexDuringSegmentGeneration()) {
      if (!offlineTableConfig.getIndexingConfig().getInvertedIndexColumns().isEmpty()) {
        size = size + (long)(size * INDEX_RATIO);
        LOGGER.info("Size (in bytes) estimate with indices included is ", size);
      }
    }

    int replication = offlineTableConfig.getValidationConfig().getReplicationNumber();
    long configured = offlineTableConfig.getQuotaConfig().storageSizeBytes() * replication;
    long uploadSizeForAllReplicas = size * replication;

    if ("REFRESH".equalsIgnoreCase(offlineTableConfig.getValidationConfig().getSegmentPushType())) {
      // we don't need to poll servers for existing size - just compare configured and new
      if (uploadSizeForAllReplicas > configured) {
        String message = "Upload of " + DataSize.fromBytes(size) + " will exceed quota. Estimated refresh size "
            + DataSize.fromBytes(uploadSizeForAllReplicas) + " (including " + replication
            + " replicas and index size estimated at a ratio of " + INDEX_RATIO
            + " will exceed configured storage size " + DataSize.fromBytes(configured)
            + "= " + DataSize.fromBytes(offlineTableConfig.getQuotaConfig().storageSizeBytes()) + "* " + replication;

        throw new ControllerApplicationException(LOGGER, message, Response.Status.REQUEST_ENTITY_TOO_LARGE);
      } else {
        return new SuccessResponse("Upload of size " + size + " within quota for table " + tableName);
      }
    }

    // append use case - poll server for existing size
    TableSizeReader
        tableSizeReader = new TableSizeReader(_executor, _connectionManager, _pinotHelixResourceManager);
    TableSizeReader.TableSizeDetails tableSizeDetails = null;
    try {
      tableSizeDetails = tableSizeReader.getTableSizeDetails(tableName,
          _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, String.format("Failed to read table size for %s", tableName),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }

    if (tableSizeDetails.reportedSizeInBytes == -1) {
      // TODO: handle partial errors too once #3067 is merged
      throw new ControllerApplicationException(LOGGER, String.format("Failed to read table size for %s", tableName),
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    if (tableSizeDetails.estimatedSizeInBytes + uploadSizeForAllReplicas > configured) {
      String message = "Upload of " + DataSize.fromBytes(size) + " will exceed quota. Estimated append size "
          + DataSize.fromBytes(uploadSizeForAllReplicas) + " (including " + replication
          + " replicas and index size estimated at a ratio of " + INDEX_RATIO
          + " will exceed configured storage size " + DataSize.fromBytes(configured)
          + "= " + DataSize.fromBytes(offlineTableConfig.getQuotaConfig().storageSizeBytes()) + "* " + replication;
      throw new ControllerApplicationException(LOGGER, message, Response.Status.REQUEST_ENTITY_TOO_LARGE);
    }

    return new SuccessResponse("Upload of size " + size + " within quota for table " + tableName);
  }
}
