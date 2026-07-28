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
package org.apache.pinot.server.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.compression.ColumnCompressionStatsAccumulator;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsContribution;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsInfo;
import org.apache.pinot.common.restlet.resources.ResourceUtils;
import org.apache.pinot.common.restlet.resources.SegmentCompressionStatsContribution;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.spi.config.table.TableConfig;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * API to provide table sizes
 */
@Api(tags = "Table", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```"),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")}))
@Path("/")
public class TableSizeResource {

  @Inject
  private ServerInstance _serverInstance;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/size")
  @ApiOperation(value = "Show table storage size", notes = "Lists size of all the segments of the table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error"),
      @ApiResponse(code = 404, message = "Table not found")
  })
  public String getTableSize(
      @ApiParam(value = "Table Name with type", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Provide detailed information") @DefaultValue("true") @QueryParam("detailed") boolean detailed,
      @ApiParam(value = "Include segment compression statistics when detailed=true and the table enables "
          + "tableIndexConfig.compressionStatsEnabled")
      @DefaultValue("false") @QueryParam("includeCompressionStats") boolean includeCompressionStats,
      @ApiParam(value = "Include per-column compression statistics when detailed=true and the table enables "
          + "tableIndexConfig.compressionStatsEnabled; this also populates segment summary fields")
      @DefaultValue("false") @QueryParam("includeColumnCompressionStats") boolean includeColumnCompressionStats,
      @Context HttpHeaders headers)
      throws WebApplicationException {
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    InstanceDataManager instanceDataManager = _serverInstance.getInstanceDataManager();

    if (instanceDataManager == null) {
      throw new WebApplicationException("Invalid server initialization", Response.Status.INTERNAL_SERVER_ERROR);
    }

    TableDataManager tableDataManager = instanceDataManager.getTableDataManager(tableName);
    if (tableDataManager == null) {
      throw new WebApplicationException("Table: " + tableName + " is not found", Response.Status.NOT_FOUND);
    }

    Pair<TableConfig, ?> cachedPair = tableDataManager.getCachedTableConfigAndSchema();
    boolean compressionStatsEnabled = detailed && (includeCompressionStats || includeColumnCompressionStats)
        && cachedPair != null && cachedPair.getLeft() != null
        && cachedPair.getLeft().getIndexingConfig() != null
        && cachedPair.getLeft().getIndexingConfig().isCompressionStatsEnabled();

    long tableSizeInBytes = 0L;
    int columnContributions = 0;
    List<SegmentSizeInfo> segmentSizeInfos = new ArrayList<>();
    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    try {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        long segmentSizeBytes = 0L;
        boolean hasImmutable = false;
        boolean compressionStatsComplete = true;
        long uncompressedValueSizeInBytes = 0;
        long compressionForwardIndexAndDictionaryStorageSizeInBytes = 0;
        Map<String, ColumnCompressionStatsAccumulator> columnAccumulators =
            compressionStatsEnabled && includeColumnCompressionStats ? new HashMap<>() : null;
        for (IndexSegment segment : segmentDataManager.getReportableSegments()) {
          if (segment instanceof ImmutableSegment immutableSegment) {
            segmentSizeBytes += immutableSegment.getSegmentSizeBytes();
            if (compressionStatsEnabled) {
              if (includeColumnCompressionStats) {
                columnContributions = TablesResource.addColumnContributions(columnContributions, immutableSegment,
                    null);
              }
              SegmentCompressionStatsContribution contribution =
                  SegmentCompressionStatsReader.read(immutableSegment.getSegmentMetadata(),
                      includeColumnCompressionStats);
              if (contribution.isComplete()) {
                uncompressedValueSizeInBytes += contribution.getUncompressedValueSizeInBytes();
                compressionForwardIndexAndDictionaryStorageSizeInBytes +=
                    contribution.getForwardIndexAndDictionaryStorageSizeInBytes();
              } else {
                compressionStatsComplete = false;
              }
              if (columnAccumulators != null && contribution.getColumnCompressionStats() != null) {
                for (Map.Entry<String, ColumnCompressionStatsContribution> entry
                    : contribution.getColumnCompressionStats().entrySet()) {
                  columnAccumulators.computeIfAbsent(entry.getKey(), ignored ->
                          new ColumnCompressionStatsAccumulator())
                      .add(entry.getValue());
                }
              }
            }
            hasImmutable = true;
          }
        }
        if (hasImmutable) {
          if (detailed) {
            if (compressionStatsEnabled) {
              Map<String, ColumnCompressionStatsInfo> columnCompressionStats = null;
              if (columnAccumulators != null && !columnAccumulators.isEmpty()) {
                columnCompressionStats = new HashMap<>();
                for (Map.Entry<String, ColumnCompressionStatsAccumulator> entry : columnAccumulators.entrySet()) {
                  columnCompressionStats.put(entry.getKey(),
                      entry.getValue().toColumnCompressionStatsInfo(entry.getKey()));
                }
              }
              segmentSizeInfos.add(new SegmentSizeInfo(segmentDataManager.getSegmentName(), segmentSizeBytes,
                  compressionStatsComplete ? uncompressedValueSizeInBytes : -1,
                  compressionStatsComplete ? compressionForwardIndexAndDictionaryStorageSizeInBytes : -1,
                  columnCompressionStats));
            } else {
              segmentSizeInfos.add(new SegmentSizeInfo(segmentDataManager.getSegmentName(), segmentSizeBytes));
            }
          }
          tableSizeInBytes += segmentSizeBytes;
        }
      }
    } finally {
      // we could release segmentDataManagers as we iterate in the loop above
      // but this is cleaner with clear semantics of usage. Also, above loop
      // executes fast so duration of holding segments is not a concern
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }

    TableSizeInfo tableSizeInfo = new TableSizeInfo(tableDataManager.getTableName(), tableSizeInBytes,
        segmentSizeInfos, TableSizeInfo.CURRENT_METADATA_VERSION);
    //invalid to use the segmentDataManagers below
    return ResourceUtils.convertToJsonString(tableSizeInfo);
  }

  // same as above but with /tables (plural) path for consistency.
  // /table was by mistake. We will use plural from hereon
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/table/{tableName}/size")
  @ApiOperation(value = "Show table storage size", notes = "Lists size of all the segments of the table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error"),
      @ApiResponse(code = 404, message = "Table not found")
  })
  @Deprecated
  public String getTableSizeOld(
      @ApiParam(value = "Table Name with type", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Provide detailed information") @DefaultValue("true") @QueryParam("detailed") boolean detailed,
      @ApiParam(value = "Include segment compression statistics when detailed=true and the table enables "
          + "tableIndexConfig.compressionStatsEnabled")
      @DefaultValue("false") @QueryParam("includeCompressionStats") boolean includeCompressionStats,
      @ApiParam(value = "Include per-column compression statistics when detailed=true and the table enables "
          + "tableIndexConfig.compressionStatsEnabled; this also populates segment summary fields")
      @DefaultValue("false") @QueryParam("includeColumnCompressionStats") boolean includeColumnCompressionStats,
      @Context HttpHeaders headers)
      throws WebApplicationException {
    return this.getTableSize(tableName, detailed, includeCompressionStats, includeColumnCompressionStats, headers);
  }
}
