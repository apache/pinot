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
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsInfo;
import org.apache.pinot.common.restlet.resources.ResourceUtils;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.StandardIndexes;
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

    // Check feature flag — only collect per-column compression stats if enabled
    Pair<TableConfig, ?> cachedPair = tableDataManager.getCachedTableConfigAndSchema();
    boolean compressionStatsEnabled = cachedPair != null && cachedPair.getLeft() != null
        && cachedPair.getLeft().getIndexingConfig() != null
        && cachedPair.getLeft().getIndexingConfig().isCompressionStatsEnabled();

    long tableSizeInBytes = 0L;
    List<SegmentSizeInfo> segmentSizeInfos = new ArrayList<>();
    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    try {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        if (segmentDataManager instanceof ImmutableSegmentDataManager) {
          ImmutableSegment immutableSegment = (ImmutableSegment) segmentDataManager.getSegment();
          long segmentSizeBytes = immutableSegment.getSegmentSizeBytes();
          if (detailed) {
            if (compressionStatsEnabled) {
              long rawFwdIndexSize = 0;
              long compressedFwdIndexSize = 0;
              Map<String, ColumnCompressionStatsInfo> columnCompressionStats = null;
              IndexService indexService = IndexService.getInstance();
              SegmentMetadata segmentMetadata = immutableSegment.getSegmentMetadata();
              for (ColumnMetadata colMeta : segmentMetadata.getColumnMetadataMap().values()) {
                String codec = colMeta.getCompressionCodec();
                long fwdIndexSize = colMeta.getIndexSizeFor(StandardIndexes.forward());
                if (fwdIndexSize <= 0) {
                  continue;
                }
                long rawIngestSize;
                long onDiskSize;
                if (codec != null) {
                  // Raw column: use persisted uncompressed size and forward index size
                  rawIngestSize = colMeta.getUnonDiskSizeBytes();
                  onDiskSize = fwdIndexSize;
                  if (rawIngestSize > 0) {
                    rawFwdIndexSize += rawIngestSize;
                    compressedFwdIndexSize += onDiskSize;
                  } else {
                    // Old raw segment without persisted uncompressed size — skip
                    continue;
                  }
                } else if (colMeta.hasDictionary()) {
                  // Dict column: onDisk = fwd + dict file; rawIngest from metadata if available
                  long dictFileSize = colMeta.getIndexSizeFor(StandardIndexes.dictionary());
                  onDiskSize = fwdIndexSize + (dictFileSize >= 0 ? dictFileSize : 0);
                  rawIngestSize = colMeta.getDictColumnRawIngestSizeBytes();
                  codec = ColumnCompressionStatsInfo.CODEC_DICT_ENCODED;
                } else {
                  // Old raw segment without persisted codec — skip
                  continue;
                }
                double ratio = (rawIngestSize > 0 && onDiskSize > 0) ? (double) rawIngestSize / onDiskSize : 0;
                List<String> indexNames = new ArrayList<>();
                for (int i = 0, n = colMeta.getNumIndexes(); i < n; i++) {
                  indexNames.add(indexService.get(colMeta.getIndexType(i)).getId());
                }
                if (columnCompressionStats == null) {
                  columnCompressionStats = new HashMap<>();
                }
                columnCompressionStats.put(colMeta.getColumnName(),
                    new ColumnCompressionStatsInfo(colMeta.getColumnName(),
                        rawIngestSize, onDiskSize, ratio, codec,
                        indexNames.isEmpty() ? null : indexNames, null));
              }
              segmentSizeInfos.add(new SegmentSizeInfo(immutableSegment.getSegmentName(), segmentSizeBytes,
                  rawFwdIndexSize, compressedFwdIndexSize, immutableSegment.getTier(), columnCompressionStats));
            } else {
              segmentSizeInfos.add(new SegmentSizeInfo(immutableSegment.getSegmentName(), segmentSizeBytes,
                  -1, -1, immutableSegment.getTier()));
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

    TableSizeInfo tableSizeInfo =
        new TableSizeInfo(tableDataManager.getTableName(), tableSizeInBytes, segmentSizeInfos);
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
      @Context HttpHeaders headers)
      throws WebApplicationException {
    return this.getTableSize(tableName, detailed, headers);
  }
}
