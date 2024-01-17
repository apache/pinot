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

import com.google.common.base.Preconditions;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.response.server.TableIndexMetadataResponse;
import org.apache.pinot.common.restlet.resources.ResourceUtils;
import org.apache.pinot.common.restlet.resources.SegmentConsumerInfo;
import org.apache.pinot.common.restlet.resources.TableMetadataInfo;
import org.apache.pinot.common.restlet.resources.TableSegmentValidationInfo;
import org.apache.pinot.common.restlet.resources.TableSegments;
import org.apache.pinot.common.restlet.resources.TablesList;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.SegmentUploader;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.server.access.AccessControlFactory;
import org.apache.pinot.server.api.AdminApiApplication;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.stream.ConsumerPartitionState;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = "Table", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class TablesResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(TablesResource.class);
  private static final String PEER_SEGMENT_DOWNLOAD_DIR = "peerSegmentDownloadDir";
  private static final String SEGMENT_UPLOAD_DIR = "segmentUploadDir";

  @Inject
  private ServerInstance _serverInstance;

  @Inject
  private AccessControlFactory _accessControlFactory;

  @Inject
  @Named(AdminApiApplication.SERVER_INSTANCE_ID)
  private String _instanceId;

  @GET
  @Path("/tables")
  @Produces(MediaType.APPLICATION_JSON)
  //swagger annotations
  @ApiOperation(value = "List tables", notes = "List all the tables on this server")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success", response = TablesList.class),
      @ApiResponse(code = 500, message = "Server initialization error", response = ErrorInfo.class)
  })
  public String listTables() {
    InstanceDataManager instanceDataManager = ServerResourceUtils.checkGetInstanceDataManager(_serverInstance);
    List<String> tables = new ArrayList<>(instanceDataManager.getAllTables());
    return ResourceUtils.convertToJsonString(new TablesList(tables));
  }

  @GET
  @Path("/tables/{tableName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List table segments", notes = "List segments of table hosted on this server")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success", response = TableSegments.class),
      @ApiResponse(code = 500, message = "Server initialization error", response = ErrorInfo.class)
  })
  public String listTableSegments(
      @ApiParam(value = "Table name including type", required = true, example = "myTable_OFFLINE")
      @PathParam("tableName") String tableName) {
    TableDataManager tableDataManager = ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableName);
    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    try {
      List<String> segments = new ArrayList<>(segmentDataManagers.size());
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        segments.add(segmentDataManager.getSegmentName());
      }
      return ResourceUtils.convertToJsonString(new TableSegments(segments));
    } finally {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
  }

  @GET
  @Encoded
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableName}/metadata")
  @ApiOperation(value = "List metadata for all segments of a given table", notes = "List segments metadata of table "
      + "hosted on this server")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error"),
      @ApiResponse(code = 404, message = "Table not found")
  })
  public String getSegmentMetadata(
      @ApiParam(value = "Table Name with type", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Column name", allowMultiple = true) @QueryParam("columns") @DefaultValue("")
      List<String> columns)
      throws WebApplicationException {
    InstanceDataManager instanceDataManager = _serverInstance.getInstanceDataManager();

    if (instanceDataManager == null) {
      throw new WebApplicationException("Invalid server initialization", Response.Status.INTERNAL_SERVER_ERROR);
    }

    TableDataManager tableDataManager = instanceDataManager.getTableDataManager(tableName);
    if (tableDataManager == null) {
      throw new WebApplicationException("Table: " + tableName + " is not found", Response.Status.NOT_FOUND);
    }

    List<String> decodedColumns = new ArrayList<>(columns.size());
    for (String column : columns) {
      try {
        decodedColumns.add(URLDecoder.decode(column, StandardCharsets.UTF_8.name()));
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e.getCause());
      }
    }

    boolean allColumns = false;
    // For robustness, loop over all columns, if any of the columns is "*", return metadata for all columns.
    for (String column : decodedColumns) {
      if (column.equals("*")) {
        allColumns = true;
        break;
      }
    }
    Set<String> columnSet = allColumns ? null : new HashSet<>(decodedColumns);

    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    long totalSegmentSizeBytes = 0;
    long totalNumRows = 0;
    Map<String, Double> columnLengthMap = new HashMap<>();
    Map<String, Double> columnCardinalityMap = new HashMap<>();
    Map<String, Double> maxNumMultiValuesMap = new HashMap<>();
    Map<String, Map<String, Double>> columnIndexSizesMap = new HashMap<>();
    try {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        if (segmentDataManager instanceof ImmutableSegmentDataManager) {
          ImmutableSegment immutableSegment = (ImmutableSegment) segmentDataManager.getSegment();
          long segmentSizeBytes = immutableSegment.getSegmentSizeBytes();
          SegmentMetadataImpl segmentMetadata =
              (SegmentMetadataImpl) segmentDataManager.getSegment().getSegmentMetadata();

          totalSegmentSizeBytes += segmentSizeBytes;
          totalNumRows += segmentMetadata.getTotalDocs();

          if (columnSet == null) {
            columnSet = segmentMetadata.getAllColumns();
          } else {
            columnSet.retainAll(segmentMetadata.getAllColumns());
          }
          for (String column : columnSet) {
            ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataMap().get(column);
            int columnLength = 0;
            DataType storedDataType = columnMetadata.getDataType().getStoredType();
            if (storedDataType.isFixedWidth()) {
              // For type of fixed width: INT, LONG, FLOAT, DOUBLE, BOOLEAN (stored as INT), TIMESTAMP (stored as LONG),
              // set the columnLength as the fixed width.
              columnLength = storedDataType.size();
            } else if (columnMetadata.hasDictionary()) {
              // For type of variable width (String, Bytes), if it's stored using dictionary encoding, set the
              // columnLength as the max length in dictionary.
              columnLength = columnMetadata.getColumnMaxLength();
            } else if (storedDataType == DataType.STRING || storedDataType == DataType.BYTES) {
              // For type of variable width (String, Bytes), if it's stored using raw bytes, set the columnLength as
              // the length of the max value.
              if (columnMetadata.getMaxValue() != null) {
                String maxValueString = (String) columnMetadata.getMaxValue();
                columnLength = maxValueString.getBytes(StandardCharsets.UTF_8).length;
              }
            } else {
              // For type of STRUCT, MAP, LIST, set the columnLength as DEFAULT_MAX_LENGTH (512).
              columnLength = FieldSpec.DEFAULT_MAX_LENGTH;
            }
            int columnCardinality = columnMetadata.getCardinality();
            columnLengthMap.merge(column, (double) columnLength, Double::sum);
            columnCardinalityMap.merge(column, (double) columnCardinality, Double::sum);
            if (!columnMetadata.isSingleValue()) {
              int maxNumMultiValues = columnMetadata.getMaxNumberOfMultiValues();
              maxNumMultiValuesMap.merge(column, (double) maxNumMultiValues, Double::sum);
            }
            for (Map.Entry<IndexType<?, ?, ?>, Long> entry : columnMetadata.getIndexSizeMap().entrySet()) {
              String indexName = entry.getKey().getId();
              Map<String, Double> columnIndexSizes = columnIndexSizesMap.getOrDefault(column, new HashMap<>());
              Double indexSize = columnIndexSizes.getOrDefault(indexName, 0d) + entry.getValue();
              columnIndexSizes.put(indexName, indexSize);
              columnIndexSizesMap.put(column, columnIndexSizes);
            }
          }
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

    TableMetadataInfo tableMetadataInfo =
        new TableMetadataInfo(tableDataManager.getTableName(), totalSegmentSizeBytes, segmentDataManagers.size(),
            totalNumRows, columnLengthMap, columnCardinalityMap, maxNumMultiValuesMap, columnIndexSizesMap);
    return ResourceUtils.convertToJsonString(tableMetadataInfo);
  }

  @GET
  @Encoded
  @Path("/tables/{tableName}/indexes")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Provide index metadata", notes = "Provide index details for the table")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error",
      response = ErrorInfo.class), @ApiResponse(code = 404, message = "Table or segment not found", response =
      ErrorInfo.class)
  })
  public String getTableIndexes(
      @ApiParam(value = "Table name including type", required = true, example = "myTable_OFFLINE")
      @PathParam("tableName") String tableName)
      throws Exception {
    TableDataManager tableDataManager = ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableName);
    List<SegmentDataManager> allSegments = tableDataManager.acquireAllSegments();
    try {
      int totalSegmentCount = 0;
      Map<String, Map<String, Integer>> columnToIndexesCount = new HashMap<>();
      for (SegmentDataManager segmentDataManager : allSegments) {
        if (segmentDataManager instanceof RealtimeSegmentDataManager) {
          // REALTIME segments may not have indexes since not all indexes have mutable implementations
          continue;
        }
        totalSegmentCount++;
        IndexSegment segment = segmentDataManager.getSegment();
        segment.getColumnNames().forEach(col -> {
          columnToIndexesCount.putIfAbsent(col, new HashMap<>());
          DataSource colDataSource = segment.getDataSource(col);
          IndexService.getInstance().getAllIndexes().forEach(idxType -> {
            int count = colDataSource.getIndex(idxType) != null ? 1 : 0;
            columnToIndexesCount.get(col).merge(idxType.getId(), count, Integer::sum);
          });
        });
      }
      TableIndexMetadataResponse tableIndexMetadataResponse =
          new TableIndexMetadataResponse(totalSegmentCount, columnToIndexesCount);
      return JsonUtils.objectToString(tableIndexMetadataResponse);
    } finally {
      for (SegmentDataManager segmentDataManager : allSegments) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
  }

  @GET
  @Encoded
  @Path("/tables/{tableName}/segments/{segmentName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Provide segment metadata", notes = "Provide segments metadata for the segment on server")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error", response = ErrorInfo.class),
      @ApiResponse(code = 404, message = "Table or segment not found", response = ErrorInfo.class)
  })
  public String getSegmentMetadata(
      @ApiParam(value = "Table name including type", required = true, example = "myTable_OFFLINE")
      @PathParam("tableName") String tableName,
      @ApiParam(value = "Segment name", required = true) @PathParam("segmentName") String segmentName,
      @ApiParam(value = "Column name", allowMultiple = true) @QueryParam("columns") @DefaultValue("")
      List<String> columns) {
    for (int i = 0; i < columns.size(); i++) {
      try {
        columns.set(i, URLDecoder.decode(columns.get(i), StandardCharsets.UTF_8.name()));
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e.getCause());
      }
    }

    TableDataManager tableDataManager = ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableName);
    try {
      segmentName = URLDecoder.decode(segmentName, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e.getCause());
    }
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
    if (segmentDataManager == null) {
      throw new WebApplicationException(String.format("Table %s segments %s does not exist", tableName, segmentName),
          Response.Status.NOT_FOUND);
    }

    try {
      return SegmentMetadataFetcher.getSegmentMetadata(segmentDataManager, columns);
    } catch (Exception e) {
      LOGGER.error("Failed to convert table {} segment {} to json", tableName, segmentName);
      throw new WebApplicationException("Failed to convert segment metadata to json",
          Response.Status.INTERNAL_SERVER_ERROR);
    } finally {
      tableDataManager.releaseSegment(segmentDataManager);
    }
  }

  @GET
  @Path("/tables/{tableName}/segments/crc")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Provide segment crc information", notes = "Provide crc information for the segments on server")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error", response = ErrorInfo.class),
      @ApiResponse(code = 404, message = "Table or segment not found", response = ErrorInfo.class)
  })
  public String getCrcMetadataForTable(
      @ApiParam(value = "Table name including type", required = true, example = "myTable_OFFLINE")
      @PathParam("tableName") String tableName) {
    TableDataManager tableDataManager = ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableName);
    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    try {
      Map<String, String> segmentCrcForTable = new HashMap<>();
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        segmentCrcForTable.put(segmentDataManager.getSegmentName(),
            segmentDataManager.getSegment().getSegmentMetadata().getCrc());
      }
      return ResourceUtils.convertToJsonString(segmentCrcForTable);
    } catch (Exception e) {
      throw new WebApplicationException("Failed to convert crc information to json",
          Response.Status.INTERNAL_SERVER_ERROR);
    } finally {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
  }

  // TODO Add access control similar to PinotSegmentUploadDownloadRestletResource for segment download.
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Path("/segments/{tableNameWithType}/{segmentName}")
  @ApiOperation(value = "Download an immutable segment", notes = "Download an immutable segment in zipped tar format.")
  public Response downloadSegment(
      @ApiParam(value = "Name of the table with type REALTIME OR OFFLINE", required = true, example = "myTable_OFFLINE")
      @PathParam("tableNameWithType") String tableNameWithType,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @Context HttpHeaders httpHeaders)
      throws Exception {
    LOGGER.info("Received a request to download segment {} for table {}", segmentName, tableNameWithType);
    // Validate data access
    ServerResourceUtils.validateDataAccess(_accessControlFactory, tableNameWithType, httpHeaders);

    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
    if (segmentDataManager == null) {
      throw new WebApplicationException(
          String.format("Table %s segment %s does not exist", tableNameWithType, segmentName),
          Response.Status.NOT_FOUND);
    }
    try {
      // TODO Limit the number of concurrent downloads of segments because compression is an expensive operation.
      // Store the tar.gz segment file in the server's segmentTarDir folder with a unique file name.
      // Note that two clients asking the same segment file will result in the same tar.gz files being created twice.
      // Will revisit for optimization if performance becomes an issue.
      File tmpSegmentTarDir =
          new File(_serverInstance.getInstanceDataManager().getSegmentFileDirectory(), PEER_SEGMENT_DOWNLOAD_DIR);
      tmpSegmentTarDir.mkdir();

      File segmentTarFile = org.apache.pinot.common.utils.FileUtils.concatAndValidateFile(tmpSegmentTarDir,
          tableNameWithType + "_" + segmentName + "_" + UUID.randomUUID() + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION,
          "Invalid table / segment name: %s , %s", tableNameWithType, segmentName);

      TarGzCompressionUtils.createTarGzFile(new File(tableDataManager.getTableDataDir(), segmentName), segmentTarFile);
      Response.ResponseBuilder builder = Response.ok();
      builder.entity((StreamingOutput) output -> {
        try {
          Files.copy(segmentTarFile.toPath(), output);
        } finally {
          FileUtils.deleteQuietly(segmentTarFile);
        }
      });
      builder.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + segmentTarFile.getName());
      builder.header(HttpHeaders.CONTENT_LENGTH, segmentTarFile.length());
      return builder.build();
    } finally {
      tableDataManager.releaseSegment(segmentDataManager);
    }
  }

  /**
   * Download snapshot for the given immutable segment for upsert table. This endpoint is used when get snapshot from
   * peer to avoid recompute when reload segments.
   */
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Path("/segments/{tableNameWithType}/{segmentName}/validDocIds")
  @ApiOperation(value = "Download validDocIds for an REALTIME immutable segment", notes = "Download validDocIds for "
      + "an immutable segment in bitmap format.")
  public Response downloadValidDocIds(
      @ApiParam(value = "Name of the table with type REALTIME", required = true, example = "myTable_REALTIME")
      @PathParam("tableNameWithType") String tableNameWithType,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @Context HttpHeaders httpHeaders) {
    segmentName = URIUtils.decode(segmentName);
    LOGGER.info("Received a request to download validDocIds for segment {} table {}", segmentName, tableNameWithType);
    // Validate data access
    ServerResourceUtils.validateDataAccess(_accessControlFactory, tableNameWithType, httpHeaders);

    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
    if (segmentDataManager == null) {
      throw new WebApplicationException(
          String.format("Table %s segment %s does not exist", tableNameWithType, segmentName),
          Response.Status.NOT_FOUND);
    }

    try {
      IndexSegment indexSegment = segmentDataManager.getSegment();
      if (!(indexSegment instanceof ImmutableSegmentImpl)) {
        throw new WebApplicationException(
            String.format("Table %s segment %s is not a immutable segment", tableNameWithType, segmentName),
            Response.Status.BAD_REQUEST);
      }
      MutableRoaringBitmap validDocIds =
          indexSegment.getValidDocIds() != null ? indexSegment.getValidDocIds().getMutableRoaringBitmap() : null;
      if (validDocIds == null) {
        throw new WebApplicationException(
            String.format("Missing validDocIds for table %s segment %s does not exist", tableNameWithType, segmentName),
            Response.Status.NOT_FOUND);
      }

      byte[] validDocIdsBytes = RoaringBitmapUtils.serialize(validDocIds);
      Response.ResponseBuilder builder = Response.ok(validDocIdsBytes);
      builder.header(HttpHeaders.CONTENT_LENGTH, validDocIdsBytes.length);
      return builder.build();
    } finally {
      tableDataManager.releaseSegment(segmentDataManager);
    }
  }

  @GET
  @Path("/tables/{tableNameWithType}/validDocIdMetadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Provides segment validDocId metadata", notes = "Provides segment validDocId metadata")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error", response = ErrorInfo.class),
      @ApiResponse(code = 404, message = "Table or segment not found", response = ErrorInfo.class)
  })
  public String getValidDocIdMetadata(
      @ApiParam(value = "Table name including type", required = true, example = "myTable_REALTIME")
      @PathParam("tableNameWithType") String tableNameWithType,
      @ApiParam(value = "Segment name", allowMultiple = true) @QueryParam("segmentNames")
      List<String> segmentNames) {
    return ResourceUtils.convertToJsonString(processValidDocIdMetadata(tableNameWithType, segmentNames));
  }

  @POST
  @Path("/tables/{tableNameWithType}/validDocIdMetadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Provides segment validDocId metadata", notes = "Provides segment validDocId metadata")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error", response = ErrorInfo.class),
      @ApiResponse(code = 404, message = "Table or segment not found", response = ErrorInfo.class)
  })
  public String getValidDocIdMetadata(
      @ApiParam(value = "Table name including type", required = true, example = "myTable_REALTIME")
      @PathParam("tableNameWithType") String tableNameWithType, TableSegments tableSegments) {
    List<String> segmentNames = tableSegments.getSegments();
    return ResourceUtils.convertToJsonString(processValidDocIdMetadata(tableNameWithType, segmentNames));
  }

  private List<Map<String, Object>> processValidDocIdMetadata(String tableNameWithType, List<String> segments) {
    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);
    List<String> missingSegments = new ArrayList<>();
    List<SegmentDataManager> segmentDataManagers;
    if (segments == null || segments.isEmpty()) {
      segmentDataManagers = tableDataManager.acquireAllSegments();
    } else {
      segmentDataManagers = tableDataManager.acquireSegments(segments, missingSegments);
      if (!missingSegments.isEmpty()) {
        throw new WebApplicationException(
            String.format("Table %s has missing segments: %s)", tableNameWithType, segments),
            Response.Status.NOT_FOUND);
      }
    }
    List<Map<String, Object>> allValidDocIdMetadata = new ArrayList<>();
    for (SegmentDataManager segmentDataManager : segmentDataManagers) {
      try {
        IndexSegment indexSegment = segmentDataManager.getSegment();
        if (indexSegment == null) {
          LOGGER.warn("Table {} segment {} does not exist", tableNameWithType, segmentDataManager.getSegmentName());
          continue;
        }
        // Skip the consuming segments
        if (!(indexSegment instanceof ImmutableSegmentImpl)) {
          String msg = String.format("Table %s segment %s is not a immutable segment", tableNameWithType,
              segmentDataManager.getSegmentName());
          LOGGER.warn(msg);
          continue;
        }
        MutableRoaringBitmap validDocIds =
            indexSegment.getValidDocIds() != null ? indexSegment.getValidDocIds().getMutableRoaringBitmap() : null;
        if (validDocIds == null) {
          String msg = String.format("Missing validDocIds for table %s segment %s does not exist", tableNameWithType,
              segmentDataManager.getSegmentName());
          LOGGER.warn(msg);
          throw new WebApplicationException(msg, Response.Status.NOT_FOUND);
        }
        Map<String, Object> validDocIdMetadata = new HashMap<>();
        int totalDocs = indexSegment.getSegmentMetadata().getTotalDocs();
        int totalValidDocs = validDocIds.getCardinality();
        int totalInvalidDocs = totalDocs - totalValidDocs;
        validDocIdMetadata.put("segmentName", segmentDataManager.getSegmentName());
        validDocIdMetadata.put("totalDocs", totalDocs);
        validDocIdMetadata.put("totalValidDocs", totalValidDocs);
        validDocIdMetadata.put("totalInvalidDocs", totalInvalidDocs);
        allValidDocIdMetadata.add(validDocIdMetadata);
      } finally {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
    return allValidDocIdMetadata;
  }

  /**
   * Upload a low level consumer segment to segment store and return the segment download url. This endpoint is used
   * when segment store copy is unavailable for committed low level consumer segments.
   * Please note that invocation of this endpoint may cause query performance to suffer, since we tar up the segment
   * to upload it.
   *
   * @see <a href="https://tinyurl.com/f63ru4sb></a>
   * @param realtimeTableName table name with type.
   * @param segmentName name of the segment to be uploaded
   * @param timeoutMs timeout for the segment upload to the deep-store. If this is negative, the default timeout
   *                  would be used.
   * @return full url where the segment is uploaded
   * @throws Exception if an error occurred during the segment upload.
   */
  @POST
  @Path("/segments/{realtimeTableName}/{segmentName}/upload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Upload a low level consumer segment to segment store and return the segment download url",
      notes = "Upload a low level consumer segment to segment store and return the segment download url")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error", response = ErrorInfo.class),
      @ApiResponse(code = 404, message = "Table or segment not found", response = ErrorInfo.class),
      @ApiResponse(code = 400, message = "Bad request", response = ErrorInfo.class)
  })
  public String uploadLLCSegment(
      @ApiParam(value = "Name of the REALTIME table", required = true) @PathParam("realtimeTableName")
      String realtimeTableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") String segmentName,
      @QueryParam("uploadTimeoutMs") @DefaultValue("-1") int timeoutMs)
      throws Exception {
    LOGGER.info("Received a request to upload low level consumer segment {} for table {}", segmentName,
        realtimeTableName);

    // Check it's realtime table
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(realtimeTableName);
    if (TableType.OFFLINE == tableType) {
      throw new WebApplicationException(
          String.format("Cannot upload low level consumer segment for OFFLINE table: %s", realtimeTableName),
          Response.Status.BAD_REQUEST);
    }

    // Check the segment is low level consumer segment
    if (!LLCSegmentName.isLLCSegment(segmentName)) {
      throw new WebApplicationException(String.format("Segment %s is not a low level consumer segment", segmentName),
          Response.Status.BAD_REQUEST);
    }

    String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(realtimeTableName);
    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);
    SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
    if (segmentDataManager == null) {
      throw new WebApplicationException(
          String.format("Table %s segment %s does not exist", realtimeTableName, segmentName),
          Response.Status.NOT_FOUND);
    }

    File segmentTarFile = null;
    try {
      // Create the tar.gz segment file in the server's segmentTarUploadDir folder with a unique file name.
      File segmentTarUploadDir =
          new File(_serverInstance.getInstanceDataManager().getSegmentFileDirectory(), SEGMENT_UPLOAD_DIR);
      segmentTarUploadDir.mkdir();

      segmentTarFile = org.apache.pinot.common.utils.FileUtils.concatAndValidateFile(segmentTarUploadDir,
          tableNameWithType + "_" + segmentName + "_" + UUID.randomUUID() + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION,
          "Invalid table / segment name: %s, %s", tableNameWithType, segmentName);

      TarGzCompressionUtils.createTarGzFile(new File(tableDataManager.getTableDataDir(), segmentName), segmentTarFile);

      // Use segment uploader to upload the segment tar file to segment store and return the segment download url.
      SegmentUploader segmentUploader = _serverInstance.getInstanceDataManager().getSegmentUploader();
      URI segmentDownloadUrl;
      if (timeoutMs <= 0) {
        // Use default timeout if passed timeout is not positive
        segmentDownloadUrl = segmentUploader.uploadSegment(segmentTarFile, new LLCSegmentName(segmentName));
      } else {
        segmentDownloadUrl = segmentUploader.uploadSegment(segmentTarFile, new LLCSegmentName(segmentName), timeoutMs);
      }
      if (segmentDownloadUrl == null) {
        throw new WebApplicationException(
            String.format("Failed to upload table %s segment %s to segment store", realtimeTableName, segmentName),
            Response.Status.INTERNAL_SERVER_ERROR);
      }
      return segmentDownloadUrl.toString();
    } finally {
      FileUtils.deleteQuietly(segmentTarFile);
      tableDataManager.releaseSegment(segmentDataManager);
    }
  }

  @GET
  @Path("tables/{realtimeTableName}/consumingSegmentsInfo")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the info for consumers of this REALTIME table", notes =
      "Get consumers info from the table data manager. Note that the partitionToOffsetMap has been deprecated "
          + "and will be removed in the next release. The info is now embedded within each partition's state as "
          + "currentOffsetsMap")
  public List<SegmentConsumerInfo> getConsumingSegmentsInfo(
      @ApiParam(value = "Name of the REALTIME table", required = true) @PathParam("realtimeTableName")
      String realtimeTableName) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(realtimeTableName);
    if (TableType.OFFLINE == tableType) {
      throw new WebApplicationException("Cannot get consuming segment info for OFFLINE table: " + realtimeTableName);
    }
    String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(realtimeTableName);

    List<SegmentConsumerInfo> segmentConsumerInfoList = new ArrayList<>();
    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);
    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    try {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        if (segmentDataManager instanceof RealtimeSegmentDataManager) {
          RealtimeSegmentDataManager realtimeSegmentDataManager = (RealtimeSegmentDataManager) segmentDataManager;
          Map<String, ConsumerPartitionState> partitionStateMap =
              realtimeSegmentDataManager.getConsumerPartitionState();
          Map<String, String> recordsLagMap = new HashMap<>();
          Map<String, String> availabilityLagMsMap = new HashMap<>();
          realtimeSegmentDataManager.getPartitionToLagState(partitionStateMap).forEach((k, v) -> {
            recordsLagMap.put(k, v.getRecordsLag());
            availabilityLagMsMap.put(k, v.getAvailabilityLagMs());
          });
          @Deprecated
          Map<String, String> partitiionToOffsetMap = realtimeSegmentDataManager.getPartitionToCurrentOffset();
          segmentConsumerInfoList.add(new SegmentConsumerInfo(segmentDataManager.getSegmentName(),
              realtimeSegmentDataManager.getConsumerState().toString(),
              realtimeSegmentDataManager.getLastConsumedTimestamp(), partitiionToOffsetMap,
              new SegmentConsumerInfo.PartitionOffsetInfo(partitiionToOffsetMap, partitionStateMap.entrySet().stream()
                  .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getUpstreamLatestOffset().toString())),
                  recordsLagMap, availabilityLagMsMap)));
        }
      }
    } catch (Exception e) {
      throw new WebApplicationException("Caught exception when getting consumer info for table: " + realtimeTableName);
    } finally {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        tableDataManager.releaseSegment(segmentDataManager);
      }
    }
    return segmentConsumerInfoList;
  }

  @GET
  @Path("tables/{tableNameWithType}/allSegmentsLoaded")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Validates if the ideal state matches with the segment state on this server", notes =
      "Validates if the ideal state matches with the segment state on this server")
  public TableSegmentValidationInfo validateTableSegmentState(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableNameWithType")
      String tableNameWithType) {
    // Get table current ideal state
    IdealState tableIdealState = HelixHelper.getTableIdealState(_serverInstance.getHelixManager(), tableNameWithType);
    TableDataManager tableDataManager =
        ServerResourceUtils.checkGetTableDataManager(_serverInstance, tableNameWithType);

    // Validate segments in ideal state which belong to this server
    long maxEndTimeMs = -1;
    Map<String, Map<String, String>> instanceStatesMap = tableIdealState.getRecord().getMapFields();
    for (Map.Entry<String, Map<String, String>> entry : instanceStatesMap.entrySet()) {
      String segmentState = entry.getValue().get(_instanceId);
      if (segmentState != null) {
        // Segment hosted by this server. Validate segment state
        String segmentName = entry.getKey();
        SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
        try {
          switch (segmentState) {
            case SegmentStateModel.CONSUMING:
              // Only validate presence of segment
              if (segmentDataManager == null) {
                return new TableSegmentValidationInfo(false, -1);
              }
              break;
            case SegmentStateModel.ONLINE:
              // Validate segment CRC
              SegmentZKMetadata zkMetadata =
                  ZKMetadataProvider.getSegmentZKMetadata(_serverInstance.getHelixManager().getHelixPropertyStore(),
                      tableNameWithType, segmentName);
              Preconditions.checkState(zkMetadata != null,
                  "Segment zk metadata not found for segment : " + segmentName);
              if (segmentDataManager == null || !segmentDataManager.getSegment().getSegmentMetadata().getCrc()
                  .equals(String.valueOf(zkMetadata.getCrc()))) {
                return new TableSegmentValidationInfo(false, -1);
              }
              maxEndTimeMs = Math.max(maxEndTimeMs, zkMetadata.getEndTimeMs());
              break;
            default:
              break;
          }
        } finally {
          if (segmentDataManager != null) {
            tableDataManager.releaseSegment(segmentDataManager);
          }
        }
      }
    }
    return new TableSegmentValidationInfo(true, maxEndTimeMs);
  }
}
