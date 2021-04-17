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
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.inject.Inject;
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
import org.apache.pinot.common.restlet.resources.ResourceUtils;
import org.apache.pinot.common.restlet.resources.SegmentConsumerInfo;
import org.apache.pinot.common.restlet.resources.TableSegments;
import org.apache.pinot.common.restlet.resources.TablesList;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.manager.TableDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.SegmentUploader;
import org.apache.pinot.segment.local.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.server.api.access.AccessControl;
import org.apache.pinot.server.api.access.AccessControlFactory;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = "Table")
@Path("/")
public class TablesResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(TablesResource.class);
  private static final String PEER_SEGMENT_DOWNLOAD_DIR = "peerSegmentDownloadDir";
  private static final String SEGMENT_UPLOAD_DIR = "segmentUploadDir";

  @Inject
  private ServerInstance _serverInstance;

  @Inject
  private AccessControlFactory _accessControlFactory;

  @GET
  @Path("/tables")
  @Produces(MediaType.APPLICATION_JSON)
  //swagger annotations
  @ApiOperation(value = "List tables", notes = "List all the tables on this server")
  @ApiResponses(
      value = {@ApiResponse(code = 200, message = "Success", response = TablesList.class), @ApiResponse(code = 500,
          message = "Server initialization error", response = ErrorInfo.class)})
  public String listTables() {
    InstanceDataManager instanceDataManager = checkGetInstanceDataManager();
    List<String> tables = new ArrayList<>(instanceDataManager.getAllTables());
    return ResourceUtils.convertToJsonString(new TablesList(tables));
  }

  private InstanceDataManager checkGetInstanceDataManager() {
    if (_serverInstance == null) {
      throw new WebApplicationException("Server initialization error. Missing server instance");
    }
    InstanceDataManager instanceDataManager = _serverInstance.getInstanceDataManager();
    if (instanceDataManager == null) {
      throw new WebApplicationException("Server initialization error. Missing data manager",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return instanceDataManager;
  }

  private TableDataManager checkGetTableDataManager(String tableName) {
    InstanceDataManager dataManager = checkGetInstanceDataManager();
    TableDataManager tableDataManager = dataManager.getTableDataManager(tableName);
    if (tableDataManager == null) {
      throw new WebApplicationException("Table " + tableName + " does not exist", Response.Status.NOT_FOUND);
    }
    return tableDataManager;
  }

  @GET
  @Path("/tables/{tableName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List table segments", notes = "List segments of table hosted on this server")
  @ApiResponses(
      value = {@ApiResponse(code = 200, message = "Success", response = TableSegments.class), @ApiResponse(code = 500,
          message = "Server initialization error", response = ErrorInfo.class)})
  public String listTableSegments(@ApiParam(value = "Table name including type", required = true,
      example = "myTable_OFFLINE") @PathParam("tableName") String tableName) {
    TableDataManager tableDataManager = checkGetTableDataManager(tableName);
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
  @Path("/tables/{tableName}/segments/{segmentName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Provide segment metadata", notes = "Provide segments metadata for the segment on server")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500,
      message = "Internal server error", response = ErrorInfo.class), @ApiResponse(code = 404,
          message = "Table or segment not found", response = ErrorInfo.class)})
  public String getSegmentMetadata(
      @ApiParam(value = "Table name including type", required = true,
          example = "myTable_OFFLINE") @PathParam("tableName") String tableName,
      @ApiParam(value = "Segment name", required = true) @PathParam("segmentName") String segmentName,
      @ApiParam(value = "Column name",
          allowMultiple = true) @QueryParam("columns") @DefaultValue("") List<String> columns) {
    TableDataManager tableDataManager = checkGetTableDataManager(tableName);
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
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500,
      message = "Internal server error", response = ErrorInfo.class), @ApiResponse(code = 404,
          message = "Table or segment not found", response = ErrorInfo.class)})
  public String getCrcMetadataForTable(@ApiParam(value = "Table name including type", required = true,
      example = "myTable_OFFLINE") @PathParam("tableName") String tableName) {
    TableDataManager tableDataManager = checkGetTableDataManager(tableName);
    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    try {
      Map<String, String> segmentCrcForTable = new HashMap<>();
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        SegmentMetadataImpl segmentMetadata =
            (SegmentMetadataImpl) segmentDataManager.getSegment().getSegmentMetadata();
        segmentCrcForTable.put(segmentDataManager.getSegmentName(), segmentMetadata.getCrc());
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
      @ApiParam(value = "Name of the table with type REALTIME OR OFFLINE", required = true,
          example = "myTable_OFFLINE") @PathParam("tableNameWithType") String tableNameWithType,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @Context HttpHeaders httpHeaders) throws Exception {
    LOGGER.info("Received a request to download segment {} for table {}", segmentName, tableNameWithType);
    // Validate data access
    boolean hasDataAccess;
    try {
      AccessControl accessControl = _accessControlFactory.create();
      hasDataAccess = accessControl.hasDataAccess(httpHeaders, tableNameWithType);
    } catch (Exception e) {
      throw new WebApplicationException("Caught exception while validating access to table: " + tableNameWithType,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    if (!hasDataAccess) {
      throw new WebApplicationException("No data access to table: " + tableNameWithType, Response.Status.FORBIDDEN);
    }

    TableDataManager tableDataManager = checkGetTableDataManager(tableNameWithType);
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

      File segmentTarFile = new File(tmpSegmentTarDir, tableNameWithType + "_" + segmentName + "_" + UUID.randomUUID()
          + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
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
   * Upload a low level consumer segment to segment store and return the segment download url. This endpoint is used when segment store copy is unavailable for committed low level consumer segments.
   * Please note that invocation of this endpoint may cause query performance to suffer, since we tar up the segment to upload it.
   * @see <a href="https://cwiki.apache.org/confluence/display/PINOT/By-passing+deep-store+requirement+for+Realtime+segment+completion#BypassingdeepstorerequirementforRealtimesegmentcompletion-Failurecasesandhandling">By-passing deep-store requirement for Realtime segment completion:Failure cases and handling</a>
   */
  @POST
  @Path("/segments/{realtimeTableName}/{segmentName}/upload")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Upload a low level consumer segment to segment store and return the segment download url",
      notes = "Upload a low level consumer segment to segment store and return the segment download url")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500,
      message = "Internal server error", response = ErrorInfo.class), @ApiResponse(code = 404,
          message = "Table or segment not found",
          response = ErrorInfo.class), @ApiResponse(code = 400, message = "Bad request", response = ErrorInfo.class)})
  public String uploadLLCSegment(
      @ApiParam(value = "Name of the REALTIME table",
          required = true) @PathParam("realtimeTableName") String realtimeTableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") String segmentName)
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
    if (!LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
      throw new WebApplicationException(String.format("Segment %s is not a low level consumer segment", segmentName),
          Response.Status.BAD_REQUEST);
    }

    String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(realtimeTableName);
    TableDataManager tableDataManager = checkGetTableDataManager(tableNameWithType);
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

      segmentTarFile = new File(segmentTarUploadDir, tableNameWithType + "_" + segmentName + "_" + UUID.randomUUID()
          + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
      TarGzCompressionUtils.createTarGzFile(new File(tableDataManager.getTableDataDir(), segmentName), segmentTarFile);

      // Use segment uploader to upload the segment tar file to segment store and return the segment download url.
      SegmentUploader segmentUploader = _serverInstance.getInstanceDataManager().getSegmentUploader();
      URI segmentDownloadUrl = segmentUploader.uploadSegment(segmentTarFile, new LLCSegmentName(segmentName));
      if (segmentDownloadUrl == null) {
        throw new WebApplicationException(
            String.format("Failed to upload table %s segment %s to segment store", realtimeTableName, segmentName),
            Response.Status.INTERNAL_SERVER_ERROR);
      }
      return segmentDownloadUrl.getPath();
    } finally {
      FileUtils.deleteQuietly(segmentTarFile);
      tableDataManager.releaseSegment(segmentDataManager);
    }
  }

  @GET
  @Path("tables/{realtimeTableName}/consumingSegmentsInfo")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get the info for consumers of this REALTIME table",
      notes = "Get consumers info from the table data manager")
  public List<SegmentConsumerInfo> getConsumingSegmentsInfo(@ApiParam(value = "Name of the REALTIME table",
      required = true) @PathParam("realtimeTableName") String realtimeTableName) {

    TableType tableType = TableNameBuilder.getTableTypeFromTableName(realtimeTableName);
    if (TableType.OFFLINE == tableType) {
      throw new WebApplicationException("Cannot get consuming segment info for OFFLINE table: " + realtimeTableName);
    }
    String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(realtimeTableName);

    List<SegmentConsumerInfo> segmentConsumerInfoList = new ArrayList<>();
    TableDataManager tableDataManager = checkGetTableDataManager(tableNameWithType);
    List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
    try {
      for (SegmentDataManager segmentDataManager : segmentDataManagers) {
        if (segmentDataManager instanceof RealtimeSegmentDataManager) {
          RealtimeSegmentDataManager realtimeSegmentDataManager = (RealtimeSegmentDataManager) segmentDataManager;
          String segmentName = segmentDataManager.getSegmentName();
          segmentConsumerInfoList
              .add(new SegmentConsumerInfo(segmentName, realtimeSegmentDataManager.getConsumerState().toString(),
                  realtimeSegmentDataManager.getLastConsumedTimestamp(),
                  realtimeSegmentDataManager.getPartitionToCurrentOffset()));
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
}
