/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.fetcher.SegmentFetcher;
import com.linkedin.pinot.common.segment.fetcher.SegmentFetcherFactory;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import com.linkedin.pinot.controller.util.TableSizeReader;
import com.linkedin.pinot.controller.validation.StorageQuotaChecker;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.helix.model.IdealState;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.joda.time.Interval;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.SEGMENT_TAG)
@Path("/")
public class PinotSegmentUploadRestletResource {
  public static Logger LOGGER = LoggerFactory.getLogger(PinotSegmentUploadRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  ControllerConf _controllerConf;

  @Inject
  ControllerMetrics _controllerMetrics;

  @Inject
  HttpConnectionManager _connectionManager;

  @Inject
  Executor _executor;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments")
  @Deprecated
  public String listAllSegmentNames(
  ) throws Exception {
    FileUploadPathProvider provider = new FileUploadPathProvider(_controllerConf);
    final JSONArray ret = new JSONArray();
    for (final File file : provider.getBaseDataDir().listFiles()) {
      final String fileName = file.getName();
      if (fileName.equalsIgnoreCase("fileUploadTemp") || fileName.equalsIgnoreCase("schemasTemp")) {
        continue;
      }

      final String url = _controllerConf.generateVipUrl() + "/segments/" + fileName;
      ret.put(url);
    }
    return ret.toString();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}")
  @ApiOperation(value = "Lists names of all segments of a table", notes = "Lists names of all segment names of a table")
  public String listAllSegmentNames(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) throws Exception {
    JSONArray ret = new JSONArray();
    final String realtime = "REALTIME";
    final String offline = "OFFLINE";

    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableTypeStr == null) {
      ret.put(formatSegments(tableName, CommonConstants.Helix.TableType.OFFLINE));
      ret.put(formatSegments(tableName, CommonConstants.Helix.TableType.REALTIME));
    } else {
      ret.put(formatSegments(tableName, tableType));
    }
    return ret.toString();
  }


  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Path("/segments/{tableName}/{segmentName}")
  @ApiOperation(value = "Download a segment", notes = "Download a segment")
  public Response downloadSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) {
    FileUploadPathProvider provider;
    try {
      provider = new FileUploadPathProvider(_controllerConf);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    try {
      segmentName = URLDecoder.decode(segmentName, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      String errStr = "Could not decode segment name '" + segmentName + "'";
      throw new ControllerApplicationException(LOGGER, errStr, Response.Status.BAD_REQUEST);
    }
    final File dataFile = new File(provider.getBaseDataDir(), StringUtil.join("/", tableName, segmentName));
    if (!dataFile.exists()) {
      throw new ControllerApplicationException(LOGGER, "Segment " + segmentName + " or table " + tableName + " not found",
          Response.Status.NOT_FOUND);
    }
    Response.ResponseBuilder builder = Response.ok(dataFile);
    builder.header("Content-Disposition", "attachment; filename=" + dataFile.getName());
    builder.header("Content-Length", dataFile.length());
    return builder.build();
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}/{segmentName}")
  @ApiOperation(value = "Deletes a segment", notes = "Deletes a segment")
  public SuccessResponse deleteOneSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "realtime|offline", required = true) @QueryParam("type") String tableTypeStr
  ) {
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == null) {
      throw new ControllerApplicationException(LOGGER, "Table type must not be null", Response.Status.BAD_REQUEST);
    }
    try {
      segmentName = URLDecoder.decode(segmentName, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      String errStr = "Could not decode segment name '" + segmentName + "'";
      throw new ControllerApplicationException(LOGGER, errStr, Response.Status.BAD_REQUEST);
    }
    PinotSegmentRestletResource.toggleStateInternal(tableName, StateType.DROP, tableType, segmentName, _pinotHelixResourceManager);

    return new SuccessResponse("Segment deleted");
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}")
  @ApiOperation(value = "Deletes all segments of a table", notes = "Deletes all segments of a table")
  public SuccessResponse deleteAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = true) @QueryParam("type") String tableTypeStr
  ) {
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == null) {
      throw new ControllerApplicationException(LOGGER, "Table type must not be null", Response.Status.BAD_REQUEST);
    }
    PinotSegmentRestletResource.toggleStateInternal(tableName, StateType.DROP, tableType, null, _pinotHelixResourceManager);

    return new SuccessResponse("All segments of table " + TableNameBuilder.forType(tableType).tableNameWithType(tableName) + " deleted");
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/segments")
  @ApiOperation(value = "Upload a segment", notes = "Upload a segment as binary")
  public SuccessResponse uploadSegmentAsMultiPart(
      FormDataMultiPart multiPart,
      @Context HttpHeaders headers,
      @Context Request request
  ) {
    return uploadSegmentInternal(multiPart, null, headers, request);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/segments")
  @ApiOperation(value = "Upload a segment", notes = "Upload a segment as json")
  // TODO Does it even work if the segment is sent as a JSON body? Need to compare with the other API
  public SuccessResponse uploadSegmentAsJson(
      String segmentJsonStr,    // If segment is present as json body
      @Context HttpHeaders headers,
      @Context Request request
  ) {
    return uploadSegmentInternal(null, segmentJsonStr, headers, request);
  }

  private SuccessResponse uploadSegmentInternal(FormDataMultiPart multiPart, String segmentJsonStr, HttpHeaders headers,
      Request request) {
    File tempTarredSegmentFile = null;
    File tempSegmentDir = null;

    try {
      FileUploadPathProvider provider = new FileUploadPathProvider(_controllerConf);
      String tempSegmentName = "tmp-" + System.nanoTime();
      tempTarredSegmentFile = new File(provider.getFileUploadTmpDir(), tempSegmentName);
      tempTarredSegmentFile.deleteOnExit();
      tempSegmentDir = new File(provider.getTmpUntarredPath(), tempSegmentName);
      tempSegmentDir.deleteOnExit();

      // Get upload type
      List<String> uploadTypes = headers.getRequestHeader(FileUploadUtils.UPLOAD_TYPE);
      FileUploadUtils.FileUploadType uploadType = FileUploadUtils.FileUploadType.getDefaultUploadType();
      if (uploadTypes != null && uploadTypes.size() > 0) {
        String uploadTypeStr = uploadTypes.get(0);
        try {
          uploadType = FileUploadUtils.FileUploadType.valueOf(uploadTypeStr);
        } catch (Exception e) {
          // Ignore, since default is already set.
        }
      }

      String downloadURI = null;
      boolean found = false;
      switch (uploadType) {
        case JSON:
        case URI:
          // Get download URI
          try {
            downloadURI = getDownloadUri(uploadType, headers, segmentJsonStr);
          } catch (Exception e) {
            String errorMsg =
                String.format("Failed to get download Uri for upload file type: %s, with error %s", uploadType,
                    e.getMessage());
            _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
            throw new ControllerApplicationException(LOGGER, errorMsg, Response.Status.BAD_REQUEST);
          }

          // Get segment fetcher based on the download URI
          SegmentFetcher segmentFetcher;
          try {
            segmentFetcher = SegmentFetcherFactory.getSegmentFetcherBasedOnURI(downloadURI);
          } catch (Exception e) {
            String errorMsg = String.format("Failed to get SegmentFetcher from download Uri: %s", downloadURI);
            _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
            throw new ControllerApplicationException(LOGGER, errorMsg, Response.Status.INTERNAL_SERVER_ERROR);
          }

          // Download segment tar to local.
          tempTarredSegmentFile.deleteOnExit();
          try {
            segmentFetcher.fetchSegmentToLocal(downloadURI, tempTarredSegmentFile);
          } catch (Exception e) {
            String errorMsg = String.format("Failed to fetch segment tar from download Uri: %s to %s", downloadURI,
                tempTarredSegmentFile.toString());
            _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
            throw new ControllerApplicationException(LOGGER, errorMsg, Response.Status.INTERNAL_SERVER_ERROR);
          }
          if (tempTarredSegmentFile.length() > 0) {
            found = true;
          }
          break;

        case TAR:
          Map<String, List<FormDataBodyPart>> map = multiPart.getFields();
          if (!validateMultiPart(map, "UNKNOWN")) {
            throw new ControllerApplicationException(LOGGER, "Invalid multi-part form", Response.Status.BAD_REQUEST);
          }
          String partName = map.keySet().iterator().next();
          FormDataBodyPart bodyPart = map.get(partName).get(0);
          try (InputStream inputStream = bodyPart.getValueAs(InputStream.class);
              FileOutputStream outputStream = new FileOutputStream(tempTarredSegmentFile)) {
            IOUtils.copyLarge(inputStream, outputStream);
            outputStream.flush();
          }
          if (tempTarredSegmentFile.length() > 0) {
            found = true;
          }
          break;

        default:
          throw new UnsupportedOperationException("Unsupported upload type: " + uploadType);
      }

      if (found) {
        // Found the data file

        // While there is TarGzCompressionUtils.unTarOneFile, we use unTar here to unpack all files
        // in the segment in order to ensure the segment is not corrupted
        TarGzCompressionUtils.unTar(tempTarredSegmentFile, tempSegmentDir);
        File[] files = tempSegmentDir.listFiles();
        Preconditions.checkState(files != null && files.length == 1);
        File indexDir = files[0];

        SegmentMetadata segmentMetadata = new SegmentMetadataImpl(indexDir);
        String clientAddress = InetAddress.getByName(request.getRemoteAddr()).getHostName();
        LOGGER.info("Processing upload request for segment '{}' from client '{}'", segmentMetadata.getName(),
            clientAddress);
        uploadSegment(indexDir, segmentMetadata, tempTarredSegmentFile, downloadURI, provider);
        return new SuccessResponse("success"); // Current APIs return an empty status string on success.
      } else {
        // Some problem happened, sent back a simple line of text.
        String errorMsg = "No file was uploaded";
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
        throw new ControllerApplicationException(LOGGER, errorMsg, Response.Status.INTERNAL_SERVER_ERROR);
      }
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    } finally {
      FileUtils.deleteQuietly(tempTarredSegmentFile);
      FileUtils.deleteQuietly(tempSegmentDir);
    }
  }

  private PinotResourceManagerResponse uploadSegment(File indexDir, SegmentMetadata segmentMetadata,
      File tempTarredSegmentFile, String downloadUrl, FileUploadPathProvider provider)
      throws IOException, JSONException {
    String tableName = segmentMetadata.getTableName();
    String segmentName = segmentMetadata.getName();
    TableConfig offlineTableConfig =
        ZKMetadataProvider.getOfflineTableConfig(_pinotHelixResourceManager.getPropertyStore(), tableName);

    if (offlineTableConfig == null) {
      throw new ControllerApplicationException(LOGGER, "Missing table: " + tableName, Response.Status.NOT_FOUND);
    }

    StorageQuotaChecker.QuotaCheckerResponse quotaResponse =
        checkStorageQuota(indexDir, segmentMetadata, offlineTableConfig);
    if (!quotaResponse.isSegmentWithinQuota) {
      // this is not an "error" hence we don't increment segment upload errors
      String errStr = "Rejecting segment upload for table: " + tableName + "segment: " +
          segmentName + "reason: {}" + quotaResponse.reason;
      throw new ControllerApplicationException(LOGGER, errStr, Response.Status.FORBIDDEN);
    }

    PinotResourceManagerResponse response;
    if (!isSegmentTimeValid(segmentMetadata)) {
      response = new PinotResourceManagerResponse("Invalid segment start/end time", false);
    } else {
      // Move tarred segment file to data directory when there is no external download URL
      if (downloadUrl == null) {
        File tarredSegmentFile = new File(new File(provider.getBaseDataDir(), tableName), segmentName);
        FileUtils.deleteQuietly(tarredSegmentFile);
        FileUtils.moveFile(tempTarredSegmentFile, tarredSegmentFile);
        downloadUrl = ControllerConf.constructDownloadUrl(tableName, segmentName, provider.getVip());
      }
      // TODO: this will read table configuration again from ZK. We should optimize that
      response = _pinotHelixResourceManager.addSegment(segmentMetadata, downloadUrl);
    }

    if (!response.isSuccessful()) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, "Error uploading segment", Response.Status.INTERNAL_SERVER_ERROR);
    }
    return response;
  }

  private String getDownloadUri(FileUploadUtils.FileUploadType uploadType, HttpHeaders headers, String segmentJsonStr) throws Exception {
    switch (uploadType) {
      case URI:
        return headers.getRequestHeader(FileUploadUtils.DOWNLOAD_URI).get(0);
      case JSON:
        // Get segmentJsonStr
        JSONTokener tokener = new JSONTokener(segmentJsonStr);
        JSONObject segmentJson = new JSONObject(tokener);
        // Download segment from the given Uri
        return segmentJson.getString(CommonConstants.Segment.Offline.DOWNLOAD_URL);
      default:
        break;
    }
    throw new UnsupportedOperationException("Not support getDownloadUri method for upload type - " + uploadType);
  }

  private org.json.JSONObject formatSegments(String tableName, CommonConstants.Helix.TableType tableType) throws Exception {
    org.json.JSONObject obj = new org.json.JSONObject();
    obj.put(tableType.toString(), getSegments(tableName, tableType.toString()));
    return obj;
  }

  private JSONArray getSegments(String tableName, String tableType) {

    final JSONArray ret = new JSONArray();

    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);

    String tableNameWithType;
    if (CommonConstants.Helix.TableType.valueOf(tableType).toString().equals("REALTIME")) {
      tableNameWithType = realtimeTableName;
    } else {
      tableNameWithType = offlineTableName;
    }

    List<String> segmentList = _pinotHelixResourceManager.getSegmentsFor(tableNameWithType);
    IdealState idealState =
        HelixHelper.getTableIdealState(_pinotHelixResourceManager.getHelixZkManager(), tableNameWithType);

    for (String segmentName : segmentList) {
      Map<String, String> map = idealState.getInstanceStateMap(segmentName);
      if (map == null) {
        continue;
      }
      if (!map.containsValue(PinotHelixSegmentOnlineOfflineStateModelGenerator.OFFLINE_STATE)) {
        ret.put(segmentName);
      }
    }

    return ret;
  }

  // Validate that there is one file that is in the input.
  public static boolean validateMultiPart(Map<String, List<FormDataBodyPart>> map, String segmentName) {
    boolean isGood = true;
    if (map.size() != 1) {
      LOGGER.warn("Incorrect number of multi-part elements: {} (segmentName {}). Picking one", map.size(), segmentName);
      isGood = false;
    }
    List<FormDataBodyPart> bodyParts = map.get(map.keySet().iterator().next());
    if (bodyParts.size() != 1) {
      LOGGER.warn("Incorrect number of elements in list in first part: {} (segmentName {}). Picking first one", bodyParts.size(), segmentName);
      isGood = false;
    }
    return isGood;
  }

  /**
   * check if the segment represented by segmentFile is within the storage quota
   * @param segmentFile untarred segment. This should not be null.
   *                    segmentFile must exist on disk and must be a directory
   * @param metadata segment metadata. This should not be null
   */
  private StorageQuotaChecker.QuotaCheckerResponse checkStorageQuota(@Nonnull File segmentFile,
      @Nonnull SegmentMetadata metadata, @Nonnull TableConfig offlineTableConfig) {
    TableSizeReader
        tableSizeReader = new TableSizeReader(_executor, _connectionManager, _pinotHelixResourceManager);
    StorageQuotaChecker quotaChecker = new StorageQuotaChecker(offlineTableConfig, tableSizeReader);
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(metadata.getTableName());
    return quotaChecker.isSegmentStorageWithinQuota(segmentFile, offlineTableName, metadata.getName(),
        _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
  }

  /**
   * Returns true if:
   * - Segment does not have a start/end time, OR
   * - The start/end time are in a valid range (Jan 01 1971 - Jan 01, 2071)
   * @param metadata
   * @return
   */
  private boolean isSegmentTimeValid(SegmentMetadata metadata) {
    Interval interval = metadata.getTimeInterval();
    if (interval == null) {
      return true;
    }

    long startMillis = interval.getStartMillis();
    long endMillis = interval.getEndMillis();

    if (!TimeUtils.timeValueInValidRange(startMillis) || !TimeUtils.timeValueInValidRange(endMillis)) {
      Date minDate = new Date(TimeUtils.getValidMinTimeMillis());
      Date maxDate = new Date(TimeUtils.getValidMaxTimeMillis());

      LOGGER.error("Invalid start time '{}ms' or end time '{}ms' for segment {}, must be between '{}' and '{}' (timecolumn {}, timeunit {})",
          interval.getStartMillis(), interval.getEndMillis(), metadata.getName(), minDate, maxDate, metadata.getTimeColumn(),
          metadata.getTimeUnit().toString());
      return false;
    }

    return true;
  }

}
