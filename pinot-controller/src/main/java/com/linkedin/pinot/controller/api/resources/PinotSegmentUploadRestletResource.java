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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.helix.model.IdealState;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.joda.time.Interval;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metrics.ControllerMeter;
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
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import com.linkedin.pinot.controller.validation.StorageQuotaChecker;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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


public class PinotSegmentUploadRestletResource {
  public static Logger LOGGER = LoggerFactory.getLogger(PinotSegmentUploadRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  ControllerConf _controllerConf;

  @Inject
  HttpConnectionManager _connectionManager;

  @Inject
  Executor _executor;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments")
  @Deprecated
  public JSONArray listAllSegmentNames(
  ) {
    try {
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
      return ret;
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}")
  @ApiOperation(value = "Lists names of all segments of a table", notes = "Lists names of all segment names of a table")
  public JSONArray listAllSegmentNames(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) {
    try {
      JSONArray ret = new JSONArray();
      final String realtime = "REALTIME";
      final String offline = "OFFLINE";

      if (tableTypeStr == null) {
        ret.put(formatSegments(tableName, CommonConstants.Helix.TableType.valueOf(offline)));
        ret.put(formatSegments(tableName, CommonConstants.Helix.TableType.valueOf(realtime)));
      } else {
        ret.put(formatSegments(tableName, CommonConstants.Helix.TableType.valueOf(tableTypeStr)));
      }
      return ret;
    } catch (Exception e) {
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }


  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Path("/segments/{tableName}/{segmentName}")
  @ApiOperation(value = "Download a segment", notes = "Download a segment")
  public Response downloadSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") String segmentName,
      @ApiParam(value = "realtime|offline", required = false) @QueryParam("type") String tableTypeStr
  ) {
    try {
      FileUploadPathProvider provider = new FileUploadPathProvider(_controllerConf);
      segmentName = URLDecoder.decode(segmentName, "UTF-8");
      final File dataFile = new File(provider.getBaseDataDir(), StringUtil.join("/", tableName, segmentName));
      if (!dataFile.exists()) {
        throw new WebApplicationException("Segment " + segmentName + " or table " + tableName + " not found",
            Response.Status.NOT_FOUND);
      }
      Response.ResponseBuilder builder = Response.ok(dataFile);
      builder.header("Content-Disposition", "attachment; filename=" + dataFile.getName());
      return builder.build();
    } catch (Exception e) {
      throw new WebApplicationException(e);
    }
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}/{segmentName}")
  @ApiOperation(value = "Deletes a segment", notes = "Deletes a segment")
  public SuccessResponse deleteOneSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") String segmentName,
      @ApiParam(value = "realtime|offline", required = true) @QueryParam("type") String tableTypeStr
  ) {
    // TODO Use the Enable|Disable|Drop code to set the state to "drop"
    // May be move this API to that file?

    throw new WebApplicationException("Not implemented", Response.Status.INTERNAL_SERVER_ERROR);
//    return new SuccessResponse("Not yet implemented");
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}")
  @ApiOperation(value = "Deletes all segments of a table", notes = "Deletes all segments of a table")
  public SuccessResponse deleteAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = true) @QueryParam("type") String tableTypeStr
  ) {
    // TODO Use the Enable|Disable|Drop code to set the state to "drop"
    // May be move this API to that file?

    throw new WebApplicationException("Not implemented", Response.Status.INTERNAL_SERVER_ERROR);
//    return new SuccessResponse("Not yet implemented");
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/segments")
  @ApiOperation(value = "Upload a segment", notes = "Upload a segment")
  // TODO Does it even work if the segment is sent as a JSON body? Need to compare with the other API
  public Response uploadSegment(
      FormDataMultiPart multiPart,
      String segmentJsonStr,    // If segment is present as json body
      @Context HttpHeaders headers,
      @Context HttpServletRequest request
  ) {
    File dataFile = null;
    String downloadURI = null;
    boolean found = false;
    FileOutputStream os = null;
    InputStream is = null;

    try {
      FileUploadPathProvider provider = new FileUploadPathProvider(_controllerConf);
      File tempDir = provider.getFileUploadTmpDir();

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

      switch (uploadType) {
        case JSON:
        case URI:
          // Download segment from the given Uri
          try {
            downloadURI = getDownloadUri(uploadType, headers, segmentJsonStr);
          } catch (Exception e) {
            String errorMsg = String
                .format("Failed to get download Uri for upload file type: %s, with error %s", uploadType,
                    e.getMessage());
            LOGGER.warn(errorMsg);
            ControllerRestApplication.getControllerMetrics()
                .addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
            throw new WebApplicationException(errorMsg, Response.Status.BAD_REQUEST);
          }

          SegmentFetcher segmentFetcher = null;
          // Get segmentFetcher based on uri parsed from download uri
          try {
            segmentFetcher = SegmentFetcherFactory.getSegmentFetcherBasedOnURI(downloadURI);
          } catch (Exception e) {
            String errorMsg = String.format("Failed to get SegmentFetcher from download Uri: %s", downloadURI);
            LOGGER.warn(errorMsg);
            ControllerRestApplication.getControllerMetrics()
                .addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
            throw new WebApplicationException(errorMsg, Response.Status.INTERNAL_SERVER_ERROR);
          }
          // Download segment tar to local.
          dataFile = new File(tempDir, "tmp-" + System.nanoTime());
          dataFile.deleteOnExit();
          try {
            segmentFetcher.fetchSegmentToLocal(downloadURI, dataFile);
          } catch (Exception e) {
            String errorMsg = String
                .format("Failed to fetch segment tar from download Uri: %s to %s", downloadURI, dataFile.toString());
            LOGGER.warn(errorMsg);
            ControllerRestApplication.getControllerMetrics()
                .addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
            throw new WebApplicationException(errorMsg, Response.Status.INTERNAL_SERVER_ERROR);
          }
          if (dataFile.exists() && dataFile.length() > 0) {
            found = true;
          }
          break;
        case TAR:
        default:
          Map<String, List<FormDataBodyPart>> map = multiPart.getFields();
          if (!validateMultiPart(map, "UNKNOWN")) {
            throw new WebApplicationException("Invalid multi-part form", Response.Status.BAD_REQUEST);
          }
          final String partName = map.keySet().iterator().next();
          final FormDataBodyPart bodyPart = map.get(partName).get(0);
          is = bodyPart.getValueAs(InputStream.class);
          dataFile = new File(provider.getFileUploadTmpDir(), partName + UUID.randomUUID().toString());
          dataFile.deleteOnExit();
          os = new FileOutputStream(dataFile);
          IOUtils.copyLarge(is, os);
          os.flush();
          os.close();
          is.close();
          found = true;
      }

      if (found) {
        File tempUntarredPath = provider.getTmpUntarredPath();
        File tmpSegmentDir = new File(tempUntarredPath,
            dataFile.getName() + "-" + _controllerConf.getControllerHost() + "_" + _controllerConf.getControllerPort() + "-" + System.currentTimeMillis());
        LOGGER.info("Untar segment to temp dir: " + tmpSegmentDir);
        tmpSegmentDir.deleteOnExit();
        if (tmpSegmentDir.exists()) {
          FileUtils.deleteDirectory(tmpSegmentDir);
        }
        if (!tmpSegmentDir.exists()) {
          tmpSegmentDir.mkdirs();
        }
        // While there is TarGzCompressionUtils.unTarOneFile, we use unTar here to unpack all files
        // in the segment in order to ensure the segment is not corrupted
        TarGzCompressionUtils.unTar(dataFile, tmpSegmentDir);
        File segmentFile = tmpSegmentDir.listFiles()[0];
        String clientAddrString = request.getRemoteAddr();
        String clientAddress = InetAddress.getByName(clientAddrString).getHostName();
        LOGGER.info("Processing upload request for segment '{}' from client '{}'", segmentFile.getName(), clientAddress);
        PinotResourceManagerResponse resourceManagerResponse =  uploadSegment(segmentFile, dataFile, downloadURI, provider);
        Response.ResponseBuilder builder = Response.ok();
        builder.header(FileUploadPathProvider.HDR_CONTROLLER_HOST, FileUploadPathProvider.getControllerHostName());
        builder.header(FileUploadPathProvider.HDR_CONTROLLER_VERSION, FileUploadPathProvider.getHdrControllerVersion());
        return builder.build();
      } else {
        // Some problem happened, sent back a simple line of text.
        String errorMsg = "No file was uploaded";
        LOGGER.warn(errorMsg);
        ControllerRestApplication.getControllerMetrics()
            .addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
        throw new WebApplicationException(errorMsg, Response.Status.INTERNAL_SERVER_ERROR);
      }
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      throw new WebApplicationException(e);
    } finally {
        try {
          if (os != null) {
            os.close();
          }
          if (is != null) {
            is.close();
          }
        } catch (Exception e) {
          LOGGER.error("Could not close input or output stream");
        }
    }
  }

  private PinotResourceManagerResponse uploadSegment(File indexDir, File dataFile, String downloadUrl,FileUploadPathProvider provider)
      throws ConfigurationException, IOException, JSONException {
    final SegmentMetadata metadata = new SegmentMetadataImpl(indexDir);
    final File tableDir = new File(provider.getBaseDataDir(), metadata.getTableName());
    String tableName = metadata.getTableName();
    File segmentFile = new File(tableDir, dataFile.getName());
    TableConfig offlineTableConfig =
        ZKMetadataProvider.getOfflineTableConfig(_pinotHelixResourceManager.getPropertyStore(), tableName);

    if (offlineTableConfig == null) {
      LOGGER.info("Missing configuration for table: {} in helix", metadata.getTableName());
      throw new WebApplicationException("Missing table: " + tableName, Response.Status.NOT_FOUND);
    }
    StorageQuotaChecker.QuotaCheckerResponse quotaResponse = checkStorageQuota(indexDir, metadata, offlineTableConfig);
    if (!quotaResponse.isSegmentWithinQuota) {
      // this is not an "error" hence we don't increment segment upload errors
      LOGGER.info("Rejecting segment upload for table: {}, segment: {}, reason: {}", metadata.getTableName(),
          metadata.getName(), quotaResponse.reason);
      throw new WebApplicationException(quotaResponse.reason, Response.Status.FORBIDDEN);
    }

    PinotResourceManagerResponse response;
    if (!isSegmentTimeValid(metadata)) {
      response = new PinotResourceManagerResponse("Invalid segment start/end time", false);
    } else {
      if (downloadUrl == null) {
        // We only move segment file to data directory when Pinot Controller will
        // serve the data downloading from Pinot Servers.
        if (segmentFile.exists()) {
          FileUtils.deleteQuietly(segmentFile);
        }
        FileUtils.moveFile(dataFile, segmentFile);
        downloadUrl = ControllerConf.constructDownloadUrl(tableName, dataFile.getName(), provider.getVip());
      }
      // TODO: this will read table configuration again from ZK. We should optimize that
      response = _pinotHelixResourceManager.addSegment(metadata, downloadUrl);
    }

    if (!response.isSuccessful()) {
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(
          ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
      throw new WebApplicationException("Error uploading segment", Response.Status.INTERNAL_SERVER_ERROR);
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
    TableSizeReader tableSizeReader = new TableSizeReader(_executor, _connectionManager, _pinotHelixResourceManager);
    // TODO: FIXME: pass tableSizeReader from correct package instead of null
    StorageQuotaChecker quotaChecker = new StorageQuotaChecker(offlineTableConfig, null);
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
      Date startDate = new Date(interval.getStartMillis());
      Date endDate = new Date(interval.getEndMillis());

      Date minDate = new Date(TimeUtils.getValidMinTimeMillis());
      Date maxDate = new Date(TimeUtils.getValidMaxTimeMillis());

      LOGGER.error("Invalid start time '{}' or end time '{}' for segment, must be between '{}' and '{}'", startDate,
          endDate, minDate, maxDate);
      return false;
    }

    return true;
  }

}
