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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Preconditions;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.segment.SegmentMetadata;
import org.apache.pinot.common.segment.fetcher.SegmentFetcherFactory;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerLeadershipManager;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.upload.SegmentValidator;
import org.apache.pinot.controller.api.upload.SegmentValidatorResponse;
import org.apache.pinot.controller.api.upload.ZKOperator;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import org.apache.pinot.core.crypt.NoOpPinotCrypter;
import org.apache.pinot.core.crypt.PinotCrypter;
import org.apache.pinot.core.crypt.PinotCrypterFactory;
import org.apache.pinot.core.metadata.DefaultMetadataExtractor;
import org.apache.pinot.core.metadata.MetadataExtractorFactory;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.SEGMENT_TAG)
@Path("/")
public class PinotSegmentUploadRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentUploadRestletResource.class);
  private static final String TMP_DIR_PREFIX = "tmp-";
  private static final String ENCRYPTED_SUFFIX = "_encrypted";

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

  @Inject
  AccessControlFactory _accessControlFactory;

  @Inject
  ControllerLeadershipManager _controllerLeadershipManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments")
  @Deprecated
  public String listAllSegmentNames()
      throws Exception {
    FileUploadPathProvider provider = new FileUploadPathProvider(_controllerConf);
    ArrayNode ret = JsonUtils.newArrayNode();
    for (final File file : provider.getBaseDataDir().listFiles()) {
      final String fileName = file.getName();
      if (fileName.equalsIgnoreCase("fileUploadTemp") || fileName.equalsIgnoreCase("schemasTemp")) {
        continue;
      }

      final String url = _controllerConf.generateVipUrl() + "/segments/" + fileName;
      ret.add(url);
    }
    return ret.toString();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}")
  @ApiOperation(value = "Lists names of all segments of a table", notes = "Lists names of all segment names of a table")
  public String listAllSegmentNames(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr) {
    ArrayNode ret = JsonUtils.newArrayNode();
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableTypeStr == null) {
      ret.add(formatSegments(tableName, CommonConstants.Helix.TableType.OFFLINE));
      ret.add(formatSegments(tableName, CommonConstants.Helix.TableType.REALTIME));
    } else {
      ret.add(formatSegments(tableName, tableType));
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
      @Context HttpHeaders httpHeaders) {
    // Validate data access
    boolean hasDataAccess;
    try {
      AccessControl accessControl = _accessControlFactory.create();
      hasDataAccess = accessControl.hasDataAccess(httpHeaders, tableName);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Caught exception while validating access to table: " + tableName, Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    if (!hasDataAccess) {
      throw new ControllerApplicationException(LOGGER, "No data access to table: " + tableName,
          Response.Status.FORBIDDEN);
    }

    FileUploadPathProvider provider;
    try {
      provider = new FileUploadPathProvider(_controllerConf);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    segmentName = URIUtils.decode(segmentName);
    File dataFile = new File(provider.getBaseDataDir(), String.join(File.separator, tableName, segmentName));
    if (!dataFile.exists()) {
      throw new ControllerApplicationException(LOGGER,
          "Segment " + segmentName + " or table " + tableName + " not found", Response.Status.NOT_FOUND);
    }
    Response.ResponseBuilder builder = Response.ok(dataFile);
    builder.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + dataFile.getName());
    builder.header(HttpHeaders.CONTENT_LENGTH, dataFile.length());
    return builder.build();
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}/{segmentName}")
  @ApiOperation(value = "Deletes a segment", notes = "Deletes a segment")
  public SuccessResponse deleteOneSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "realtime|offline", required = true) @QueryParam("type") String tableTypeStr) {
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == null) {
      throw new ControllerApplicationException(LOGGER, "Table type must not be null", Response.Status.BAD_REQUEST);
    }
    segmentName = URIUtils.decode(segmentName);
    PinotSegmentRestletResource
        .toggleStateInternal(tableName, StateType.DROP, tableType, segmentName, _pinotHelixResourceManager);

    return new SuccessResponse("Segment deleted");
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}")
  @ApiOperation(value = "Deletes all segments of a table", notes = "Deletes all segments of a table")
  public SuccessResponse deleteAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = true) @QueryParam("type") String tableTypeStr) {
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == null) {
      throw new ControllerApplicationException(LOGGER, "Table type must not be null", Response.Status.BAD_REQUEST);
    }
    PinotSegmentRestletResource
        .toggleStateInternal(tableName, StateType.DROP, tableType, null, _pinotHelixResourceManager);

    return new SuccessResponse(
        "All segments of table " + TableNameBuilder.forType(tableType).tableNameWithType(tableName) + " deleted");
  }

  private SuccessResponse uploadSegment(FormDataMultiPart multiPart, boolean enableParallelPushProtection,
      HttpHeaders headers, Request request, boolean moveSegmentToFinalLocation) {
    if (headers != null) {
      LOGGER.info("HTTP Header {} is {}", CommonConstants.Controller.SEGMENT_NAME_HTTP_HEADER,
          headers.getRequestHeader(CommonConstants.Controller.SEGMENT_NAME_HTTP_HEADER));
      LOGGER.info("HTTP Header {} is {}", CommonConstants.Controller.TABLE_NAME_HTTP_HEADER,
          headers.getRequestHeader(CommonConstants.Controller.TABLE_NAME_HTTP_HEADER));
    }

    // Get upload type
    String uploadTypeStr = headers.getHeaderString(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE);
    FileUploadDownloadClient.FileUploadType uploadType = getUploadType(uploadTypeStr);

    // Get crypter class
    String crypterClassHeader = headers.getHeaderString(FileUploadDownloadClient.CustomHeaders.CRYPTER);

    // Get URI of current segment location
    String currentSegmentLocationURI = headers.getHeaderString(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI);

    File tempEncryptedFile = null;
    File tempDecryptedFile = null;
    File tempSegmentDir = null;
    try {
      FileUploadPathProvider provider = new FileUploadPathProvider(_controllerConf);
      String tempFileName = TMP_DIR_PREFIX + System.nanoTime();
      tempDecryptedFile = new File(provider.getFileUploadTmpDir(), tempFileName);
      tempSegmentDir = new File(provider.getTmpUntarredPath(), tempFileName);

      // Set default crypter to the noop crypter when no crypter header is sent
      // In this case, the noop crypter will not do any operations, so the encrypted and decrypted file will have the same
      // file path.
      if (crypterClassHeader == null) {
        crypterClassHeader = NoOpPinotCrypter.class.getSimpleName();
        tempEncryptedFile = new File(provider.getFileUploadTmpDir(), tempFileName);
      } else {
        tempEncryptedFile = new File(provider.getFileUploadTmpDir(), tempFileName + ENCRYPTED_SUFFIX);
      }

      // TODO: Change when metadata upload added
      String metadataProviderClass = DefaultMetadataExtractor.class.getName();

      SegmentMetadata segmentMetadata;
      switch (uploadType) {
        case URI:
          segmentMetadata =
              getMetadataForURI(crypterClassHeader, currentSegmentLocationURI, tempEncryptedFile, tempDecryptedFile,
                  tempSegmentDir, metadataProviderClass);
          break;
        case SEGMENT:
          getFileFromMultipart(multiPart, tempDecryptedFile);
          segmentMetadata = getSegmentMetadata(crypterClassHeader, tempEncryptedFile, tempDecryptedFile, tempSegmentDir,
              metadataProviderClass);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported upload type: " + uploadType);
      }

      // Fetch raw table name. Try to derive the table name from the header and then from segment metadata
      String rawTableName;
      List<String> tableNameHeader = headers.getRequestHeader(CommonConstants.Controller.TABLE_NAME_HTTP_HEADER);
      if (tableNameHeader != null) {
        Preconditions.checkState(tableNameHeader.size() == 1);
        rawTableName = headers.getRequestHeader(CommonConstants.Controller.TABLE_NAME_HTTP_HEADER).get(0);
      } else {
        rawTableName = segmentMetadata.getTableName();
      }
      String segmentName = segmentMetadata.getName();

      String zkDownloadUri;
      // This boolean is here for V1 segment upload, where we keep the segment in the downloadURI sent in the header.
      // We will deprecate this behavior eventually.
      if (!moveSegmentToFinalLocation) {
        LOGGER.info("Setting zkDownloadUri to {} for segment {} of table {}, skipping move", currentSegmentLocationURI,
            segmentName, rawTableName);
        zkDownloadUri = currentSegmentLocationURI;
      } else {
        zkDownloadUri = getZkDownloadURIForSegmentUpload(provider, rawTableName, segmentName);
      }

      String clientAddress = InetAddress.getByName(request.getRemoteAddr()).getHostName();
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
      LOGGER
          .info("Processing upload request for segment: {} of table: {} from client: {}", segmentName, offlineTableName,
              clientAddress);

      // Validate segment
      SegmentValidatorResponse segmentValidatorResponse =
          new SegmentValidator(_pinotHelixResourceManager, _controllerConf, _executor, _connectionManager,
              _controllerMetrics, _controllerLeadershipManager)
              .validateSegment(rawTableName, segmentMetadata, tempSegmentDir);

      // Zk operations
      completeZkOperations(enableParallelPushProtection, headers, tempEncryptedFile, provider, rawTableName,
          segmentMetadata, segmentName, zkDownloadUri, moveSegmentToFinalLocation, segmentValidatorResponse);

      return new SuccessResponse("Successfully uploaded segment: " + segmentName + " of table: " + rawTableName);
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, "Caught internal server exception while uploading segment",
          Response.Status.INTERNAL_SERVER_ERROR, e);
    } finally {
      FileUtils.deleteQuietly(tempEncryptedFile);
      FileUtils.deleteQuietly(tempDecryptedFile);
      FileUtils.deleteQuietly(tempSegmentDir);
    }
  }

  private String getZkDownloadURIForSegmentUpload(FileUploadPathProvider provider, String rawTableName,
      String segmentName) {
    URI baseDataDirURI = provider.getBaseDataDirURI();
    if (baseDataDirURI.getScheme().equalsIgnoreCase(CommonConstants.Segment.LOCAL_SEGMENT_SCHEME)) {
      return URIUtils.constructDownloadUrl(provider.getVip(), rawTableName, segmentName);
    } else {
      // Receiving .tar.gz segment upload for pluggable storage
      LOGGER.info("Using configured data dir {} for segment {} of table {}", _controllerConf.getDataDir(), segmentName,
          rawTableName);
      return URIUtils.constructDownloadUrl(baseDataDirURI.toString(), rawTableName, segmentName);
    }
  }

  private SegmentMetadata getMetadataForURI(String crypterClassHeader, String currentSegmentLocationURI,
      File tempEncryptedFile, File tempDecryptedFile, File tempSegmentDir, String metadataProviderClass)
      throws Exception {
    SegmentMetadata segmentMetadata;
    if (currentSegmentLocationURI == null || currentSegmentLocationURI.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Failed to get downloadURI, needed for URI upload",
          Response.Status.BAD_REQUEST);
    }
    LOGGER.info("Downloading segment from {} to {}", currentSegmentLocationURI, tempEncryptedFile.getAbsolutePath());
    SegmentFetcherFactory.getInstance().getSegmentFetcherBasedOnURI(currentSegmentLocationURI)
        .fetchSegmentToLocal(currentSegmentLocationURI, tempEncryptedFile);
    segmentMetadata = getSegmentMetadata(crypterClassHeader, tempEncryptedFile, tempDecryptedFile, tempSegmentDir,
        metadataProviderClass);
    return segmentMetadata;
  }

  private SegmentMetadata getSegmentMetadata(String crypterClassHeader, File tempEncryptedFile, File tempDecryptedFile,
      File tempSegmentDir, String metadataProviderClass)
      throws Exception {

    decryptFile(crypterClassHeader, tempEncryptedFile, tempDecryptedFile);

    // Call metadata provider to extract metadata with file object uri
    return MetadataExtractorFactory.create(metadataProviderClass).extractMetadata(tempDecryptedFile, tempSegmentDir);
  }

  private void completeZkOperations(boolean enableParallelPushProtection, HttpHeaders headers, File tempDecryptedFile,
      FileUploadPathProvider provider, String rawTableName, SegmentMetadata segmentMetadata, String segmentName,
      String zkDownloadURI, boolean moveSegmentToFinalLocation, SegmentValidatorResponse segmentValidatorResponse)
      throws Exception {
    URI finalSegmentLocationURI =
        URIUtils.getUri(provider.getBaseDataDirURI().toString(), rawTableName, URIUtils.encode(segmentName));
    ZKOperator zkOperator = new ZKOperator(_pinotHelixResourceManager, _controllerConf, _controllerMetrics);
    zkOperator.completeSegmentOperations(rawTableName, segmentMetadata, finalSegmentLocationURI, tempDecryptedFile,
        enableParallelPushProtection, headers, zkDownloadURI, moveSegmentToFinalLocation, segmentValidatorResponse);
  }

  private void decryptFile(String crypterClassHeader, File tempEncryptedFile, File tempDecryptedFile) {
    PinotCrypter pinotCrypter = PinotCrypterFactory.create(crypterClassHeader);
    LOGGER.info("Using crypter class {}", pinotCrypter.getClass().getName());
    pinotCrypter.decrypt(tempEncryptedFile, tempDecryptedFile);
  }

  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/segments")
  @ApiOperation(value = "Upload a segment", notes = "Upload a segment as json")
  // We use this endpoint with URI upload because a request sent with the multipart content type will reject the POST
  // request if a multipart object is not sent. This endpoint does not move the segment to its final location;
  // it keeps it at the downloadURI header that is set. We will not support this endpoint going forward.
  public void uploadSegmentAsJson(String segmentJsonStr,
      @ApiParam(value = "Whether to enable parallel push protection") @DefaultValue("false") @QueryParam(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION) boolean enableParallelPushProtection,
      @Context HttpHeaders headers, @Context Request request, @Suspended final AsyncResponse asyncResponse) {
    try {
      asyncResponse.resume(uploadSegment(null, enableParallelPushProtection, headers, request, false));
    } catch (Throwable t) {
      asyncResponse.resume(t);
    }
  }

  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/segments")
  @ApiOperation(value = "Upload a segment", notes = "Upload a segment as binary")
  // For the multipart endpoint, we will always move segment to final location regardless of the segment endpoint.
  public void uploadSegmentAsMultiPart(FormDataMultiPart multiPart,
      @ApiParam(value = "Whether to enable parallel push protection") @DefaultValue("false") @QueryParam(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION) boolean enableParallelPushProtection,
      @Context HttpHeaders headers, @Context Request request, @Suspended final AsyncResponse asyncResponse) {
    try {
      asyncResponse.resume(uploadSegment(multiPart, enableParallelPushProtection, headers, request, true));
    } catch (Throwable t) {
      asyncResponse.resume(t);
    }
  }

  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/v2/segments")
  @ApiOperation(value = "Upload a segment", notes = "Upload a segment as json")
  // We use this endpoint with URI upload because a request sent with the multipart content type will reject the POST
  // request if a multipart object is not sent. This endpoint is recommended for use. It differs from the first
  // endpoint in how it moves the segment to a Pinot-determined final directory.
  public void uploadSegmentAsJsonV2(String segmentJsonStr,
      @ApiParam(value = "Whether to enable parallel push protection") @DefaultValue("false") @QueryParam(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION) boolean enableParallelPushProtection,
      @Context HttpHeaders headers, @Context Request request, @Suspended final AsyncResponse asyncResponse) {
    try {
      asyncResponse.resume(uploadSegment(null, enableParallelPushProtection, headers, request, true));
    } catch (Throwable t) {
      asyncResponse.resume(t);
    }
  }

  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/v2/segments")
  @ApiOperation(value = "Upload a segment", notes = "Upload a segment as binary")
  // This behavior does not differ from v1 of the same endpoint.
  public void uploadSegmentAsMultiPartV2(FormDataMultiPart multiPart,
      @ApiParam(value = "Whether to enable parallel push protection") @DefaultValue("false") @QueryParam(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION) boolean enableParallelPushProtection,
      @Context HttpHeaders headers, @Context Request request, @Suspended final AsyncResponse asyncResponse) {
    try {
      asyncResponse.resume(uploadSegment(multiPart, enableParallelPushProtection, headers, request, true));
    } catch (Throwable t) {
      asyncResponse.resume(t);
    }
  }

  private File getFileFromMultipart(FormDataMultiPart multiPart, File dstFile)
      throws IOException {
    // Read segment file or segment metadata file and directly use that information to update zk
    Map<String, List<FormDataBodyPart>> segmentMetadataMap = multiPart.getFields();
    if (!validateMultiPart(segmentMetadataMap, null)) {
      throw new ControllerApplicationException(LOGGER, "Invalid multi-part form for segment metadata",
          Response.Status.BAD_REQUEST);
    }
    FormDataBodyPart segmentMetadataBodyPart = segmentMetadataMap.values().iterator().next().get(0);
    try (InputStream inputStream = segmentMetadataBodyPart.getValueAs(InputStream.class);
        OutputStream outputStream = new FileOutputStream(dstFile)) {
      IOUtils.copyLarge(inputStream, outputStream);
    } finally {
      multiPart.cleanup();
    }
    return dstFile;
  }

  private FileUploadDownloadClient.FileUploadType getUploadType(String uploadTypeStr) {
    if (uploadTypeStr != null) {
      return FileUploadDownloadClient.FileUploadType.valueOf(uploadTypeStr);
    } else {
      return FileUploadDownloadClient.FileUploadType.getDefaultUploadType();
    }
  }

  private JsonNode formatSegments(String tableName, CommonConstants.Helix.TableType tableType) {
    return JsonUtils.newObjectNode().set(tableType.toString(), getSegments(tableName, tableType.toString()));
  }

  private ArrayNode getSegments(String tableName, String tableType) {
    ArrayNode segments = JsonUtils.newArrayNode();

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
        segments.add(segmentName);
      }
    }

    return segments;
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
      LOGGER.warn("Incorrect number of elements in list in first part: {} (segmentName {}). Picking first one",
          bodyParts.size(), segmentName);
      isGood = false;
    }
    return isGood;
  }
}
