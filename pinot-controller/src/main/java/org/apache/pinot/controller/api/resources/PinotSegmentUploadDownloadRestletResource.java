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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
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
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
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
import javax.ws.rs.core.StreamingOutput;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.restlet.resources.StartReplaceSegmentsRequest;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.upload.SegmentValidator;
import org.apache.pinot.controller.api.upload.ZKOperator;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.metadata.DefaultMetadataExtractor;
import org.apache.pinot.core.metadata.MetadataExtractorFactory;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.crypt.PinotCrypter;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.SEGMENT_TAG)
@Path("/")
public class PinotSegmentUploadDownloadRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentUploadDownloadRestletResource.class);
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
  LeadControllerManager _leadControllerManager;

  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Path("/segments/{tableName}/{segmentName}")
  @ApiOperation(value = "Download a segment", notes = "Download a segment")
  public Response downloadSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @Context HttpHeaders httpHeaders)
      throws Exception {
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

    segmentName = URIUtils.decode(segmentName);
    URI dataDirURI = ControllerFilePathProvider.getInstance().getDataDirURI();
    Response.ResponseBuilder builder = Response.ok();
    File segmentFile;
    // If the segment file is local, just use it as the return entity; otherwise copy it from remote to local first.
    if (CommonConstants.Segment.LOCAL_SEGMENT_SCHEME.equals(dataDirURI.getScheme())) {
      segmentFile = new File(new File(dataDirURI), StringUtil.join(File.separator, tableName, segmentName));
      if (!segmentFile.exists()) {
        throw new ControllerApplicationException(LOGGER,
            "Segment " + segmentName + " or table " + tableName + " not found in " + segmentFile.getAbsolutePath(),
            Response.Status.NOT_FOUND);
      }
      builder.entity(segmentFile);
    } else {
      URI remoteSegmentFileURI = URIUtils.getUri(dataDirURI.toString(), tableName, URIUtils.encode(segmentName));
      PinotFS pinotFS = PinotFSFactory.create(dataDirURI.getScheme());
      if (!pinotFS.exists(remoteSegmentFileURI)) {
        throw new ControllerApplicationException(LOGGER,
            "Segment: " + segmentName + " of table: " + tableName + " not found at: " + remoteSegmentFileURI,
            Response.Status.NOT_FOUND);
      }
      segmentFile = new File(new File(ControllerFilePathProvider.getInstance().getFileDownloadTempDir(), tableName),
          segmentName + "-" + UUID.randomUUID());
      pinotFS.copyToLocalFile(remoteSegmentFileURI, segmentFile);
      // Streaming in the tmp file and delete it afterward.
      builder.entity((StreamingOutput) output -> {
        try {
          Files.copy(segmentFile.toPath(), output);
        } finally {
          FileUtils.deleteQuietly(segmentFile);
        }
      });
    }
    builder.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + segmentFile.getName());
    builder.header(HttpHeaders.CONTENT_LENGTH, segmentFile.length());
    return builder.build();
  }

  private SuccessResponse uploadSegment(@Nullable String tableName, TableType tableType, FormDataMultiPart multiPart,
      boolean enableParallelPushProtection, HttpHeaders headers, Request request, boolean moveSegmentToFinalLocation) {
    String uploadTypeStr = null;
    String crypterClassNameInHeader = null;
    String downloadUri = null;
    String ingestionDescriptor = null;
    if (headers != null) {
      extractHttpHeader(headers, CommonConstants.Controller.SEGMENT_NAME_HTTP_HEADER);
      extractHttpHeader(headers, CommonConstants.Controller.TABLE_NAME_HTTP_HEADER);
      ingestionDescriptor = extractHttpHeader(headers, CommonConstants.Controller.INGESTION_DESCRIPTOR);
      uploadTypeStr = extractHttpHeader(headers, FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE);
      crypterClassNameInHeader = extractHttpHeader(headers, FileUploadDownloadClient.CustomHeaders.CRYPTER);
      downloadUri = extractHttpHeader(headers, FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI);
    }
    File tempEncryptedFile = null;
    File tempDecryptedFile = null;
    File tempSegmentDir = null;
    try {
      ControllerFilePathProvider provider = ControllerFilePathProvider.getInstance();
      String tempFileName = TMP_DIR_PREFIX + UUID.randomUUID();
      tempDecryptedFile = new File(provider.getFileUploadTempDir(), tempFileName);
      tempEncryptedFile = new File(provider.getFileUploadTempDir(), tempFileName + ENCRYPTED_SUFFIX);
      tempSegmentDir = new File(provider.getUntarredFileTempDir(), tempFileName);

      boolean uploadedSegmentIsEncrypted = !Strings.isNullOrEmpty(crypterClassNameInHeader);
      FileUploadDownloadClient.FileUploadType uploadType = getUploadType(uploadTypeStr);
      File dstFile = uploadedSegmentIsEncrypted ? tempEncryptedFile : tempDecryptedFile;
      switch (uploadType) {
        case URI:
          downloadSegmentFileFromURI(downloadUri, dstFile, tableName);
          break;
        case SEGMENT:
          createSegmentFileFromMultipart(multiPart, dstFile);
          break;
        case METADATA:
          moveSegmentToFinalLocation = false;
          Preconditions.checkState(downloadUri != null, "Download URI is required in segment metadata upload mode");
          createSegmentFileFromMultipart(multiPart, dstFile);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported upload type: " + uploadType);
      }

      if (uploadedSegmentIsEncrypted) {
        decryptFile(crypterClassNameInHeader, tempEncryptedFile, tempDecryptedFile);
      }

      String metadataProviderClass = DefaultMetadataExtractor.class.getName();
      SegmentMetadata segmentMetadata = getSegmentMetadata(tempDecryptedFile, tempSegmentDir, metadataProviderClass);

      // Fetch segment name
      String segmentName = segmentMetadata.getName();

      // Fetch table name. Try to derive the table name from the parameter and then from segment metadata
      String rawTableName;
      if (tableName != null && !tableName.isEmpty()) {
        rawTableName = TableNameBuilder.extractRawTableName(tableName);
        LOGGER.info("Uploading a segment {} to table: {}, push type {}, (Derived from API parameter)", segmentName,
            tableName, uploadType);
      } else {
        // TODO: remove this when we completely deprecate the table name from segment metadata
        rawTableName = segmentMetadata.getTableName();
        LOGGER.info("Uploading a segment {} to table: {}, push type {}, (Derived from segment metadata)", segmentName,
            tableName, uploadType);
      }

      String tableNameWithType;
      if (tableType == TableType.OFFLINE) {
        tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
      } else {
        if (!_pinotHelixResourceManager.isUpsertTable(rawTableName)) {
          throw new UnsupportedOperationException(
              "Upload segment to non-upsert realtime table is not supported " + rawTableName);
        }
        tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
      }

      String clientAddress = InetAddress.getByName(request.getRemoteAddr()).getHostName();
      LOGGER.info("Processing upload request for segment: {} of table: {} from client: {}, ingestion descriptor: {}",
          segmentName, tableNameWithType, clientAddress, ingestionDescriptor);

      // Skip segment validation if upload is to an offline table and only segment metadata. Skip segment validation for
      // realtime tables because the feature is experimental and only applicable to upsert enabled table currently.
      if (tableType == TableType.OFFLINE && uploadType != FileUploadDownloadClient.FileUploadType.METADATA) {
        // Validate segment
        new SegmentValidator(_pinotHelixResourceManager, _controllerConf, _executor, _connectionManager,
            _controllerMetrics, _leadControllerManager.isLeaderForTable(tableNameWithType))
            .validateOfflineSegment(tableNameWithType, segmentMetadata, tempSegmentDir);
      }

      // Encrypt segment
      String crypterClassNameInTableConfig =
          _pinotHelixResourceManager.getCrypterClassNameFromTableConfig(tableNameWithType);
      Pair<String, File> encryptionInfo =
          encryptSegmentIfNeeded(tempDecryptedFile, tempEncryptedFile, uploadedSegmentIsEncrypted,
              crypterClassNameInHeader, crypterClassNameInTableConfig, segmentName, tableNameWithType);

      String crypterClassName = encryptionInfo.getLeft();
      File finalSegmentFile = encryptionInfo.getRight();

      // ZK download URI
      String zkDownloadUri;
      // This boolean is here for V1 segment upload, where we keep the segment in the downloadURI sent in the header.
      // We will deprecate this behavior eventually.
      if (!moveSegmentToFinalLocation) {
        LOGGER
            .info("Setting zkDownloadUri: to {} for segment: {} of table: {}, skipping move", downloadUri, segmentName,
                tableNameWithType);
        zkDownloadUri = downloadUri;
      } else {
        zkDownloadUri = getZkDownloadURIForSegmentUpload(rawTableName, segmentName);
      }

      // Zk operations
      completeZkOperations(enableParallelPushProtection, headers, finalSegmentFile, tableNameWithType, segmentMetadata,
          segmentName, zkDownloadUri, moveSegmentToFinalLocation, crypterClassName);

      return new SuccessResponse("Successfully uploaded segment: " + segmentName + " of table: " + tableNameWithType);
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

  @Nullable
  private String extractHttpHeader(HttpHeaders headers, String name) {
    String value = headers.getHeaderString(name);
    if (value != null) {
      LOGGER.info("HTTP Header: {} is: {}", name, value);
    }
    return value;
  }

  Pair<String, File> encryptSegmentIfNeeded(File tempDecryptedFile, File tempEncryptedFile,
      boolean isUploadedSegmentEncrypted, String crypterUsedInUploadedSegment, String crypterClassNameInTableConfig,
      String segmentName, String tableNameWithType) {

    boolean segmentNeedsEncryption = !Strings.isNullOrEmpty(crypterClassNameInTableConfig);

    // form the output
    File finalSegmentFile =
        (isUploadedSegmentEncrypted || segmentNeedsEncryption) ? tempEncryptedFile : tempDecryptedFile;
    String crypterClassName = Strings.isNullOrEmpty(crypterClassNameInTableConfig) ? crypterUsedInUploadedSegment
        : crypterClassNameInTableConfig;
    ImmutablePair<String, File> out = ImmutablePair.of(crypterClassName, finalSegmentFile);

    if (!segmentNeedsEncryption) {
      return out;
    }

    if (isUploadedSegmentEncrypted && !crypterClassNameInTableConfig.equals(crypterUsedInUploadedSegment)) {
      throw new ControllerApplicationException(LOGGER, String.format(
          "Uploaded segment is encrypted with '%s' while table config requires '%s' as crypter "
              + "(segment name = '%s', table name = '%s').", crypterUsedInUploadedSegment,
          crypterClassNameInTableConfig, segmentName, tableNameWithType), Response.Status.INTERNAL_SERVER_ERROR);
    }

    // encrypt segment
    PinotCrypter pinotCrypter = PinotCrypterFactory.create(crypterClassNameInTableConfig);
    LOGGER.info("Using crypter class '{}' for encrypting '{}' to '{}' (segment name = '{}', table name = '{}').",
        crypterClassNameInTableConfig, tempDecryptedFile, tempEncryptedFile, segmentName, tableNameWithType);
    pinotCrypter.encrypt(tempDecryptedFile, tempEncryptedFile);

    return out;
  }

  private String getZkDownloadURIForSegmentUpload(String rawTableName, String segmentName) {
    ControllerFilePathProvider provider = ControllerFilePathProvider.getInstance();
    URI dataDirURI = provider.getDataDirURI();
    if (dataDirURI.getScheme().equalsIgnoreCase(CommonConstants.Segment.LOCAL_SEGMENT_SCHEME)) {
      return URIUtils.constructDownloadUrl(provider.getVip(), rawTableName, segmentName);
    } else {
      // Receiving .tar.gz segment upload for pluggable storage. Download URI is the same as final segment location.
      String downloadUri = URIUtils.getPath(dataDirURI.toString(), rawTableName, URIUtils.encode(segmentName));
      LOGGER.info("Using download uri: {} for segment: {} of table {}", downloadUri, segmentName, rawTableName);
      return downloadUri;
    }
  }

  private void downloadSegmentFileFromURI(String currentSegmentLocationURI, File destFile, String tableName)
      throws Exception {
    if (currentSegmentLocationURI == null || currentSegmentLocationURI.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Failed to get downloadURI, needed for URI upload",
          Response.Status.BAD_REQUEST);
    }
    LOGGER.info("Downloading segment from {} to {} for table {}", currentSegmentLocationURI, destFile.getAbsolutePath(),
        tableName);
    SegmentFetcherFactory.fetchSegmentToLocal(currentSegmentLocationURI, destFile);
  }

  private SegmentMetadata getSegmentMetadata(File tempDecryptedFile, File tempSegmentDir, String metadataProviderClass)
      throws Exception {
    // Call metadata provider to extract metadata with file object uri
    return MetadataExtractorFactory.create(metadataProviderClass).extractMetadata(tempDecryptedFile, tempSegmentDir);
  }

  private void completeZkOperations(boolean enableParallelPushProtection, HttpHeaders headers, File uploadedSegmentFile,
      String tableNameWithType, SegmentMetadata segmentMetadata, String segmentName, String zkDownloadURI,
      boolean moveSegmentToFinalLocation, String crypter)
      throws Exception {
    String basePath = ControllerFilePathProvider.getInstance().getDataDirURI().toString();
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    URI finalSegmentLocationURI = URIUtils.getUri(basePath, rawTableName, URIUtils.encode(segmentName));
    ZKOperator zkOperator = new ZKOperator(_pinotHelixResourceManager, _controllerConf, _controllerMetrics);
    zkOperator
        .completeSegmentOperations(tableNameWithType, segmentMetadata, finalSegmentLocationURI, uploadedSegmentFile,
            enableParallelPushProtection, headers, zkDownloadURI, moveSegmentToFinalLocation, crypter);
  }

  private void decryptFile(String crypterClassName, File tempEncryptedFile, File tempDecryptedFile) {
    PinotCrypter pinotCrypter = PinotCrypterFactory.create(crypterClassName);
    LOGGER.info("Using crypter class {} for decrypting {} to {}", pinotCrypter.getClass().getName(), tempEncryptedFile,
        tempDecryptedFile);
    pinotCrypter.decrypt(tempEncryptedFile, tempDecryptedFile);
  }

  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/segments")
  @Authenticate(AccessType.CREATE)
  @ApiOperation(value = "Upload a segment", notes = "Upload a segment as json")
  // We use this endpoint with URI upload because a request sent with the multipart content type will reject the POST
  // request if a multipart object is not sent. This endpoint does not move the segment to its final location;
  // it keeps it at the downloadURI header that is set. We will not support this endpoint going forward.
  public void uploadSegmentAsJson(String segmentJsonStr,
      @ApiParam(value = "Name of the table") @QueryParam(FileUploadDownloadClient.QueryParameters.TABLE_NAME)
          String tableName,
      @ApiParam(value = "Type of the table") @QueryParam(FileUploadDownloadClient.QueryParameters.TABLE_TYPE)
      @DefaultValue("OFFLINE") String tableType,
      @ApiParam(value = "Whether to enable parallel push protection") @DefaultValue("false")
      @QueryParam(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION)
          boolean enableParallelPushProtection, @Context HttpHeaders headers, @Context Request request,
      @Suspended final AsyncResponse asyncResponse) {
    try {
      asyncResponse.resume(
          uploadSegment(tableName, TableType.valueOf(tableType.toUpperCase()), null, enableParallelPushProtection,
              headers, request, false));
    } catch (Throwable t) {
      asyncResponse.resume(t);
    }
  }

  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/segments")
  @Authenticate(AccessType.CREATE)
  @ApiOperation(value = "Upload a segment", notes = "Upload a segment as binary")
  // For the multipart endpoint, we will always move segment to final location regardless of the segment endpoint.
  public void uploadSegmentAsMultiPart(FormDataMultiPart multiPart,
      @ApiParam(value = "Name of the table") @QueryParam(FileUploadDownloadClient.QueryParameters.TABLE_NAME)
          String tableName,
      @ApiParam(value = "Type of the table") @QueryParam(FileUploadDownloadClient.QueryParameters.TABLE_TYPE)
      @DefaultValue("OFFLINE") String tableType,
      @ApiParam(value = "Whether to enable parallel push protection") @DefaultValue("false")
      @QueryParam(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION)
          boolean enableParallelPushProtection, @Context HttpHeaders headers, @Context Request request,
      @Suspended final AsyncResponse asyncResponse) {
    try {
      asyncResponse.resume(
          uploadSegment(tableName, TableType.valueOf(tableType.toUpperCase()), multiPart, enableParallelPushProtection,
              headers, request, true));
    } catch (Throwable t) {
      asyncResponse.resume(t);
    }
  }

  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/v2/segments")
  @Authenticate(AccessType.CREATE)
  @ApiOperation(value = "Upload a segment", notes = "Upload a segment as json")
  // We use this endpoint with URI upload because a request sent with the multipart content type will reject the POST
  // request if a multipart object is not sent. This endpoint is recommended for use. It differs from the first
  // endpoint in how it moves the segment to a Pinot-determined final directory.
  public void uploadSegmentAsJsonV2(String segmentJsonStr,
      @ApiParam(value = "Name of the table") @QueryParam(FileUploadDownloadClient.QueryParameters.TABLE_NAME)
          String tableName,
      @ApiParam(value = "Type of the table") @QueryParam(FileUploadDownloadClient.QueryParameters.TABLE_TYPE)
      @DefaultValue("OFFLINE") String tableType,
      @ApiParam(value = "Whether to enable parallel push protection") @DefaultValue("false")
      @QueryParam(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION)
          boolean enableParallelPushProtection, @Context HttpHeaders headers, @Context Request request,
      @Suspended final AsyncResponse asyncResponse) {
    try {
      asyncResponse.resume(
          uploadSegment(tableName, TableType.valueOf(tableType.toUpperCase()), null, enableParallelPushProtection,
              headers, request, true));
    } catch (Throwable t) {
      asyncResponse.resume(t);
    }
  }

  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/v2/segments")
  @Authenticate(AccessType.CREATE)
  @ApiOperation(value = "Upload a segment", notes = "Upload a segment as binary")
  // This behavior does not differ from v1 of the same endpoint.
  public void uploadSegmentAsMultiPartV2(FormDataMultiPart multiPart,
      @ApiParam(value = "Name of the table") @QueryParam(FileUploadDownloadClient.QueryParameters.TABLE_NAME)
          String tableName,
      @ApiParam(value = "Type of the table") @QueryParam(FileUploadDownloadClient.QueryParameters.TABLE_TYPE)
      @DefaultValue("OFFLINE") String tableType,
      @ApiParam(value = "Whether to enable parallel push protection") @DefaultValue("false")
      @QueryParam(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION)
          boolean enableParallelPushProtection, @Context HttpHeaders headers, @Context Request request,
      @Suspended final AsyncResponse asyncResponse) {
    try {
      asyncResponse.resume(
          uploadSegment(tableName, TableType.valueOf(tableType.toUpperCase()), multiPart, enableParallelPushProtection,
              headers, request, true));
    } catch (Throwable t) {
      asyncResponse.resume(t);
    }
  }

  @POST
  @Path("segments/{tableName}/startReplaceSegments")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Start to replace segments", notes = "Start to replace segments")
  public Response startReplaceSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      StartReplaceSegmentsRequest startReplaceSegmentsRequest) {
    try {
      String tableNameWithType =
          TableNameBuilder.forType(TableType.valueOf(tableTypeStr.toUpperCase())).tableNameWithType(tableName);
      String segmentLineageEntryId = _pinotHelixResourceManager
          .startReplaceSegments(tableNameWithType, startReplaceSegmentsRequest.getSegmentsFrom(),
              startReplaceSegmentsRequest.getSegmentsTo());
      return Response.ok(JsonUtils.newObjectNode().put("segmentLineageEntryId", segmentLineageEntryId)).build();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @POST
  @Path("segments/{tableName}/endReplaceSegments")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "End to replace segments", notes = "End to replace segments")
  public Response endReplaceSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME") @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Segment lineage entry id returned by startReplaceSegments API")
      @QueryParam("segmentLineageEntryId") String segmentLineageEntryId) {
    try {
      String tableNameWithType =
          TableNameBuilder.forType(TableType.valueOf(tableTypeStr.toUpperCase())).tableNameWithType(tableName);
      // Check that the segment lineage entry id is valid
      Preconditions.checkNotNull(segmentLineageEntryId, "'segmentLineageEntryId' should not be null");
      _pinotHelixResourceManager.endReplaceSegments(tableNameWithType, segmentLineageEntryId);
      return Response.ok().build();
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  private File createSegmentFileFromMultipart(FormDataMultiPart multiPart, File dstFile)
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

  // Validate that there is one file that is in the input.
  public static boolean validateMultiPart(Map<String, List<FormDataBodyPart>> map, String segmentName) {
    boolean isGood = true;
    if (map.size() != 1) {
      LOGGER.warn("Incorrect number of multi-part elements: {} (segmentName {}). Picking one", map.size(), segmentName);
      isGood = false;
    }
    List<FormDataBodyPart> bodyParts = map.values().iterator().next();
    if (bodyParts.size() != 1) {
      LOGGER.warn("Incorrect number of elements in list in first part: {} (segmentName {}). Picking first one",
          bodyParts.size(), segmentName);
      isGood = false;
    }
    return isGood;
  }
}
