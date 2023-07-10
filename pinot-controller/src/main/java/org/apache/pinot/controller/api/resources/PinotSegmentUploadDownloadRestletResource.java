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

import com.google.common.annotations.VisibleForTesting;
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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.restlet.resources.EndReplaceSegmentsRequest;
import org.apache.pinot.common.restlet.resources.RevertReplaceSegmentsRequest;
import org.apache.pinot.common.restlet.resources.StartReplaceSegmentsRequest;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.upload.SegmentValidationUtils;
import org.apache.pinot.controller.api.upload.ZKOperator;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.RBACAuthorization;
import org.apache.pinot.core.metadata.DefaultMetadataExtractor;
import org.apache.pinot.core.metadata.MetadataExtractorFactory;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.crypt.PinotCrypter;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = Constants.SEGMENT_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
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
  @RBACAuthorization(targetType = "table", targetId = "tableName", permission = "DownloadSegment")
  @ApiOperation(value = "Download a segment", notes = "Download a segment")
  @TrackInflightRequestMetrics
  @TrackedByGauge(gauge = ControllerGauge.SEGMENT_DOWNLOADS_IN_PROGRESS)
  @Authenticate(AccessType.READ)
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

  private SuccessResponse uploadSegment(@Nullable String tableName, TableType tableType,
      @Nullable FormDataMultiPart multiPart, boolean copySegmentToFinalLocation, boolean enableParallelPushProtection,
      boolean allowRefresh, HttpHeaders headers, Request request) {
    if (StringUtils.isNotEmpty(tableName)) {
      TableType tableTypeFromTableName = TableNameBuilder.getTableTypeFromTableName(tableName);
      if (tableTypeFromTableName != null && tableTypeFromTableName != tableType) {
        throw new ControllerApplicationException(LOGGER,
            String.format("Table name: %s does not match table type: %s", tableName, tableType),
            Response.Status.BAD_REQUEST);
      }
    }

    // TODO: Consider validating the segment name and table name from the header against the actual segment
    extractHttpHeader(headers, CommonConstants.Controller.SEGMENT_NAME_HTTP_HEADER);
    extractHttpHeader(headers, CommonConstants.Controller.TABLE_NAME_HTTP_HEADER);

    String uploadTypeStr = extractHttpHeader(headers, FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE);
    String sourceDownloadURIStr = extractHttpHeader(headers, FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI);
    String crypterClassNameInHeader = extractHttpHeader(headers, FileUploadDownloadClient.CustomHeaders.CRYPTER);
    String ingestionDescriptor = extractHttpHeader(headers, CommonConstants.Controller.INGESTION_DESCRIPTOR);

    File tempEncryptedFile = null;
    File tempDecryptedFile = null;
    File tempSegmentDir = null;
    // The downloadUri for putting into segment zk metadata
    String segmentDownloadURIStr = sourceDownloadURIStr;
    try {
      ControllerFilePathProvider provider = ControllerFilePathProvider.getInstance();
      String tempFileName = TMP_DIR_PREFIX + UUID.randomUUID();
      tempEncryptedFile = new File(provider.getFileUploadTempDir(), tempFileName + ENCRYPTED_SUFFIX);
      tempDecryptedFile = new File(provider.getFileUploadTempDir(), tempFileName);
      tempSegmentDir = new File(provider.getUntarredFileTempDir(), tempFileName);

      boolean uploadedSegmentIsEncrypted = StringUtils.isNotEmpty(crypterClassNameInHeader);
      FileUploadDownloadClient.FileUploadType uploadType = getUploadType(uploadTypeStr);
      File destFile = uploadedSegmentIsEncrypted ? tempEncryptedFile : tempDecryptedFile;
      long segmentSizeInBytes;
      switch (uploadType) {
        case SEGMENT:
          if (multiPart == null) {
            throw new ControllerApplicationException(LOGGER,
                "Segment file (as multipart/form-data) is required for SEGMENT upload mode",
                Response.Status.BAD_REQUEST);
          }
          if (!copySegmentToFinalLocation && StringUtils.isEmpty(sourceDownloadURIStr)) {
            throw new ControllerApplicationException(LOGGER,
                "Source download URI is required in header field 'DOWNLOAD_URI' if segment should not be copied to "
                    + "the deep store",
                Response.Status.BAD_REQUEST);
          }
          createSegmentFileFromMultipart(multiPart, destFile);
          segmentSizeInBytes = destFile.length();
          break;
        case URI:
          if (StringUtils.isEmpty(sourceDownloadURIStr)) {
            throw new ControllerApplicationException(LOGGER,
                "Source download URI is required in header field 'DOWNLOAD_URI' for URI upload mode",
                Response.Status.BAD_REQUEST);
          }
          downloadSegmentFileFromURI(sourceDownloadURIStr, destFile, tableName);
          segmentSizeInBytes = destFile.length();
          break;
        case METADATA:
          if (multiPart == null) {
            throw new ControllerApplicationException(LOGGER,
                "Segment metadata file (as multipart/form-data) is required for METADATA upload mode",
                Response.Status.BAD_REQUEST);
          }
          if (StringUtils.isEmpty(sourceDownloadURIStr)) {
            throw new ControllerApplicationException(LOGGER,
                "Source download URI is required in header field 'DOWNLOAD_URI' for METADATA upload mode",
                Response.Status.BAD_REQUEST);
          }
          // override copySegmentToFinalLocation if override provided in headers:COPY_SEGMENT_TO_DEEP_STORE
          // else set to false for backward compatibility
          String copySegmentToDeepStore =
              extractHttpHeader(headers, FileUploadDownloadClient.CustomHeaders.COPY_SEGMENT_TO_DEEP_STORE);
          copySegmentToFinalLocation = Boolean.parseBoolean(copySegmentToDeepStore);
          createSegmentFileFromMultipart(multiPart, destFile);
          try {
            URI segmentURI = new URI(sourceDownloadURIStr);
            PinotFS pinotFS = PinotFSFactory.create(segmentURI.getScheme());
            segmentSizeInBytes = pinotFS.length(segmentURI);
          } catch (Exception e) {
            segmentSizeInBytes = -1;
            LOGGER.warn("Could not fetch segment size for metadata push", e);
          }
          break;
        default:
          throw new ControllerApplicationException(LOGGER, "Unsupported upload type: " + uploadType,
              Response.Status.BAD_REQUEST);
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
      if (StringUtils.isNotEmpty(tableName)) {
        rawTableName = TableNameBuilder.extractRawTableName(tableName);
      } else {
        // TODO: remove this when we completely deprecate the table name from segment metadata
        rawTableName = segmentMetadata.getTableName();
        LOGGER.warn("Table name is not provided as request query parameter when uploading segment: {} for table: {}",
            segmentName, rawTableName);
      }
      String tableNameWithType = tableType == TableType.OFFLINE
          ? TableNameBuilder.OFFLINE.tableNameWithType(rawTableName)
          : TableNameBuilder.REALTIME.tableNameWithType(rawTableName);

      String clientAddress = InetAddress.getByName(request.getRemoteAddr()).getHostName();
      LOGGER.info("Processing upload request for segment: {} of table: {} with upload type: {} from client: {}, "
          + "ingestion descriptor: {}", segmentName, tableNameWithType, uploadType, clientAddress, ingestionDescriptor);

      // Validate segment
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
      if (tableConfig == null) {
        throw new ControllerApplicationException(LOGGER, "Failed to find table: " + tableNameWithType,
            Response.Status.BAD_REQUEST);
      }
      if (tableConfig.getIngestionConfig() == null || tableConfig.getIngestionConfig().isSegmentTimeValueCheck()) {
        SegmentValidationUtils.validateTimeInterval(segmentMetadata, tableConfig);
      }
      if (uploadType != FileUploadDownloadClient.FileUploadType.METADATA) {
        SegmentValidationUtils.checkStorageQuota(tempSegmentDir, segmentMetadata, tableConfig,
            _pinotHelixResourceManager, _controllerConf, _controllerMetrics, _connectionManager, _executor,
            _leadControllerManager.isLeaderForTable(tableNameWithType));
      }

      // Encrypt segment
      String crypterNameInTableConfig = tableConfig.getValidationConfig().getCrypterClassName();
      Pair<String, File> encryptionInfo =
          encryptSegmentIfNeeded(tempDecryptedFile, tempEncryptedFile, uploadedSegmentIsEncrypted,
              crypterClassNameInHeader, crypterNameInTableConfig, segmentName, tableNameWithType);

      String crypterName = encryptionInfo.getLeft();
      File segmentFile = encryptionInfo.getRight();

      // Update download URI if controller is responsible for moving the segment to the deep store
      URI finalSegmentLocationURI = null;
      if (copySegmentToFinalLocation) {
        URI dataDirURI = provider.getDataDirURI();
        String dataDirPath = dataDirURI.toString();
        String encodedSegmentName = URIUtils.encode(segmentName);
        String finalSegmentLocationPath = URIUtils.getPath(dataDirPath, rawTableName, encodedSegmentName);
        if (dataDirURI.getScheme().equalsIgnoreCase(CommonConstants.Segment.LOCAL_SEGMENT_SCHEME)) {
          segmentDownloadURIStr = URIUtils.getPath(provider.getVip(), "segments", rawTableName, encodedSegmentName);
        } else {
          segmentDownloadURIStr = finalSegmentLocationPath;
        }
        finalSegmentLocationURI = URIUtils.getUri(finalSegmentLocationPath);
      }
      LOGGER.info("Using segment download URI: {} for segment: {} of table: {} (move segment: {})",
          segmentDownloadURIStr, segmentFile, tableNameWithType, copySegmentToFinalLocation);

      ZKOperator zkOperator = new ZKOperator(_pinotHelixResourceManager, _controllerConf, _controllerMetrics);
      zkOperator.completeSegmentOperations(tableNameWithType, segmentMetadata, uploadType, finalSegmentLocationURI,
          segmentFile, sourceDownloadURIStr, segmentDownloadURIStr, crypterName, segmentSizeInBytes,
          enableParallelPushProtection, allowRefresh, headers);

      return new SuccessResponse("Successfully uploaded segment: " + segmentName + " of table: " + tableNameWithType);
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, "Exception while uploading segment: " + e.getMessage(),
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

  @VisibleForTesting
  Pair<String, File> encryptSegmentIfNeeded(File tempDecryptedFile, File tempEncryptedFile,
      boolean isUploadedSegmentEncrypted, String crypterUsedInUploadedSegment, String crypterClassNameInTableConfig,
      String segmentName, String tableNameWithType) {

    boolean segmentNeedsEncryption = StringUtils.isNotEmpty(crypterClassNameInTableConfig);

    // form the output
    File finalSegmentFile =
        (isUploadedSegmentEncrypted || segmentNeedsEncryption) ? tempEncryptedFile : tempDecryptedFile;
    String crypterClassName = StringUtils.isEmpty(crypterClassNameInTableConfig) ? crypterUsedInUploadedSegment
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

  private void downloadSegmentFileFromURI(String currentSegmentLocationURI, File destFile, String tableName)
      throws Exception {
    if (currentSegmentLocationURI == null || currentSegmentLocationURI.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Failed to get downloadURI, needed for URI upload",
          Response.Status.BAD_REQUEST);
    }
    LOGGER.info("Downloading segment from {} to {} for table {}", currentSegmentLocationURI, destFile.getAbsolutePath(),
        tableName);
    URI uri = new URI(currentSegmentLocationURI);
    if (uri.getScheme().equalsIgnoreCase("file")) {
      throw new ControllerApplicationException(LOGGER, "Unsupported URI: " + currentSegmentLocationURI,
          Response.Status.BAD_REQUEST);
    }
    SegmentFetcherFactory.fetchSegmentToLocal(currentSegmentLocationURI, destFile);
  }

  private SegmentMetadata getSegmentMetadata(File tempDecryptedFile, File tempSegmentDir, String metadataProviderClass)
      throws Exception {
    // Call metadata provider to extract metadata with file object uri
    return MetadataExtractorFactory.create(metadataProviderClass).extractMetadata(tempDecryptedFile, tempSegmentDir);
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
  @RBACAuthorization(targetType = "cluster", permission = "UploadSegment")
  @Authenticate(AccessType.CREATE)
  @ApiOperation(value = "Upload a segment", notes = "Upload a segment as json")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully uploaded segment"),
      @ApiResponse(code = 400, message = "Bad Request"),
      @ApiResponse(code = 403, message = "Segment validation fails"),
      @ApiResponse(code = 409, message = "Segment already exists or another parallel push in progress"),
      @ApiResponse(code = 410, message = "Segment to refresh does not exist"),
      @ApiResponse(code = 412, message = "CRC check fails"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  @TrackInflightRequestMetrics
  @TrackedByGauge(gauge = ControllerGauge.SEGMENT_UPLOADS_IN_PROGRESS)
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
          boolean enableParallelPushProtection,
      @ApiParam(value = "Whether to refresh if the segment already exists") @DefaultValue("true")
      @QueryParam(FileUploadDownloadClient.QueryParameters.ALLOW_REFRESH) boolean allowRefresh,
      @Context HttpHeaders headers, @Context Request request, @Suspended final AsyncResponse asyncResponse) {
    try {
      asyncResponse.resume(uploadSegment(tableName, TableType.valueOf(tableType.toUpperCase()), null, false,
          enableParallelPushProtection, allowRefresh, headers, request));
    } catch (Throwable t) {
      asyncResponse.resume(t);
    }
  }

  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/segments")
  @RBACAuthorization(targetType = "cluster", permission = "UploadSegment")
  @Authenticate(AccessType.CREATE)
  @ApiOperation(value = "Upload a segment", notes = "Upload a segment as binary")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully uploaded segment"),
      @ApiResponse(code = 400, message = "Bad Request"),
      @ApiResponse(code = 403, message = "Segment validation fails"),
      @ApiResponse(code = 409, message = "Segment already exists or another parallel push in progress"),
      @ApiResponse(code = 410, message = "Segment to refresh does not exist"),
      @ApiResponse(code = 412, message = "CRC check fails"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  @TrackInflightRequestMetrics
  @TrackedByGauge(gauge = ControllerGauge.SEGMENT_UPLOADS_IN_PROGRESS)
  // For the multipart endpoint, we will always move segment to final location regardless of the segment endpoint.
  public void uploadSegmentAsMultiPart(FormDataMultiPart multiPart,
      @ApiParam(value = "Name of the table") @QueryParam(FileUploadDownloadClient.QueryParameters.TABLE_NAME)
          String tableName,
      @ApiParam(value = "Type of the table") @QueryParam(FileUploadDownloadClient.QueryParameters.TABLE_TYPE)
      @DefaultValue("OFFLINE") String tableType,
      @ApiParam(value = "Whether to enable parallel push protection") @DefaultValue("false")
      @QueryParam(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION)
          boolean enableParallelPushProtection,
      @ApiParam(value = "Whether to refresh if the segment already exists") @DefaultValue("true")
      @QueryParam(FileUploadDownloadClient.QueryParameters.ALLOW_REFRESH) boolean allowRefresh,
      @Context HttpHeaders headers, @Context Request request, @Suspended final AsyncResponse asyncResponse) {
    try {
      asyncResponse.resume(uploadSegment(tableName, TableType.valueOf(tableType.toUpperCase()), multiPart, true,
          enableParallelPushProtection, allowRefresh, headers, request));
    } catch (Throwable t) {
      asyncResponse.resume(t);
    }
  }

  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/v2/segments")
  @RBACAuthorization(targetType = "table", targetId = "tableName", permission = "UploadSegment")
  @Authenticate(AccessType.CREATE)
  @ApiOperation(value = "Upload a segment", notes = "Upload a segment as json")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully uploaded segment"),
      @ApiResponse(code = 400, message = "Bad Request"),
      @ApiResponse(code = 403, message = "Segment validation fails"),
      @ApiResponse(code = 409, message = "Segment already exists or another parallel push in progress"),
      @ApiResponse(code = 410, message = "Segment to refresh does not exist"),
      @ApiResponse(code = 412, message = "CRC check fails"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  @TrackInflightRequestMetrics
  @TrackedByGauge(gauge = ControllerGauge.SEGMENT_UPLOADS_IN_PROGRESS)
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
          boolean enableParallelPushProtection,
      @ApiParam(value = "Whether to refresh if the segment already exists") @DefaultValue("true")
      @QueryParam(FileUploadDownloadClient.QueryParameters.ALLOW_REFRESH) boolean allowRefresh,
      @Context HttpHeaders headers, @Context Request request, @Suspended final AsyncResponse asyncResponse) {
    try {
      asyncResponse.resume(
          uploadSegment(tableName, TableType.valueOf(tableType.toUpperCase()), null, true, enableParallelPushProtection,
              allowRefresh, headers, request));
    } catch (Throwable t) {
      asyncResponse.resume(t);
    }
  }

  @POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/v2/segments")
  @RBACAuthorization(targetType = "table", targetId = "tableName", permission = "UploadSegment")
  @Authenticate(AccessType.CREATE)
  @ApiOperation(value = "Upload a segment", notes = "Upload a segment as binary")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Successfully uploaded segment"),
      @ApiResponse(code = 400, message = "Bad Request"),
      @ApiResponse(code = 403, message = "Segment validation fails"),
      @ApiResponse(code = 409, message = "Segment already exists or another parallel push in progress"),
      @ApiResponse(code = 410, message = "Segment to refresh does not exist"),
      @ApiResponse(code = 412, message = "CRC check fails"),
      @ApiResponse(code = 500, message = "Internal error")
  })
  @TrackInflightRequestMetrics
  @TrackedByGauge(gauge = ControllerGauge.SEGMENT_UPLOADS_IN_PROGRESS)
  // This behavior does not differ from v1 of the same endpoint.
  public void uploadSegmentAsMultiPartV2(FormDataMultiPart multiPart,
      @ApiParam(value = "Name of the table") @QueryParam(FileUploadDownloadClient.QueryParameters.TABLE_NAME)
          String tableName,
      @ApiParam(value = "Type of the table") @QueryParam(FileUploadDownloadClient.QueryParameters.TABLE_TYPE)
      @DefaultValue("OFFLINE") String tableType,
      @ApiParam(value = "Whether to enable parallel push protection") @DefaultValue("false")
      @QueryParam(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION)
          boolean enableParallelPushProtection,
      @ApiParam(value = "Whether to refresh if the segment already exists") @DefaultValue("true")
      @QueryParam(FileUploadDownloadClient.QueryParameters.ALLOW_REFRESH) boolean allowRefresh,
      @Context HttpHeaders headers, @Context Request request, @Suspended final AsyncResponse asyncResponse) {
    try {
      asyncResponse.resume(uploadSegment(tableName, TableType.valueOf(tableType.toUpperCase()), multiPart, true,
          enableParallelPushProtection, allowRefresh, headers, request));
    } catch (Throwable t) {
      asyncResponse.resume(t);
    }
  }

  @POST
  @Path("segments/{tableName}/startReplaceSegments")
  @RBACAuthorization(targetType = "table", targetId = "tableName", permission = "ReplaceSegments")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Start to replace segments", notes = "Start to replace segments")
  public Response startReplaceSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME", required = true) @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Force cleanup") @QueryParam("forceCleanup") @DefaultValue("false") boolean forceCleanup,
      @ApiParam(value = "Fields belonging to start replace segment request", required = true)
          StartReplaceSegmentsRequest startReplaceSegmentsRequest) {
    TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == null) {
      throw new ControllerApplicationException(LOGGER, "Table type should either be offline or realtime",
          Response.Status.BAD_REQUEST);
    }
    String tableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    try {
      String segmentLineageEntryId = _pinotHelixResourceManager.startReplaceSegments(tableNameWithType,
          startReplaceSegmentsRequest.getSegmentsFrom(), startReplaceSegmentsRequest.getSegmentsTo(), forceCleanup,
          startReplaceSegmentsRequest.getCustomMap());
      return Response.ok(JsonUtils.newObjectNode().put("segmentLineageEntryId", segmentLineageEntryId)).build();
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.NUMBER_START_REPLACE_FAILURE, 1);
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @POST
  @Path("segments/{tableName}/endReplaceSegments")
  @RBACAuthorization(targetType = "table", targetId = "tableName", permission = "ReplaceSegments")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "End to replace segments", notes = "End to replace segments")
  public Response endReplaceSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME", required = true) @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Segment lineage entry id returned by startReplaceSegments API", required = true)
      @QueryParam("segmentLineageEntryId") String segmentLineageEntryId,
      @ApiParam(value = "Fields belonging to end replace segment request", required = false)
          EndReplaceSegmentsRequest endReplaceSegmentsRequest) {
    TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == null) {
      throw new ControllerApplicationException(LOGGER, "Table type should either be offline or realtime",
          Response.Status.BAD_REQUEST);
    }
    String tableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    try {
      // Check that the segment lineage entry id is valid
      Preconditions.checkNotNull(segmentLineageEntryId, "'segmentLineageEntryId' should not be null");
      _pinotHelixResourceManager.endReplaceSegments(tableNameWithType, segmentLineageEntryId,
          endReplaceSegmentsRequest);
      return Response.ok().build();
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.NUMBER_END_REPLACE_FAILURE, 1);
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @POST
  @Path("segments/{tableName}/revertReplaceSegments")
  @RBACAuthorization(targetType = "table", targetId = "tableName", permission = "ReplaceSegments")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Revert segments replacement", notes = "Revert segments replacement")
  public Response revertReplaceSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "OFFLINE|REALTIME", required = true) @QueryParam("type") String tableTypeStr,
      @ApiParam(value = "Segment lineage entry id to revert", required = true) @QueryParam("segmentLineageEntryId")
          String segmentLineageEntryId,
      @ApiParam(value = "Force revert in case the user knows that the lineage entry is interrupted")
      @QueryParam("forceRevert") @DefaultValue("false") boolean forceRevert,
      @ApiParam(value = "Fields belonging to revert replace segment request", required = false)
          RevertReplaceSegmentsRequest revertReplaceSegmentsRequest) {
    TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == null) {
      throw new ControllerApplicationException(LOGGER, "Table type should either be offline or realtime",
          Response.Status.BAD_REQUEST);
    }
    String tableNameWithType =
        ResourceUtils.getExistingTableNamesWithType(_pinotHelixResourceManager, tableName, tableType, LOGGER).get(0);
    try {
      // Check that the segment lineage entry id is valid
      Preconditions.checkNotNull(segmentLineageEntryId, "'segmentLineageEntryId' should not be null");
      _pinotHelixResourceManager.revertReplaceSegments(tableNameWithType, segmentLineageEntryId, forceRevert,
          revertReplaceSegmentsRequest);
      return Response.ok().build();
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.NUMBER_REVERT_REPLACE_FAILURE, 1);
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  private static void createSegmentFileFromMultipart(FormDataMultiPart multiPart, File destFile)
      throws IOException {
    // Read segment file or segment metadata file and directly use that information to update zk
    Map<String, List<FormDataBodyPart>> segmentMetadataMap = multiPart.getFields();
    if (!validateMultiPart(segmentMetadataMap, null)) {
      throw new ControllerApplicationException(LOGGER, "Invalid multi-part form for segment metadata",
          Response.Status.BAD_REQUEST);
    }
    FormDataBodyPart segmentMetadataBodyPart = segmentMetadataMap.values().iterator().next().get(0);
    try (InputStream inputStream = segmentMetadataBodyPart.getValueAs(InputStream.class);
        OutputStream outputStream = new FileOutputStream(destFile)) {
      IOUtils.copyLarge(inputStream, outputStream);
    } finally {
      multiPart.cleanup();
    }
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
