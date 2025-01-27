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

import com.google.common.base.Function;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.SegmentCompletionUtils;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.server.api.resources.reingestion.ReIngestionRequest;
import org.apache.pinot.server.api.resources.reingestion.ReIngestionResponse;
import org.apache.pinot.server.api.resources.reingestion.utils.SimpleRealtimeSegmentDataManager;
import org.apache.pinot.server.realtime.ControllerLeaderLocator;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.HTTPS_PROTOCOL;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = "ReIngestion", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```"),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")}))
@Path("/")
public class ReIngestionResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReIngestionResource.class);
  public static final FileUploadDownloadClient FILE_UPLOAD_DOWNLOAD_CLIENT = new FileUploadDownloadClient();
  //TODO: Maximum number of concurrent re-ingestions allowed should be configurable
  private static final int MAX_PARALLEL_REINGESTIONS = 10;

  // Map to track ongoing ingestion per segment
  private static final ConcurrentHashMap<String, AtomicBoolean> SEGMENT_INGESTION_MAP = new ConcurrentHashMap<>();

  // Semaphore to enforce global concurrency limit
  private static final Semaphore REINGESTION_SEMAPHORE = new Semaphore(MAX_PARALLEL_REINGESTIONS);

  @Inject
  private ServerInstance _serverInstance;

  @POST
  @Path("/reingestSegment")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Re-ingest segment", notes = "Re-ingest data for a segment from startOffset to endOffset and "
      + "upload the segment")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success", response = ReIngestionResponse.class), @ApiResponse(code = 500,
      message = "Internal server error", response = ErrorInfo.class)
  })
  public Response reIngestSegment(ReIngestionRequest request) {
    try {
      String tableNameWithType = request.getTableNameWithType();
      String segmentName = request.getSegmentName();

      // Try to acquire a permit from the semaphore to ensure we don't exceed max concurrency
      if (!REINGESTION_SEMAPHORE.tryAcquire()) {
        return Response.status(Response.Status.SERVICE_UNAVAILABLE)
            .entity("Too many re-ingestions in progress. Please try again later.")
            .build();
      }

      // Check if the segment is already being re-ingested
      AtomicBoolean isIngesting = SEGMENT_INGESTION_MAP.computeIfAbsent(segmentName, k -> new AtomicBoolean(false));
      if (!isIngesting.compareAndSet(false, true)) {
        // The segment is already being ingested
        REINGESTION_SEMAPHORE.release();
        return Response.status(Response.Status.CONFLICT)
            .entity("Re-ingestion for segment: " + segmentName + " is already in progress.")
            .build();
      }

      InstanceDataManager instanceDataManager = _serverInstance.getInstanceDataManager();
      if (instanceDataManager == null) {
        throw new WebApplicationException(new RuntimeException("Invalid server initialization"),
            Response.Status.INTERNAL_SERVER_ERROR);
      }

      TableDataManager tableDataManager = instanceDataManager.getTableDataManager(tableNameWithType);
      if (tableDataManager == null) {
        throw new WebApplicationException("Table data manager not found for table: " + tableNameWithType,
            Response.Status.NOT_FOUND);
      }

      IndexLoadingConfig indexLoadingConfig = tableDataManager.fetchIndexLoadingConfig();
      LOGGER.info("Executing re-ingestion for table: {}, segment: {}", tableNameWithType, segmentName);

      // Get TableConfig and Schema
      TableConfig tableConfig = indexLoadingConfig.getTableConfig();
      if (tableConfig == null) {
        throw new WebApplicationException("Table config not found for table: " + tableNameWithType,
            Response.Status.NOT_FOUND);
      }

      Schema schema = indexLoadingConfig.getSchema();
      if (schema == null) {
        throw new WebApplicationException("Schema not found for table: " + tableNameWithType,
            Response.Status.NOT_FOUND);
      }

      // Fetch SegmentZKMetadata
      SegmentZKMetadata segmentZKMetadata = tableDataManager.fetchZKMetadata(segmentName);
      if (segmentZKMetadata == null) {
        throw new WebApplicationException("Segment metadata not found for segment: " + segmentName,
            Response.Status.NOT_FOUND);
      }

      // Get startOffset, endOffset, partitionGroupId
      String startOffsetStr = segmentZKMetadata.getStartOffset();
      String endOffsetStr = segmentZKMetadata.getEndOffset();

      if (startOffsetStr == null || endOffsetStr == null) {
        return Response.serverError().entity("Start offset or end offset is null for segment: " + segmentName).build();
      }

      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      int partitionGroupId = llcSegmentName.getPartitionGroupId();

      Map<String, String> streamConfigMap;
      try {
        streamConfigMap = IngestionConfigUtils.getStreamConfigMaps(tableConfig).get(0);
      } catch (Exception e) {
        return Response.serverError().entity("Failed to get stream config for table: " + tableNameWithType).build();
      }

      StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

      // Set up directories
      File resourceTmpDir = new File(FileUtils.getTempDirectory(), "resourceTmpDir_" + System.currentTimeMillis());
      File resourceDataDir = new File(FileUtils.getTempDirectory(), "resourceDataDir_" + System.currentTimeMillis());

      if (!resourceTmpDir.exists()) {
        resourceTmpDir.mkdirs();
      }
      if (!resourceDataDir.exists()) {
        resourceDataDir.mkdirs();
      }

      LOGGER.info("Starting SimpleRealtimeSegmentDataManager...");
      // Instantiate SimpleRealtimeSegmentDataManager
      SimpleRealtimeSegmentDataManager manager =
          new SimpleRealtimeSegmentDataManager(segmentName, tableNameWithType, partitionGroupId, segmentZKMetadata,
              tableConfig, schema, indexLoadingConfig, streamConfig, startOffsetStr, endOffsetStr, resourceTmpDir,
              resourceDataDir, _serverInstance.getServerMetrics());

      try {

        manager.startConsumption();

        waitForCondition((Void) -> manager.isDoneConsuming(), 1000, 300000, 0);

        manager.stopConsumption();

        // After ingestion is complete, get the segment
        if (!manager.isSuccess()) {
          throw new Exception("Consumer failed to reingest data: " + manager.getConsumptionException());
        }

        LOGGER.info("Starting build for segment {}", segmentName);
        SimpleRealtimeSegmentDataManager.SegmentBuildDescriptor segmentBuildDescriptor =
            manager.buildSegmentInternal();

        // Get the segment directory
        File segmentTarFile = segmentBuildDescriptor.getSegmentTarFile();

        if (segmentTarFile == null) {
          throw new Exception("Failed to build segment: " + segmentName);
        }

        ServerSegmentCompletionProtocolHandler protocolHandler =
            new ServerSegmentCompletionProtocolHandler(_serverInstance.getServerMetrics(), tableNameWithType);

        AuthProvider authProvider = protocolHandler.getAuthProvider();
        List<Header> headers = AuthProviderUtils.toRequestHeaders(authProvider);

        String controllerUrl = getControllerUrl(tableNameWithType, protocolHandler);

        String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
        String segmentStoreUri = indexLoadingConfig.getSegmentStoreURI();
        String destUriStr = StringUtil.join(File.separator, segmentStoreUri, rawTableName,
            SegmentCompletionUtils.generateTmpSegmentFileName(segmentName));
        try (PinotFS pinotFS = PinotFSFactory.create(new URI(segmentStoreUri).getScheme())) {
          // copy segment to deep store
          URI destUri = new URI(destUriStr);
          if (pinotFS.exists(destUri)) {
            pinotFS.delete(destUri, true);
          }
          pinotFS.copyFromLocalFile(segmentTarFile, destUri);
        } catch (Exception e) {
          throw new IOException("Failed to copy segment to deep store: " + destUriStr, e);
        }

        headers.add(new BasicHeader(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI, destUriStr));
        pushSegmentMetadata(tableNameWithType, controllerUrl, segmentTarFile, headers, segmentName, protocolHandler);

        LOGGER.info("Segment metadata pushed, waiting for segment to be uploaded");
        // wait for segment metadata to have status as UPLOADED
        waitForCondition((Void) -> {
          SegmentZKMetadata zkMetadata = tableDataManager.fetchZKMetadata(segmentName);
          if (zkMetadata.getStatus() != CommonConstants.Segment.Realtime.Status.UPLOADED) {
            return false;
          }

          SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segmentName);
          return segmentDataManager instanceof ImmutableSegmentDataManager;
        }, 5000, 300000, 0);

        // trigger segment reset call on API
        LOGGER.info("Triggering segment reset for uploaded segment {}", segmentName);
        HttpClient httpClient = HttpClient.getInstance();
        Map<String, String> headersMap = headers.stream().collect(Collectors.toMap(Header::getName, Header::getValue));
        resetSegment(httpClient, controllerUrl, tableNameWithType, segmentName, null, headersMap);

        LOGGER.info("Re-ingested Segment {} uploaded successfully", segmentName);
      } catch (Exception e) {
        return Response.serverError().entity("Error during re-ingestion: " + e.getMessage()).build();
      } finally {
        // Clean up
        manager.offload();
        manager.destroy();

        // Delete temporary directories
        FileUtils.deleteQuietly(resourceTmpDir);
        FileUtils.deleteQuietly(resourceDataDir);

        isIngesting.set(false);
      }
      // Return success response
      return Response.ok().entity(new ReIngestionResponse("Segment re-ingested and uploaded successfully")).build();
    } catch (Exception e) {
      LOGGER.error("Error during re-ingestion", e);
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    } finally {
      REINGESTION_SEMAPHORE.release();
    }
  }

  private void waitForCondition(
      Function<Void, Boolean> condition, long checkIntervalMs, long timeoutMs, long gracePeriodMs) {
    long endTime = System.currentTimeMillis() + timeoutMs;

    // Adding grace period before starting the condition checks
    if (gracePeriodMs > 0) {
      LOGGER.info("Waiting for a grace period of {} ms before starting condition checks", gracePeriodMs);
      try {
        Thread.sleep(gracePeriodMs);
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted during grace period wait", e);
      }
    }

    while (System.currentTimeMillis() < endTime) {
      try {
        if (Boolean.TRUE.equals(condition.apply(null))) {
          LOGGER.info("Condition satisfied: {}", condition);
          return;
        }
        Thread.sleep(checkIntervalMs);
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while checking the condition", e);
      }
    }

    throw new RuntimeException("Timeout waiting for condition: " + condition);
  }

  public void resetSegment(HttpClient httpClient, String controllerVipUrl, String tableNameWithType, String segmentName,
      String targetInstance, Map<String, String> headers)
      throws IOException {
    try {
      //TODO: send correct headers
      HttpClient.wrapAndThrowHttpException(httpClient.sendJsonPostRequest(
          new URI(getURLForSegmentReset(controllerVipUrl, tableNameWithType, segmentName, targetInstance)), null,
          headers));
    } catch (HttpErrorStatusException | URISyntaxException e) {
      throw new IOException(e);
    }
  }

  private String getURLForSegmentReset(String controllerVipUrl, String tableNameWithType, String segmentName,
      @Nullable String targetInstance) {
    String query = targetInstance == null ? "reset" : "reset?targetInstance=" + targetInstance;
    return StringUtil.join("/", controllerVipUrl, "segments", tableNameWithType, encode(segmentName), query);
  }

  private String encode(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }

  /**
   * Push segment metadata to the Pinot Controller in METADATA mode.
   *
   * @param tableNameWithType The table name with type (e.g., "myTable_OFFLINE")
   * @param controllerUrl The base URL of the Pinot Controller (e.g., "http://controller-host:9000")
   * @param segmentFile   The local segment tar.gz file
   * @param authHeaders   A map of authentication or additional headers for the request
   */
  public void pushSegmentMetadata(String tableNameWithType, String controllerUrl, File segmentFile,
      List<Header> authHeaders, String segmentName, ServerSegmentCompletionProtocolHandler protocolHandler)
      throws Exception {
    LOGGER.info("Pushing metadata of segment {} of table {} to controller: {}", segmentFile.getName(),
        tableNameWithType, controllerUrl);
    String tableName = tableNameWithType;
    File segmentMetadataFile = generateSegmentMetadataTar(segmentFile);

    LOGGER.info("Generated segment metadata tar file: {}", segmentMetadataFile.getAbsolutePath());
    try {
      // Prepare headers
      List<Header> headers = authHeaders;

      // The upload type must be METADATA
      headers.add(new BasicHeader(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE,
          FileUploadDownloadClient.FileUploadType.METADATA.toString()));

      headers.add(new BasicHeader(FileUploadDownloadClient.CustomHeaders.COPY_SEGMENT_TO_DEEP_STORE, "true"));

      // Set table name parameter
      List<NameValuePair> parameters = getSegmentPushCommonParams(tableNameWithType);

      // Construct the endpoint URI
      URI uploadEndpoint = FileUploadDownloadClient.getUploadSegmentURI(new URI(controllerUrl));

      LOGGER.info("Uploading segment metadata to: {} with headers: {}", uploadEndpoint, headers);

      // Perform the metadata upload
      SimpleHttpResponse response = protocolHandler.getFileUploadDownloadClient()
          .uploadSegmentMetadata(uploadEndpoint, segmentName, segmentMetadataFile, headers, parameters,
              HttpClient.DEFAULT_SOCKET_TIMEOUT_MS);

      LOGGER.info("Response for pushing metadata of segment {} of table {} to {} - {}: {}", segmentName, tableName,
          controllerUrl, response.getStatusCode(), response.getResponse());
    } finally {
      FileUtils.deleteQuietly(segmentMetadataFile);
    }
  }

  private List<NameValuePair> getSegmentPushCommonParams(String tableNameWithType) {
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION,
        "true"));
    params.add(new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME,
        TableNameBuilder.extractRawTableName(tableNameWithType)));
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    if (tableType != null) {
      params.add(new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_TYPE, tableType.toString()));
    } else {
      throw new RuntimeException(String.format("Failed to determine the tableType from name: %s", tableNameWithType));
    }
    return params;
  }

  /**
   * Generate a tar.gz file containing only the metadata files (metadata.properties, creation.meta)
   * from a given Pinot segment tar.gz file.
   */
  private File generateSegmentMetadataTar(File segmentTarFile)
      throws Exception {

    if (!segmentTarFile.exists()) {
      throw new IllegalArgumentException("Segment tar file does not exist: " + segmentTarFile.getAbsolutePath());
    }

    LOGGER.info("Generating segment metadata tar file from segment tar: {}", segmentTarFile.getAbsolutePath());
    File tempDir = Files.createTempDirectory("pinot-segment-temp").toFile();
    String uuid = UUID.randomUUID().toString();
    try {
      File metadataDir = new File(tempDir, "segmentMetadataDir-" + uuid);
      if (!metadataDir.mkdirs()) {
        throw new RuntimeException("Failed to create metadata directory: " + metadataDir.getAbsolutePath());
      }

      LOGGER.info("Trying to untar Metadata file from: [{}] to [{}]", segmentTarFile, metadataDir);
      TarCompressionUtils.untarOneFile(segmentTarFile, V1Constants.MetadataKeys.METADATA_FILE_NAME,
          new File(metadataDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));

      // Extract creation.meta
      LOGGER.info("Trying to untar CreationMeta file from: [{}] to [{}]", segmentTarFile, metadataDir);
      TarCompressionUtils.untarOneFile(segmentTarFile, V1Constants.SEGMENT_CREATION_META,
          new File(metadataDir, V1Constants.SEGMENT_CREATION_META));

      File segmentMetadataFile =
          new File(FileUtils.getTempDirectory(), "segmentMetadata-" + UUID.randomUUID() + ".tar.gz");
      TarCompressionUtils.createCompressedTarFile(metadataDir, segmentMetadataFile);
      return segmentMetadataFile;
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  private String getControllerUrl(String rawTableName, ServerSegmentCompletionProtocolHandler protocolHandler) {
    ControllerLeaderLocator leaderLocator = ControllerLeaderLocator.getInstance();
    final Pair<String, Integer> leaderHostPort = leaderLocator.getControllerLeader(rawTableName);
    if (leaderHostPort == null) {
      LOGGER.warn("No leader found for table: {}", rawTableName);
      return null;
    }
    Integer port = leaderHostPort.getRight();
    String protocol = protocolHandler.getProtocol();
    Integer controllerHttpsPort = protocolHandler.getControllerHttpsPort();
    if (controllerHttpsPort != null) {
      port = controllerHttpsPort;
      protocol = HTTPS_PROTOCOL;
    }

    return URIUtils.buildURI(protocol, leaderHostPort.getLeft() + ":" + port, "", Collections.emptyMap()).toString();
  }
}
