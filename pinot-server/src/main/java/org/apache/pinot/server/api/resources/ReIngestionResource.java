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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
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
import org.apache.pinot.server.api.resources.reingestion.utils.StatelessRealtimeSegmentDataManager;
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

  //TODO: Make this configurable
  private static final int MIN_REINGESTION_THREADS = 4;
  private static final int MAX_PARALLEL_REINGESTIONS = 8;

  // Tracks if a particular segment is currently being re-ingested
  private static final ConcurrentHashMap<String, AtomicBoolean>
      SEGMENT_INGESTION_MAP = new ConcurrentHashMap<>();

  // Executor for asynchronous re-ingestion
  private static final ExecutorService REINGESTION_EXECUTOR =
      new ThreadPoolExecutor(MIN_REINGESTION_THREADS, MAX_PARALLEL_REINGESTIONS, 0L, TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<>(), // unbounded queue for the reingestion tasks
          new ThreadFactoryBuilder().setNameFormat("reingestion-worker-%d").build());

  // Keep track of jobs by jobId => job info
  private static final ConcurrentHashMap<String, ReIngestionJob> RUNNING_JOBS = new ConcurrentHashMap<>();
  public static final long CONSUMPTION_END_TIMEOUT_MS = Duration.ofMinutes(30).toMillis();
  public static final long UPLOAD_END_TIMEOUT_MS = Duration.ofMinutes(5).toMillis();
  public static final long CHECK_INTERVAL_MS = Duration.ofSeconds(5).toMillis();

  @Inject
  private ServerInstance _serverInstance;

  /**
   * Simple data class to hold job details.
   */
  private static class ReIngestionJob {
    private final String _jobId;
    private final String _tableNameWithType;
    private final String _segmentName;
    private final long _startTimeMs;

    ReIngestionJob(String jobId, String tableNameWithType, String segmentName) {
      _jobId = jobId;
      _tableNameWithType = tableNameWithType;
      _segmentName = segmentName;
      _startTimeMs = System.currentTimeMillis();
    }

    public String getJobId() {
      return _jobId;
    }

    public String getTableNameWithType() {
      return _tableNameWithType;
    }

    public String getSegmentName() {
      return _segmentName;
    }

    public long getStartTimeMs() {
      return _startTimeMs;
    }
  }

  /**
   * New API to get all running re-ingestion jobs.
   */
  @GET
  @Path("/reingestSegment/jobs")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Get all running re-ingestion jobs along with job IDs")
  public Response getAllRunningReingestionJobs() {
    // Filter only the jobs still marked as running
    List<ReIngestionJob> runningJobs = new ArrayList<>(RUNNING_JOBS.values());
    return Response.ok(runningJobs).build();
  }

  @POST
  @Path("/reingestSegment")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Re-ingest segment asynchronously", notes = "Returns a jobId immediately; ingestion runs in "
      + "background.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success", response = ReIngestionResponse.class),
      @ApiResponse(code = 500, message = "Internal server error", response = ErrorInfo.class)
  })
  public Response reIngestSegment(ReIngestionRequest request) {
    String tableNameWithType = request.getTableNameWithType();
    String segmentName = request.getSegmentName();

    InstanceDataManager instanceDataManager = _serverInstance.getInstanceDataManager();
    if (instanceDataManager == null) {
      throw new WebApplicationException("Invalid server initialization", Response.Status.INTERNAL_SERVER_ERROR);
    }

    TableDataManager tableDataManager = instanceDataManager.getTableDataManager(tableNameWithType);
    if (tableDataManager == null) {
      throw new WebApplicationException("Table data manager not found for table: " + tableNameWithType,
          Response.Status.NOT_FOUND);
    }

    IndexLoadingConfig indexLoadingConfig = tableDataManager.fetchIndexLoadingConfig();
    LOGGER.info("Executing re-ingestion for table: {}, segment: {}", tableNameWithType, segmentName);

    // Get TableConfig, Schema, ZK metadata
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

    SegmentZKMetadata segmentZKMetadata = tableDataManager.fetchZKMetadata(segmentName);
    if (segmentZKMetadata == null) {
      throw new WebApplicationException("Segment metadata not found for segment: " + segmentName,
          Response.Status.NOT_FOUND);
    }

    // Check if download url is present
    if (segmentZKMetadata.getDownloadUrl() != null) {
      throw new WebApplicationException(
          "Download URL is already present for segment: " + segmentName + ". No need to re-ingest.",
          Response.Status.BAD_REQUEST);
    }

    // Grab start/end offsets
    String startOffsetStr = segmentZKMetadata.getStartOffset();
    String endOffsetStr = segmentZKMetadata.getEndOffset();
    if (startOffsetStr == null || endOffsetStr == null) {
      throw new WebApplicationException("Null start/end offset for segment: " + segmentName,
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    // Check if this segment is already being re-ingested
    AtomicBoolean isIngesting = SEGMENT_INGESTION_MAP.computeIfAbsent(segmentName, k -> new AtomicBoolean(false));
    if (!isIngesting.compareAndSet(false, true)) {
      return Response.status(Response.Status.CONFLICT)
          .entity("Re-ingestion for segment: " + segmentName + " is already in progress.")
          .build();
    }

    // Generate a jobId for tracking
    String jobId = UUID.randomUUID().toString();
    ReIngestionJob job = new ReIngestionJob(jobId, tableNameWithType, segmentName);

    // Kick off the actual work asynchronously
    REINGESTION_EXECUTOR.submit(() -> {
      try {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        int partitionGroupId = llcSegmentName.getPartitionGroupId();

        Map<String, String> streamConfigMap = IngestionConfigUtils.getStreamConfigMaps(tableConfig).get(0);
        StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

        StatelessRealtimeSegmentDataManager manager = new StatelessRealtimeSegmentDataManager(
            segmentName, tableNameWithType, partitionGroupId, segmentZKMetadata, tableConfig, schema,
            indexLoadingConfig, streamConfig, startOffsetStr, endOffsetStr, _serverInstance.getServerMetrics());

        RUNNING_JOBS.put(jobId, job);
        doReIngestSegment(manager, segmentName, tableNameWithType, indexLoadingConfig, tableDataManager);
      } catch (Exception e) {
        LOGGER.error("Error during async re-ingestion for job {} (segment={})", jobId, segmentName, e);
      } finally {
        isIngesting.set(false);
        RUNNING_JOBS.remove(jobId);
      }
    });

    ReIngestionResponse immediateResponse = new ReIngestionResponse(
        "Re-ingestion job submitted successfully with jobId: " + jobId);
    return Response.ok(immediateResponse).build();
  }

  /**
   * The actual re-ingestion logic, moved into a separate method for clarity.
   * This is essentially the old synchronous logic you had in reIngestSegment.
   */
  private void doReIngestSegment(StatelessRealtimeSegmentDataManager manager, String segmentName,
      String tableNameWithType, IndexLoadingConfig indexLoadingConfig, TableDataManager tableDataManager)
      throws Exception {
    try {
      manager.startConsumption();
      waitForCondition((Void) -> manager.isDoneConsuming(), CHECK_INTERVAL_MS,
          CONSUMPTION_END_TIMEOUT_MS, 0);
      manager.stopConsumption();

      if (!manager.isSuccess()) {
        throw new Exception("Consumer failed: " + manager.getConsumptionException());
      }

      LOGGER.info("Starting build for segment {}", segmentName);
      StatelessRealtimeSegmentDataManager.SegmentBuildDescriptor segmentBuildDescriptor =
          manager.buildSegmentInternal();

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
        URI destUri = new URI(destUriStr);
        if (pinotFS.exists(destUri)) {
          pinotFS.delete(destUri, true);
        }
        pinotFS.copyFromLocalFile(segmentTarFile, destUri);
      }

      headers.add(new BasicHeader(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI, destUriStr));
      pushSegmentMetadata(tableNameWithType, controllerUrl, segmentTarFile, headers, segmentName, protocolHandler);

      // Wait for segment to be uploaded
      waitForCondition((Void) -> {
        SegmentZKMetadata zkMetadata = tableDataManager.fetchZKMetadata(segmentName);
        if (zkMetadata.getStatus() != CommonConstants.Segment.Realtime.Status.UPLOADED) {
          return false;
        }
        SegmentDataManager segDataManager = tableDataManager.acquireSegment(segmentName);
        return segDataManager instanceof ImmutableSegmentDataManager;
      }, CHECK_INTERVAL_MS, UPLOAD_END_TIMEOUT_MS, 0);

      // Trigger segment reset
      HttpClient httpClient = HttpClient.getInstance();
      Map<String, String> headersMap = headers.stream()
          .collect(Collectors.toMap(Header::getName, Header::getValue));
      resetSegment(httpClient, controllerUrl, tableNameWithType, segmentName, null, headersMap);

      LOGGER.info("Re-ingested segment {} uploaded successfully", segmentName);
    } finally {
      manager.offload();
      manager.destroy();
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
