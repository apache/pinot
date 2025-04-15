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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.segment.local.realtime.writer.StatelessRealtimeSegmentWriter;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


@Api(tags = "Reingestion", authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```"),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")}))
@Path("/")
public class ReingestionResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReingestionResource.class);

  // TODO: Make them configurable
  private static final int MAX_PARALLEL_REINGESTIONS = Math.max(Runtime.getRuntime().availableProcessors() / 2, 1);
  public static final long CONSUMPTION_END_TIMEOUT_MS = Duration.ofMinutes(30).toMillis();
  public static final long CHECK_INTERVAL_MS = Duration.ofSeconds(5).toMillis();

  // Tracks if a particular segment is currently being re-ingested
  private final ConcurrentHashMap<String, AtomicBoolean> _reingestingSegments = new ConcurrentHashMap<>();

  // Executor for asynchronous re-ingestion
  private final ExecutorService _reingestionExecutor = Executors.newFixedThreadPool(MAX_PARALLEL_REINGESTIONS,
      new ThreadFactoryBuilder().setNameFormat("reingestion-worker-%d").build());

  // Keep track of jobs by jobId => job info
  private final ConcurrentHashMap<String, ReingestionJob> _runningJobs = new ConcurrentHashMap<>();

  @Inject
  private ServerInstance _serverInstance;

  /**
   * Simple data class to hold job details.
   */
  public static class ReingestionJob {
    private final String _jobId;
    private final String _segmentName;
    private final long _startTimeMs;

    @JsonCreator
    ReingestionJob(@JsonProperty("jobId") String jobId, @JsonProperty("segmentName") String segmentName) {
      _jobId = jobId;
      _segmentName = segmentName;
      _startTimeMs = System.currentTimeMillis();
    }

    public String getJobId() {
      return _jobId;
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
    List<ReingestionJob> runningJobs = new ArrayList<>(_runningJobs.values());
    return Response.ok(runningJobs).build();
  }

  @POST
  @Path("/reingestSegment/{segmentName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Re-ingest segment asynchronously", notes = "Returns a jobId immediately; ingestion runs in "
      + "background.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success", response = ReingestionJob.class),
      @ApiResponse(code = 500, message = "Internal server error", response = ErrorInfo.class)
  })
  public Response reingestSegment(@PathParam("segmentName") String segmentName) {
    LOGGER.info("Re-ingesting segment: {}", segmentName);

    // if segment is not in LLC format, return error
    if (!LLCSegmentName.isLLCSegment(segmentName)) {
      throw new WebApplicationException("Segment name is not in LLC format: " + segmentName,
          Response.Status.BAD_REQUEST);
    }
    LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(llcSegmentName.getTableName());

    RealtimeTableDataManager tableDataManager =
        (RealtimeTableDataManager) _serverInstance.getInstanceDataManager().getTableDataManager(realtimeTableName);
    if (tableDataManager == null) {
      throw new WebApplicationException("Table data manager not found for table: " + realtimeTableName,
          Response.Status.NOT_FOUND);
    }

    SegmentZKMetadata segmentZKMetadata;
    try {
      segmentZKMetadata = tableDataManager.fetchZKMetadata(segmentName);
    } catch (Exception e) {
      throw new WebApplicationException("Segment ZK metadata not found for segment: " + segmentName,
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

    IndexLoadingConfig indexLoadingConfig = tableDataManager.fetchIndexLoadingConfig();

    // Check if this segment is already being re-ingested
    AtomicBoolean isIngesting = _reingestingSegments.computeIfAbsent(segmentName, k -> new AtomicBoolean(false));
    if (!isIngesting.compareAndSet(false, true)) {
      return Response.status(Response.Status.CONFLICT)
          .entity("Re-ingestion for segment: " + segmentName + " is already in progress.")
          .build();
    }

    // Generate a jobId for tracking
    String jobId = UUID.randomUUID().toString();
    ReingestionJob job = new ReingestionJob(jobId, segmentName);

    // Kick off the actual work asynchronously
    _reingestionExecutor.submit(() -> {
      try {
        _runningJobs.put(jobId, job);
        doReingestSegment(realtimeTableName, segmentZKMetadata, indexLoadingConfig,
            tableDataManager.getSegmentBuildSemaphore());
      } catch (Exception e) {
        LOGGER.error("Error during async re-ingestion for job {} (segment={})", jobId, segmentName, e);
        _serverInstance.getServerMetrics()
            .addMeteredTableValue(realtimeTableName, ServerMeter.SEGMENT_REINGESTION_FAILURE, 1);
      } finally {
        _runningJobs.remove(jobId);
        _reingestingSegments.remove(segmentName);
      }
    });

    return Response.ok(job).build();
  }

  /**
   * The actual re-ingestion logic, moved into a separate method for clarity.
   * This is essentially the old synchronous logic you had in reingestSegment.
   */
  private void doReingestSegment(String realtimeTableName, SegmentZKMetadata segmentZKMetadata,
      IndexLoadingConfig indexLoadingConfig, @Nullable Semaphore segmentBuildSemaphore)
      throws Exception {
    String segmentName = segmentZKMetadata.getSegmentName();
    try (StatelessRealtimeSegmentWriter writer = new StatelessRealtimeSegmentWriter(segmentZKMetadata,
        indexLoadingConfig, segmentBuildSemaphore)) {
      writer.startConsumption();
      waitForCondition((Void) -> writer.isDoneConsuming(), CHECK_INTERVAL_MS, CONSUMPTION_END_TIMEOUT_MS, 0);
      writer.stopConsumption();

      if (!writer.isSuccess()) {
        throw new RuntimeException("Failed to consume records", writer.getConsumptionException());
      }

      File segmentTarFile = writer.buildSegment();

      ServerSegmentCompletionProtocolHandler protocolHandler =
          new ServerSegmentCompletionProtocolHandler(_serverInstance.getServerMetrics(), realtimeTableName);
      protocolHandler.uploadReingestedSegment(segmentName, indexLoadingConfig.getSegmentStoreURI(), segmentTarFile);

      LOGGER.info("Re-ingested segment {} uploaded successfully", segmentName);
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
}
