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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionManager;
import org.apache.pinot.controller.helix.core.realtime.segment.CommittingSegmentDescriptor;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.core.data.manager.realtime.SegmentCompletionUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// Do NOT tag this class with @Api. We don't want these exposed in swagger.
// @Api(tags = Constants.INTERNAL_TAG)
@Path("/")
public class LLCSegmentCompletionHandlers {
  private static final Logger LOGGER = LoggerFactory.getLogger(LLCSegmentCompletionHandlers.class);
  private static final Object SEGMENT_UPLOAD_LOCK = new Object();
  private static final String SCHEME = "file://";

  @Inject
  SegmentCompletionManager _segmentCompletionManager;

  @VisibleForTesting
  public static String getScheme() {
    return SCHEME;
  }

  // We don't want to document these in swagger since they are internal APIs
  @GET
  @Path(SegmentCompletionProtocol.MSG_TYPE_EXTEND_BUILD_TIME)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_ADMIN_INFO)
  @Produces(MediaType.APPLICATION_JSON)
  public String extendBuildTime(@QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_STREAM_PARTITION_MSG_OFFSET) String streamPartitionMsgOffset,
      @QueryParam(SegmentCompletionProtocol.PARAM_EXTRA_TIME_SEC) int extraTimeSec) {
    if (instanceId == null || segmentName == null || streamPartitionMsgOffset == null) {
      LOGGER.error("Invalid call: segmentName={}, instanceId={}, streamPartitionMsgOffset={}", segmentName, instanceId,
          streamPartitionMsgOffset);
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }
    if (extraTimeSec <= 0) {
      LOGGER.warn("Invalid value {} for extra build time from instance {} for segment {}", extraTimeSec, instanceId,
          segmentName);
      extraTimeSec = SegmentCompletionProtocol.getDefaultMaxSegmentCommitTimeSeconds();
    }

    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params()
        .withInstanceId(instanceId)
        .withSegmentName(segmentName)
        .withStreamPartitionMsgOffset(streamPartitionMsgOffset)
        .withExtraTimeSec(extraTimeSec);
    LOGGER.info("Processing extendBuildTime: {}", requestParams);

    String response = _segmentCompletionManager.extendBuildTime(requestParams).toJsonString();
    LOGGER.info("Response to extendBuildTime: {}", response);
    return response;
  }

  @GET
  @Path(SegmentCompletionProtocol.MSG_TYPE_CONSUMED)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_ADMIN_INFO)
  @Produces(MediaType.APPLICATION_JSON)
  public String segmentConsumed(@QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_STREAM_PARTITION_MSG_OFFSET) String streamPartitionMsgOffset,
      @QueryParam(SegmentCompletionProtocol.PARAM_REASON) String stopReason,
      @QueryParam(SegmentCompletionProtocol.PARAM_MEMORY_USED_BYTES) long memoryUsedBytes,
      @QueryParam(SegmentCompletionProtocol.PARAM_ROW_COUNT) int numRows) {
    if (instanceId == null || segmentName == null || streamPartitionMsgOffset == null) {
      LOGGER.error("Invalid call: segmentName={}, instanceId={}, streamPartitionMsgOffset={}", segmentName, instanceId,
          streamPartitionMsgOffset);
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }

    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params()
        .withInstanceId(instanceId)
        .withSegmentName(segmentName)
        .withStreamPartitionMsgOffset(streamPartitionMsgOffset)
        .withReason(stopReason)
        .withMemoryUsedBytes(memoryUsedBytes)
        .withNumRows(numRows);
    LOGGER.info("Processing segmentConsumed: {}", requestParams);

    String response = _segmentCompletionManager.segmentConsumed(requestParams).toJsonString();
    LOGGER.info("Response to segmentConsumed for segment: {} is: {}", segmentName, response);
    return response;
  }

  @GET
  @Path(SegmentCompletionProtocol.MSG_TYPE_STOPPED_CONSUMING)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_ADMIN_INFO)
  @Produces(MediaType.APPLICATION_JSON)
  public String segmentStoppedConsuming(@QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_STREAM_PARTITION_MSG_OFFSET) String streamPartitionMsgOffset,
      @QueryParam(SegmentCompletionProtocol.PARAM_REASON) String stopReason) {
    if (instanceId == null || segmentName == null || streamPartitionMsgOffset == null) {
      LOGGER.error("Invalid call: segmentName={}, instanceId={}, streamPartitionMsgOffset={}", segmentName, instanceId,
          streamPartitionMsgOffset);
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }

    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params()
        .withInstanceId(instanceId)
        .withSegmentName(segmentName)
        .withStreamPartitionMsgOffset(streamPartitionMsgOffset)
        .withReason(stopReason);
    LOGGER.info("Processing segmentStoppedConsuming: {}", requestParams);

    String response = _segmentCompletionManager.segmentStoppedConsuming(requestParams).toJsonString();
    LOGGER.info("Response to segmentStoppedConsuming for segment: {} is: {}", segmentName, response);
    return response;
  }

  @GET
  @Path(SegmentCompletionProtocol.MSG_TYPE_COMMIT_START)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_ADMIN_INFO)
  @Produces(MediaType.APPLICATION_JSON)
  public String segmentCommitStart(@QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_STREAM_PARTITION_MSG_OFFSET) String streamPartitionMsgOffset,
      @QueryParam(SegmentCompletionProtocol.PARAM_MEMORY_USED_BYTES) long memoryUsedBytes,
      @QueryParam(SegmentCompletionProtocol.PARAM_BUILD_TIME_MILLIS) long buildTimeMillis,
      @QueryParam(SegmentCompletionProtocol.PARAM_WAIT_TIME_MILLIS) long waitTimeMillis,
      @QueryParam(SegmentCompletionProtocol.PARAM_ROW_COUNT) int numRows,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_SIZE_BYTES) long segmentSizeBytes) {
    if (instanceId == null || segmentName == null || streamPartitionMsgOffset == null) {
      LOGGER.error("Invalid call: segmentName={}, instanceId={}, streamPartitionMsgOffset={}", segmentName, instanceId,
          streamPartitionMsgOffset);
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }

    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params()
        .withInstanceId(instanceId)
        .withSegmentName(segmentName)
        .withStreamPartitionMsgOffset(streamPartitionMsgOffset)
        .withMemoryUsedBytes(memoryUsedBytes)
        .withBuildTimeMillis(buildTimeMillis)
        .withWaitTimeMillis(waitTimeMillis)
        .withNumRows(numRows)
        .withSegmentSizeBytes(segmentSizeBytes);
    LOGGER.info("Processing segmentCommitStart: {}", requestParams);

    String response = _segmentCompletionManager.segmentCommitStart(requestParams).toJsonString();
    LOGGER.info("Response to segmentCommitStart for segment: {} is: {}", segmentName, response);
    return response;
  }

  // This method may be called in any controller, leader or non-leader. It is used only when the server decides to use
  // split commit protocol for the segment commit.
  // TODO: remove this API. Should not upload segment via controller
  @POST
  @Path(SegmentCompletionProtocol.MSG_TYPE_SEGMENT_UPLOAD)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.UPLOAD_SEGMENT)
  @Authenticate(AccessType.CREATE)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @TrackInflightRequestMetrics
  @TrackedByGauge(gauge = ControllerGauge.SEGMENT_UPLOADS_IN_PROGRESS)
  public String segmentUpload(@QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_STREAM_PARTITION_MSG_OFFSET) String streamPartitionMsgOffset,
      FormDataMultiPart multiPart) {
    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params()
        .withInstanceId(instanceId)
        .withSegmentName(segmentName)
        .withStreamPartitionMsgOffset(streamPartitionMsgOffset);
    LOGGER.info("Processing segmentUpload: {}", requestParams);

    // Get the segment from the form input and put it into the data directory (could be remote)
    File localTempFile = null;
    try {
      localTempFile = extractSegmentFromFormToLocalTempFile(multiPart, segmentName);
      String rawTableName = new LLCSegmentName(segmentName).getTableName();
      URI segmentFileURI =
          URIUtils.getUri(ControllerFilePathProvider.getInstance().getDataDirURI().toString(), rawTableName,
              URIUtils.encode(SegmentCompletionUtils.generateTmpSegmentFileName(segmentName)));
      PinotFSFactory.create(segmentFileURI.getScheme()).copyFromLocalFile(localTempFile, segmentFileURI);
      SegmentCompletionProtocol.Response.Params responseParams = new SegmentCompletionProtocol.Response.Params()
          .withStreamPartitionMsgOffset(requestParams.getStreamPartitionMsgOffset())
          .withSegmentLocation(segmentFileURI.toString())
          .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.UPLOAD_SUCCESS);

      String response = new SegmentCompletionProtocol.Response(responseParams).toJsonString();
      LOGGER.info("Response to segmentUpload for segment: {} is: {}", segmentName, response);
      return response;
    } catch (Exception e) {
      LOGGER.error("Caught exception while uploading segment: {} from instance: {}", segmentName, instanceId, e);
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    } finally {
      FileUtils.deleteQuietly(localTempFile);
    }
  }

  @POST
  @Path(SegmentCompletionProtocol.MSG_TYPE_COMMIT_END_METADATA)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.COMMIT_SEGMENT)
  @Authenticate(AccessType.CREATE)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public String segmentCommitEndWithMetadata(@QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_LOCATION) String segmentLocation,
      @QueryParam(SegmentCompletionProtocol.PARAM_STREAM_PARTITION_MSG_OFFSET) String streamPartitionMsgOffset,
      @QueryParam(SegmentCompletionProtocol.PARAM_MEMORY_USED_BYTES) long memoryUsedBytes,
      @QueryParam(SegmentCompletionProtocol.PARAM_BUILD_TIME_MILLIS) long buildTimeMillis,
      @QueryParam(SegmentCompletionProtocol.PARAM_WAIT_TIME_MILLIS) long waitTimeMillis,
      @QueryParam(SegmentCompletionProtocol.PARAM_ROW_COUNT) int numRows,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_SIZE_BYTES) long segmentSizeBytes,
      @QueryParam(SegmentCompletionProtocol.PARAM_REASON) String stopReason, FormDataMultiPart metadataFiles) {
    if (instanceId == null || segmentName == null || segmentLocation == null || metadataFiles == null
        || streamPartitionMsgOffset == null) {
      LOGGER.error("Invalid call: segmentName={}, instanceId={}, segmentLocation={}, streamPartitionMsgOffset={}",
          segmentName, instanceId, segmentLocation, streamPartitionMsgOffset);
      // TODO: memoryUsedInBytes = 0 if not present in params. Add validation when we start using it
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }

    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params()
        .withInstanceId(instanceId)
        .withSegmentName(segmentName)
        .withSegmentLocation(segmentLocation)
        .withStreamPartitionMsgOffset(streamPartitionMsgOffset)
        .withSegmentSizeBytes(segmentSizeBytes)
        .withBuildTimeMillis(buildTimeMillis)
        .withWaitTimeMillis(waitTimeMillis)
        .withNumRows(numRows)
        .withMemoryUsedBytes(memoryUsedBytes)
        .withReason(stopReason);
    LOGGER.info("Processing segmentCommitEndWithMetadata: {}", requestParams);

    SegmentMetadataImpl segmentMetadata;
    try {
      segmentMetadata = extractSegmentMetadataFromForm(metadataFiles, segmentName);
    } catch (Exception e) {
      LOGGER.error("Caught exception while extracting metadata for segment: {} from instance: {}", segmentName,
          instanceId, e);
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }

    String response = _segmentCompletionManager.segmentCommitEnd(requestParams,
            CommittingSegmentDescriptor.fromSegmentCompletionReqParamsAndMetadata(requestParams, segmentMetadata))
        .toJsonString();
    LOGGER.info("Response to segmentCommitEndWithMetadata for segment: {} is: {}", segmentName, response);
    return response;
  }

  @GET
  @Path(SegmentCompletionProtocol.MSG_TYPE_CANNOT_BUILD)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_ADMIN_INFO)
  @Produces(MediaType.APPLICATION_JSON)
  public String reduceSegmentSize(@QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_ROW_COUNT) int numRows) {
    if (instanceId == null || segmentName == null || numRows <= 0) {
      LOGGER.error("Invalid call: segmentName={}, instanceId={}, numRowsCount={}", segmentName, instanceId,
          numRows);
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }

    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params()
        .withInstanceId(instanceId)
        .withSegmentName(segmentName)
        .withNumRows(numRows);
    LOGGER.info("Processing segmentStoppedConsuming: {}", requestParams);

    return _segmentCompletionManager.reduceSegmentSizeAndReset(requestParams).toJsonString();
  }

  /**
   * Extracts the segment file from the form into a local temporary file under file upload temporary directory.
   */
  private static File extractSegmentFromFormToLocalTempFile(FormDataMultiPart form, String segmentName)
      throws IOException {
    try {
      Map<String, List<FormDataBodyPart>> map = form.getFields();
      Preconditions.checkState(PinotSegmentUploadDownloadRestletResource.validateMultiPart(map, segmentName),
          "Invalid multi-part for segment: %s", segmentName);
      FormDataBodyPart bodyPart = map.values().iterator().next().get(0);

      File localTempFile = org.apache.pinot.common.utils.FileUtils.concatAndValidateFile(
          ControllerFilePathProvider.getInstance().getFileUploadTempDir(), getTempSegmentFileName(segmentName),
          "Invalid segment name: %s", segmentName);

      try (InputStream inputStream = bodyPart.getValueAs(InputStream.class)) {
        Files.copy(inputStream, localTempFile.toPath());
      } catch (Exception e) {
        FileUtils.deleteQuietly(localTempFile);
        throw e;
      }
      return localTempFile;
    } finally {
      form.cleanup();
    }
  }

  /**
   * Extracts the segment metadata from the local segment file. Use the untarred file temporary directory to store the
   * metadata files temporarily.
   */
  private static SegmentMetadataImpl extractMetadataFromLocalSegmentFile(File segmentFile)
      throws Exception {
    File tempIndexDir = org.apache.pinot.common.utils.FileUtils.concatAndValidateFile(
        ControllerFilePathProvider.getInstance().getUntarredFileTempDir(), segmentFile.getName(),
        "Invalid segment file: %s", segmentFile);

    try {
      FileUtils.forceMkdir(tempIndexDir);

      // Extract metadata.properties
      TarCompressionUtils.untarOneFile(segmentFile, V1Constants.MetadataKeys.METADATA_FILE_NAME,
          new File(tempIndexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));

      // Extract creation.meta
      TarCompressionUtils.untarOneFile(segmentFile, V1Constants.SEGMENT_CREATION_META,
          new File(tempIndexDir, V1Constants.SEGMENT_CREATION_META));

      // Load segment metadata
      return new SegmentMetadataImpl(tempIndexDir);
    } finally {
      FileUtils.deleteQuietly(tempIndexDir);
    }
  }

  /**
   * Extracts the segment metadata from the form. Use the untarred file temporary directory to store the metadata files
   * temporarily.
   */
  private static SegmentMetadataImpl extractSegmentMetadataFromForm(FormDataMultiPart form, String segmentName)
      throws IOException, ConfigurationException {
    File tempIndexDir = org.apache.pinot.common.utils.FileUtils.concatAndValidateFile(
        ControllerFilePathProvider.getInstance().getUntarredFileTempDir(), getTempSegmentFileName(segmentName),
        "Invalid segment name: %s", segmentName);

    try {
      FileUtils.forceMkdir(tempIndexDir);

      // Extract metadata.properties
      extractFileFromForm(form, V1Constants.MetadataKeys.METADATA_FILE_NAME, tempIndexDir);

      // Extract creation.meta
      extractFileFromForm(form, V1Constants.SEGMENT_CREATION_META, tempIndexDir);

      // Load segment metadata
      return new SegmentMetadataImpl(tempIndexDir);
    } finally {
      FileUtils.deleteQuietly(tempIndexDir);
    }
  }

  /**
   * Extracts a file from the form into the given directory.
   */
  private static void extractFileFromForm(FormDataMultiPart form, String fileName, File outputDir)
      throws IOException {
    FormDataBodyPart bodyPart = form.getField(fileName);
    Preconditions.checkState(bodyPart != null, "Failed to find: %s", fileName);

    try (InputStream inputStream = bodyPart.getValueAs(InputStream.class)) {
      Files.copy(inputStream, new File(outputDir, fileName).toPath());
    }
  }

  private static String getTempSegmentFileName(String segmentName) {
    return segmentName + "." + UUID.randomUUID();
  }
}
