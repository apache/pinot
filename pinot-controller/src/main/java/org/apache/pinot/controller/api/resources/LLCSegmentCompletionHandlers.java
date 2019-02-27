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

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.google.common.base.Preconditions;
import org.apache.commons.httpclient.URI;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionManager;
import org.apache.pinot.controller.helix.core.realtime.segment.CommittingSegmentDescriptor;
import org.apache.pinot.controller.util.SegmentCompletionUtils;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.apache.pinot.filesystem.PinotFS;
import org.apache.pinot.filesystem.PinotFSFactory;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// Do NOT tag this class with @Api. We don't want these exposed in swagger.
// @Api(tags = Constants.INTERNAL_TAG)
@Path("/")
public class LLCSegmentCompletionHandlers {

  private static Logger LOGGER = LoggerFactory.getLogger(LLCSegmentCompletionHandlers.class);
  private static final String SCHEME = "file://";
  private static final String METADATA_TEMP_DIR_SUFFIX = ".metadata.tmp";

  @Inject
  ControllerConf _controllerConf;

  @VisibleForTesting
  public static String getScheme() {
    return SCHEME;
  }

  // We don't want to document these in swagger since they are internal APIs
  @GET
  @Path(SegmentCompletionProtocol.MSG_TYPE_EXTEND_BUILD_TIME)
  @Produces(MediaType.APPLICATION_JSON)
  public String extendBuildTime(@QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset,
      @QueryParam(SegmentCompletionProtocol.PARAM_EXTRA_TIME_SEC) int extraTimeSec) {

    if (instanceId == null || segmentName == null || offset == -1) {
      LOGGER.error("Invalid call: offset={}, segmentName={}, instanceId={}", offset, segmentName, instanceId);
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }
    if (extraTimeSec <= 0) {
      LOGGER.warn("Invalid value {} for extra build time from instance {} for segment {}", extraTimeSec, instanceId,
          segmentName);
      extraTimeSec = SegmentCompletionProtocol.getDefaultMaxSegmentCommitTimeSeconds();
    }

    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset)
        .withExtraTimeSec(extraTimeSec);
    LOGGER.info("Processing extendBuildTime:{}", requestParams.toString());

    SegmentCompletionProtocol.Response response = SegmentCompletionManager.getInstance().extendBuildTime(requestParams);

    final String responseStr = response.toJsonString();
    LOGGER.info("Response to extendBuildTime:{}", responseStr);
    return responseStr;
  }

  @GET
  @Path(SegmentCompletionProtocol.MSG_TYPE_CONSUMED)
  @Produces(MediaType.APPLICATION_JSON)
  public String segmentConsumed(@QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset,
      @QueryParam(SegmentCompletionProtocol.PARAM_REASON) String stopReason,
      @QueryParam(SegmentCompletionProtocol.PARAM_MEMORY_USED_BYTES) long memoryUsedBytes,
      @QueryParam(SegmentCompletionProtocol.PARAM_ROW_COUNT) int numRows) {

    if (instanceId == null || segmentName == null || offset == -1) {
      LOGGER.error("Invalid call: offset={}, segmentName={}, instanceId={}", offset, segmentName, instanceId);
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }
    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset).withReason(stopReason)
        .withMemoryUsedBytes(memoryUsedBytes).withNumRows(numRows);
    LOGGER.info("Processing segmentConsumed:{}", requestParams.toString());

    SegmentCompletionProtocol.Response response = SegmentCompletionManager.getInstance().segmentConsumed(requestParams);
    final String responseStr = response.toJsonString();
    LOGGER.info("Response to segmentConsumed:{}", responseStr);
    return responseStr;
  }

  @GET
  @Path(SegmentCompletionProtocol.MSG_TYPE_STOPPED_CONSUMING)
  @Produces(MediaType.APPLICATION_JSON)
  public String segmentStoppedConsuming(@QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset,
      @QueryParam(SegmentCompletionProtocol.PARAM_REASON) String stopReason) {

    if (instanceId == null || segmentName == null || offset == -1) {
      LOGGER.error("Invalid call: offset={}, segmentName={}, instanceId={}", offset, segmentName, instanceId);
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }
    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset).withReason(stopReason);
    LOGGER.info("Processing segmentStoppedConsuming:{}", requestParams.toString());

    SegmentCompletionProtocol.Response response =
        SegmentCompletionManager.getInstance().segmentStoppedConsuming(requestParams);
    final String responseStr = response.toJsonString();
    LOGGER.info("Response to segmentStoppedConsuming:{}", responseStr);
    return responseStr;
  }

  @GET
  @Path(SegmentCompletionProtocol.MSG_TYPE_COMMIT_START)
  @Produces(MediaType.APPLICATION_JSON)
  public String segmentCommitStart(@QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset,
      @QueryParam(SegmentCompletionProtocol.PARAM_MEMORY_USED_BYTES) long memoryUsedBytes,
      @QueryParam(SegmentCompletionProtocol.PARAM_BUILD_TIME_MILLIS) long buildTimeMillis,
      @QueryParam(SegmentCompletionProtocol.PARAM_WAIT_TIME_MILLIS) long waitTimeMillis,
      @QueryParam(SegmentCompletionProtocol.PARAM_ROW_COUNT) int numRows,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_SIZE_BYTES) long segmentSizeBytes) {
    if (instanceId == null || segmentName == null || offset == -1) {
      LOGGER.error("Invalid call: offset={}, segmentName={}, instanceId={}", offset, segmentName, instanceId);
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }

    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset)
        .withMemoryUsedBytes(memoryUsedBytes).withBuildTimeMillis(buildTimeMillis).withWaitTimeMillis(waitTimeMillis)
        .withNumRows(numRows).withSegmentSizeBytes(segmentSizeBytes);

    LOGGER.info("Processing segmentCommitStart:{}", requestParams.toString());

    SegmentCompletionProtocol.Response response =
        SegmentCompletionManager.getInstance().segmentCommitStart(requestParams);
    final String responseStr = response.toJsonString();
    LOGGER.info("Response to segmentCommitStart:{}", responseStr);
    return responseStr;
  }

  @GET
  @Path(SegmentCompletionProtocol.MSG_TYPE_COMMIT_END)
  @Produces(MediaType.APPLICATION_JSON)
  public String segmentCommitEnd(@QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_LOCATION) String segmentLocation,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset,
      @QueryParam(SegmentCompletionProtocol.PARAM_MEMORY_USED_BYTES) long memoryUsedBytes,
      @QueryParam(SegmentCompletionProtocol.PARAM_BUILD_TIME_MILLIS) long buildTimeMillis,
      @QueryParam(SegmentCompletionProtocol.PARAM_WAIT_TIME_MILLIS) long waitTimeMillis,
      @QueryParam(SegmentCompletionProtocol.PARAM_ROW_COUNT) int numRows,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_SIZE_BYTES) long segmentSizeBytes) {
    if (instanceId == null || segmentName == null || offset == -1 || segmentLocation == null) {
      LOGGER.error("Invalid call: offset={}, segmentName={}, instanceId={}, segmentLocation={}", offset, segmentName,
          instanceId, segmentLocation);
      // TODO: memoryUsedInBytes = 0 if not present in params. Add validation when we start using it
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }

    SegmentMetadataImpl segmentMetadata = extractMetadataFromSegmentFile(segmentName);
    if (segmentMetadata == null) {
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }
    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset)
        .withSegmentLocation(segmentLocation).withSegmentSizeBytes(segmentSizeBytes)
        .withBuildTimeMillis(buildTimeMillis).withWaitTimeMillis(waitTimeMillis).withNumRows(numRows)
        .withMemoryUsedBytes(memoryUsedBytes);
    LOGGER.info("Processing segmentCommitEnd:{}", requestParams.toString());

    final boolean isSuccess = true;
    final boolean isSplitCommit = true;

    CommittingSegmentDescriptor committingSegmentDescriptor =
            CommittingSegmentDescriptor.fromSegmentCompletionReqParamsAndMetadata(requestParams, segmentMetadata);
    SegmentCompletionProtocol.Response response =
        SegmentCompletionManager.getInstance().segmentCommitEnd(requestParams, isSuccess, isSplitCommit,
                committingSegmentDescriptor);
    final String responseStr = response.toJsonString();
    LOGGER.info("Response to segmentCommitEnd:{}", responseStr);
    return responseStr;
  }

  @POST
  @Path(SegmentCompletionProtocol.MSG_TYPE_COMMIT)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  public String segmentCommit(@QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset,
      @QueryParam(SegmentCompletionProtocol.PARAM_MEMORY_USED_BYTES) long memoryUsedBytes,
      @QueryParam(SegmentCompletionProtocol.PARAM_BUILD_TIME_MILLIS) long buildTimeMillis,
      @QueryParam(SegmentCompletionProtocol.PARAM_WAIT_TIME_MILLIS) long waitTimeMillis,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_SIZE_BYTES) long segmentSizeBytes,
      @QueryParam(SegmentCompletionProtocol.PARAM_ROW_COUNT) int numRows, FormDataMultiPart multiPart) {
    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset)
        .withSegmentSizeBytes(segmentSizeBytes).withBuildTimeMillis(buildTimeMillis).withWaitTimeMillis(waitTimeMillis)
        .withNumRows(numRows).withMemoryUsedBytes(memoryUsedBytes);
    LOGGER.info("Processing segmentCommit:{}", requestParams.toString());

    final SegmentCompletionManager segmentCompletionManager = SegmentCompletionManager.getInstance();
    SegmentCompletionProtocol.Response response = segmentCompletionManager.segmentCommitStart(requestParams);
    if (response.equals(SegmentCompletionProtocol.RESP_COMMIT_CONTINUE)) {
      // Get the segment and put it in the right place.
      boolean success = uploadSegment(multiPart, instanceId, segmentName, false) != null;
      SegmentMetadataImpl segmentMetadata = extractMetadataFromSegmentFile(segmentName);
      if (segmentMetadata == null) {
        return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
      }
      CommittingSegmentDescriptor committingSegmentDescriptor =
              CommittingSegmentDescriptor.fromSegmentCompletionReqParamsAndMetadata(requestParams,
                      segmentMetadata);
      response = segmentCompletionManager.segmentCommitEnd(requestParams, success, false,
              committingSegmentDescriptor);
    }

    LOGGER.info("Response to segmentCommit: instance={}  segment={} status={} offset={}", requestParams.getInstanceId(),
        requestParams.getSegmentName(), response.getStatus(), response.getOffset());

    return response.toJsonString();
  }

  // This method may be called in any controller, leader or non-leader. It is used only when the server decides to use
  // split commit protocol for the segment commit.
  @POST
  @Path(SegmentCompletionProtocol.MSG_TYPE_SEGMENT_UPLOAD)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public String segmentUpload(@QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset, FormDataMultiPart multiPart) {
    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset);
    LOGGER.info("Processing segmentUpload:{}", requestParams.toString());

    final String segmentLocation = uploadSegment(multiPart, instanceId, segmentName, true);
    if (segmentLocation == null) {
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }
    SegmentCompletionProtocol.Response.Params responseParams =
        new SegmentCompletionProtocol.Response.Params().withOffset(requestParams.getOffset())
            .withSegmentLocation(segmentLocation)
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.UPLOAD_SUCCESS);

    String response = new SegmentCompletionProtocol.Response(responseParams).toJsonString();

    LOGGER.info("Response to segmentUpload:{}", response);

    return response;
  }

  @POST
  @Path(SegmentCompletionProtocol.MSG_TYPE_COMMIT_END_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public String segmentCommitEndWithMetadata(@QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
                                             @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
                                             @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_LOCATION) String segmentLocation,
                                             @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset,
                                             @QueryParam(SegmentCompletionProtocol.PARAM_MEMORY_USED_BYTES) long memoryUsedBytes,
                                             @QueryParam(SegmentCompletionProtocol.PARAM_BUILD_TIME_MILLIS) long buildTimeMillis,
                                             @QueryParam(SegmentCompletionProtocol.PARAM_WAIT_TIME_MILLIS) long waitTimeMillis,
                                             @QueryParam(SegmentCompletionProtocol.PARAM_ROW_COUNT) int numRows,
                                             @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_SIZE_BYTES) long segmentSizeBytes,
                                             FormDataMultiPart metadataFiles) {
    if (instanceId == null || segmentName == null || offset == -1 || segmentLocation == null || metadataFiles == null) {
      LOGGER.error("Invalid call: offset={}, segmentName={}, instanceId={}, segmentLocation={}", offset, segmentName,
              instanceId, segmentLocation);
      // TODO: memoryUsedInBytes = 0 if not present in params. Add validation when we start using it
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }

    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset)
            .withSegmentLocation(segmentLocation).withSegmentSizeBytes(segmentSizeBytes)
            .withBuildTimeMillis(buildTimeMillis).withWaitTimeMillis(waitTimeMillis).withNumRows(numRows)
            .withMemoryUsedBytes(memoryUsedBytes);
    LOGGER.info("Processing segmentCommitEnd:{}", requestParams.toString());


    final boolean isSuccess = true;
    final boolean isSplitCommit = true;
    SegmentMetadataImpl segmentMetadata = extractMetadataFromInput(metadataFiles, segmentName);
    // If it fails to extract metadata from the input form, try to download the segment and extract it from the segment.
    if (segmentMetadata == null) {
      LOGGER.info("Failed to extract segment metadata for {} from input form, fallback to use the segment file.",
              segmentName);
      segmentMetadata = extractMetadataFromSegmentFile(segmentName);
    }
    // Return failure to server if both extraction efforts fail.
    if (segmentMetadata == null) {
      LOGGER.warn("Segment metadata extraction failure for segment {}", segmentName);
      return SegmentCompletionProtocol.RESP_FAILED.toJsonString();
    }
    SegmentCompletionProtocol.Response response =
            SegmentCompletionManager.getInstance().segmentCommitEnd(requestParams, isSuccess, isSplitCommit,
                    CommittingSegmentDescriptor.fromSegmentCompletionReqParamsAndMetadata(requestParams, segmentMetadata));
    final String responseStr = response.toJsonString();
    LOGGER.info("Response to segmentCommitEnd:{}", responseStr);
    return responseStr;
  }

  /**
   * Extract and return the segment metadata from the two input form data files (metadata file and creation meta).
   * Return null if any of the two files is missing or there is exception during parsing and extraction.
   *
   */
  private SegmentMetadataImpl extractMetadataFromInput(FormDataMultiPart metadataFiles, String segmentNameStr) {
    String tempMetadataDirStr = StringUtil.join("/", _controllerConf.getLocalTempDir(), segmentNameStr + METADATA_TEMP_DIR_SUFFIX);
    File tempMetadataDir = new File(tempMetadataDirStr);

    try {
      Preconditions.checkState(tempMetadataDir.mkdirs(), "Failed to create directory: %s", tempMetadataDirStr);
      // Extract metadata.properties from the metadataFiles.
      if (!extractMetadataFromInputField(metadataFiles, tempMetadataDirStr, V1Constants.MetadataKeys.METADATA_FILE_NAME)) {
        return null;
      }
      // Extract creation.meta from the metadataFiles.
      if (!extractMetadataFromInputField(metadataFiles, tempMetadataDirStr, V1Constants.SEGMENT_CREATION_META)) {
        return null;
      }
      // Load segment metadata
      return new SegmentMetadataImpl(tempMetadataDir);
    } catch (Exception e) {
      LOGGER.error("Exception extracting and reading segment metadata for " + segmentNameStr, e);
      return null;
    } finally {
      FileUtils.deleteQuietly(tempMetadataDir);
    }
  }

  /**
   *
   * Extract a single file with name metaFileName from the input FormDataMultiPart and put it under the path
   * tempMetadataDirStr + metaFileName.
   * Return true iff the extraction and copy is successful.
   */
  private boolean extractMetadataFromInputField(FormDataMultiPart metadataFiles, String tempMetadataDirStr,
                                                String metaFileName) {
    FormDataBodyPart metadataFilesField = metadataFiles.getField(metaFileName);
    Preconditions.checkNotNull(metadataFilesField, "The metadata input field %s does not exist.",
            metaFileName);

    try (InputStream metadataPropertiesInputStream = metadataFilesField.getValueAs(InputStream.class)) {
      Preconditions.checkNotNull(metadataPropertiesInputStream, "Unable to parse %s from input.",
              metaFileName);
      java.nio.file.Path metadataPropertiesPath =
              FileSystems.getDefault().getPath(tempMetadataDirStr, metaFileName);
      Files.copy(metadataPropertiesInputStream, metadataPropertiesPath);
      return true;
    } catch (IOException e) {
      LOGGER.error("Failed to copy metadata property file: {}", metaFileName, e);
    }
    return false;
  }

  /**
   * Extract the segment metadata files from the tar-zipped segment file that is expected to be in the directory for the
   * table.
   * <p>Segment tar-zipped file path: DATADIR/rawTableName/segmentName.
   * <p>We extract the metadata.properties and creation.meta into a temporary metadata directory:
   * DATADIR/rawTableName/segmentName.metadata.tmp, and load metadata from there.
   *
   * @param segmentNameStr Name of the segment
   * @return SegmentMetadataImpl if it is able to extract the metadata file from the tar-zipped segment file.
   */
  private SegmentMetadataImpl extractMetadataFromSegmentFile(final String segmentNameStr) {
    LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
    String baseDirStr = StringUtil.join("/", _controllerConf.getDataDir(), segmentName.getTableName());
    String segFileStr = StringUtil.join("/", baseDirStr, segmentNameStr);
    String tempMetadataDirStr = StringUtil.join("/", baseDirStr, segmentNameStr + METADATA_TEMP_DIR_SUFFIX);
    File tempMetadataDir = new File(tempMetadataDirStr);

    try (// Extract metadata.properties
         InputStream metadataPropertiesInputStream = TarGzCompressionUtils
                 .unTarOneFile(new FileInputStream(new File(segFileStr)), V1Constants.MetadataKeys.METADATA_FILE_NAME);
         // Extract creation.meta
         InputStream creationMetaInputStream = TarGzCompressionUtils
                 .unTarOneFile(new FileInputStream(new File(segFileStr)), V1Constants.SEGMENT_CREATION_META)
         ) {
      Preconditions.checkState(tempMetadataDir.mkdirs(), "Failed to create directory: %s", tempMetadataDirStr);
      Preconditions.checkNotNull(metadataPropertiesInputStream, "%s does not exist",
              V1Constants.MetadataKeys.METADATA_FILE_NAME);
      java.nio.file.Path metadataPropertiesPath =
              FileSystems.getDefault().getPath(tempMetadataDirStr, V1Constants.MetadataKeys.METADATA_FILE_NAME);
      Files.copy(metadataPropertiesInputStream, metadataPropertiesPath);

      Preconditions.checkNotNull(creationMetaInputStream, "%s does not exist", V1Constants.SEGMENT_CREATION_META);
      java.nio.file.Path creationMetaPath = FileSystems.getDefault().getPath(tempMetadataDirStr, V1Constants.SEGMENT_CREATION_META);
      Files.copy(creationMetaInputStream, creationMetaPath);
      // Load segment metadata
      return new SegmentMetadataImpl(tempMetadataDir);
    } catch (Exception e) {
      LOGGER.error("Exception extracting and reading segment metadata for " + segmentNameStr, e);
      return null;
    } finally {
      FileUtils.deleteQuietly(tempMetadataDir);
    }
  }

  @Nullable
  private String uploadSegment(FormDataMultiPart multiPart, String instanceId, String segmentName,
      boolean isSplitCommit) {
    try {
      Map<String, List<FormDataBodyPart>> map = multiPart.getFields();
      if (!PinotSegmentUploadRestletResource.validateMultiPart(map, segmentName)) {
        return null;
      }
      String name = map.keySet().iterator().next();
      FormDataBodyPart bodyPart = map.get(name).get(0);

      FileUploadPathProvider provider = new FileUploadPathProvider(_controllerConf);

      File localTmpFile = new File(provider.getFileUploadTmpDir(), name + "." + UUID.randomUUID().toString());
      localTmpFile.deleteOnExit();

      // Copy multipart to local
      try (InputStream inputStream = bodyPart.getValueAs(InputStream.class);
          OutputStream outputStream = new FileOutputStream(localTmpFile)) {
        IOUtils.copyLarge(inputStream, outputStream);
      }

      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      final String rawTableName = llcSegmentName.getTableName();
      final java.net.URI tableDirURI =
          ControllerConf.getUriFromPath(StringUtil.join("/", provider.getBaseDataDirURI().toString(), rawTableName));
      java.net.URI segmentFileURI;
      if (isSplitCommit) {
        String uniqueSegmentFileName = SegmentCompletionUtils.generateSegmentFileName(segmentName);
        segmentFileURI =
            ControllerConf.getUriFromPath(StringUtil.join("/", tableDirURI.toString(), uniqueSegmentFileName));
      } else {
        segmentFileURI = ControllerConf.getUriFromPath(StringUtil.join("/", tableDirURI.toString(), segmentName));
      }

      PinotFS pinotFS = PinotFSFactory.create(provider.getBaseDataDirURI().getScheme());
      try {
        if (isSplitCommit) {
          pinotFS.copyFromLocalFile(localTmpFile, segmentFileURI);
        } else {
          // Multiple threads can reach this point at the same time, if the following scenario happens
          // The server that was asked to commit did so very slowly (due to network speeds). Meanwhile the FSM in
          // SegmentCompletionManager timed out, and allowed another server to commit, which did so very quickly (somehow
          // the network speeds changed). The second server made it through the FSM and reached this point.
          // The synchronization below takes care that exactly one file gets moved in place.
          // There are still corner conditions that are not handled correctly. For example,
          // 1. What if the offset of the faster server was different?
          // 2. We know that only the faster server will get to complete the COMMIT call successfully. But it is possible
          //    that the race to this statement is won by the slower server, and so the real segment that is in there is that
          //    of the slower server.
          // In order to overcome controller restarts after the segment is renamed, but before it is committed, we DO need to
          // check for existing segment file and remove it. So, the block cannot be removed altogether.
          // For now, we live with these corner cases. Once we have split-commit enabled and working, this code will no longer
          // be used.
          synchronized (SegmentCompletionManager.getInstance()) {
            if (pinotFS.exists(segmentFileURI)) {
              LOGGER
                  .warn("Segment file {} exists. Replacing with upload from {}", segmentFileURI.toString(), instanceId);
              pinotFS.delete(segmentFileURI, true);
            }
            pinotFS.copyFromLocalFile(localTmpFile, segmentFileURI);
          }
        }
      } catch (Exception e) {
        LOGGER.error("Could not copy from {} to {}", localTmpFile.getAbsolutePath(), segmentFileURI.toString());
      } finally {
        FileUtils.deleteQuietly(localTmpFile);
      }

      LOGGER.info("Moved file {} to {}", localTmpFile.getAbsolutePath(), segmentFileURI.toString());
      return new URI(SCHEME + segmentFileURI.toString(), /* boolean escaped */ false).toString();
    } catch (InvalidControllerConfigException e) {
      LOGGER.error("Invalid controller config exception from instance {} for segment {}", instanceId, segmentName, e);
      return null;
    } catch (IOException e) {
      LOGGER.error("File upload exception from instance {} for segment {}", instanceId, segmentName, e);
      return null;
    } finally {
      multiPart.cleanup();
    }
  }
}
