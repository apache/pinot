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
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.realtime.SegmentCompletionManager;
import io.swagger.annotations.Api;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;


@Api(tags = Constants.INTERNAL_TAG)
@Path("/")
public class LLCSegmentCompletionHandlers {

  private static Logger LOGGER = LoggerFactory.getLogger(LLCSegmentCompletionHandlers.class);

  @Inject
  ControllerConf _controllerConf;

  @Inject
  PinotHelixResourceManager _helixResourceManager;

  // We don't want to document these in swagger since they are internal APIs
  @GET
  @Path(SegmentCompletionProtocol.MSG_TYPE_EXTEND_BUILD_TIME)
  @Produces(MediaType.APPLICATION_JSON)
  public String extendBuildTime(
      @QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset,
      @QueryParam(SegmentCompletionProtocol.PARAM_EXTRA_TIME_SEC) int extraTimeSec
  ) {

    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset).withExtraTimeSec(
        extraTimeSec);
    LOGGER.info(requestParams.toString());

    try {
      SegmentCompletionProtocol.Response response =
          SegmentCompletionManager.getInstance().extendBuildTime(requestParams);

      final String responseStr = response.toJsonString();
      LOGGER.info(responseStr);
      return responseStr;
    } catch (Throwable t) {
      LOGGER.error("Exception handling request", t);
      throw new WebApplicationException(t);
    }
  }

  @GET
  @Path(SegmentCompletionProtocol.MSG_TYPE_CONSUMED)
  @Produces(MediaType.APPLICATION_JSON)
  public String segmentConsumed(
      @QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset,
      @QueryParam(SegmentCompletionProtocol.PARAM_REASON) String stopReason
  ) {

    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset).withReason(stopReason);
    LOGGER.info(requestParams.toString());

    try {
      SegmentCompletionProtocol.Response response =
          SegmentCompletionManager.getInstance().segmentConsumed(requestParams);
      final String responseStr = response.toJsonString();
      LOGGER.info(responseStr);
      return responseStr;
    } catch(Throwable t) {
      LOGGER.error("Exception handling request", t);
      throw new WebApplicationException(t);
    }
  }

  @GET
  @Path(SegmentCompletionProtocol.MSG_TYPE_STOPPED_CONSUMING)
  @Produces(MediaType.APPLICATION_JSON)
  public String segmentStoppedConsuming(
      @QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset,
      @QueryParam(SegmentCompletionProtocol.PARAM_REASON) String stopReason
  ) {

    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset).withReason(stopReason);
    LOGGER.info(requestParams.toString());

    try {
      SegmentCompletionProtocol.Response response =
          SegmentCompletionManager.getInstance().segmentStoppedConsuming(requestParams);
      final String responseStr = response.toJsonString();
      LOGGER.info(responseStr);
      return responseStr;
    } catch (Throwable t) {
      LOGGER.error("Exception handling request", t);
      throw new WebApplicationException(t);
    }
  }

  @POST
  @Path(SegmentCompletionProtocol.MSG_TYPE_COMMIT)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  public String segmentCommit(
      @QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset,
      FormDataMultiPart multiPart
  )
  {
    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset);
    LOGGER.info(requestParams.toString());

    final SegmentCompletionManager segmentCompletionManager = SegmentCompletionManager.getInstance();
    SegmentCompletionProtocol.Response response = segmentCompletionManager.segmentCommitStart(requestParams, false);
    if (response.equals(SegmentCompletionProtocol.RESP_COMMIT_CONTINUE)) {
      // Get the segment and put it in the right place.
      boolean success = uploadSegment(multiPart, instanceId, segmentName);

      response = segmentCompletionManager.segmentCommitEnd(requestParams, success, false);
    }

    LOGGER.info("Response: instance={}  segment={} status={} offset={}", requestParams.getInstanceId(), requestParams.getSegmentName(),
        response.getStatus(), response.getOffset());

    return response.toJsonString();
  }


  private boolean uploadSegment(FormDataMultiPart multiPart, final String instanceId, final String segmentName) {
    FileUploadPathProvider provider = new FileUploadPathProvider(_controllerConf);

    Map<String, List<FormDataBodyPart>> map = multiPart.getFields();
    if (!validateMultiPart(map, segmentName)) {
      return false;
    }
    final String name = map.keySet().iterator().next();
    final FormDataBodyPart bodyPart = map.get(name).get(0);
    InputStream is = bodyPart.getValueAs(InputStream.class);

    File tmpFile = new File(provider.getTmpDir(), name);

    try {
      provider.saveStreamAs(is, tmpFile);
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
      final String rawTableName = llcSegmentName.getTableName();
      final File tableDir = new File(provider.getBaseDataDir(), rawTableName);
      final File segmentFile = new File(tableDir, segmentName);

      synchronized (_helixResourceManager) {
        if (segmentFile.exists()) {
          LOGGER.warn("Segment file {} exists. Replacing with upload from {}", segmentName, instanceId);
          FileUtils.deleteQuietly(segmentFile);
        }
        FileUtils.moveFile(tmpFile, segmentFile);
      }
    } catch (IOException e) {
      LOGGER.error("File upload exception from instance {} for segment {}", instanceId, segmentName, e);
      return false;
    }
    return true;
  }

  // Validate that there is one file that is in the input.
  private boolean validateMultiPart(Map<String, List<FormDataBodyPart>> map, String segmentName) {
    if (map.size() != 1) {
      LOGGER.error("Incorrect number of multi-part elements: {} for segment {}", map.size(), segmentName);
      return false;
    }
    List<FormDataBodyPart> bodyParts = map.get(map.keySet().iterator().next());
    if (bodyParts.size() != 1) {
      LOGGER.error("Incorrect number of elements in list in first part: {} for segment {}", bodyParts.size(), segmentName);
      return false;
    }
    return true;
  }
}
