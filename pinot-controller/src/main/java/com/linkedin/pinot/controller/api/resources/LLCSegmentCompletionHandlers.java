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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.controller.helix.core.realtime.SegmentCompletionManager;
import io.swagger.annotations.Api;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Api(tags = Constants.INTERNAL_TAG)
@Path("/")
public class LLCSegmentCompletionHandlers {

  private static Logger LOGGER = LoggerFactory.getLogger(LLCSegmentCompletionHandlers.class);

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

    SegmentCompletionProtocol.Response response = SegmentCompletionManager.getInstance().extendBuildTime(requestParams);

    final String responseStr = response.toJsonString();
    LOGGER.info(responseStr);
    return responseStr;
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

    SegmentCompletionProtocol.Response response = SegmentCompletionManager.getInstance().segmentConsumed(requestParams);

    final String responseStr = response.toJsonString();
    LOGGER.info(responseStr);
    return responseStr;
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

    SegmentCompletionProtocol.Response response = SegmentCompletionManager.getInstance().segmentStoppedConsuming(
        requestParams);

    final String responseStr = response.toJsonString();
    LOGGER.info(responseStr);
    return responseStr;
  }

  /*
  @POST
  @Path(SegmentCompletionProtocol.MSG_TYPE_STOPPED_CONSUMING)
  @Produces(MediaType.APPLICATION_JSON)
  public String segmentCommit(
      @QueryParam(SegmentCompletionProtocol.PARAM_INSTANCE_ID) String instanceId,
      @QueryParam(SegmentCompletionProtocol.PARAM_SEGMENT_NAME) String segmentName,
      @QueryParam(SegmentCompletionProtocol.PARAM_OFFSET) long offset
  ) {

    SegmentCompletionProtocol.Request.Params requestParams = new SegmentCompletionProtocol.Request.Params();
    requestParams.withInstanceId(instanceId).withSegmentName(segmentName).withOffset(offset);
    LOGGER.info(requestParams.toString());

    SegmentCompletionProtocol.Response response = SegmentCompletionManager.getInstance().segmentStoppedConsuming(
        requestParams);

    final String responseStr = response.toJsonString();
    LOGGER.info(responseStr);
    return responseStr;
  }
  */
}
