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
package com.linkedin.pinot.controller.api.restlet.resources;

import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.restlet.swagger.Description;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.controller.helix.core.realtime.SegmentCompletionManager;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LLCSegmentCommitEnd extends ServerResource {
  private static Logger LOGGER = LoggerFactory.getLogger(LLCSegmentCommitEnd.class);

  @HttpVerb("get")
  @Description("Segment commit end for a LLC segment using split commit")
  @Summary("Segment commit end for a LLC segment using split commit")
  @Paths({"/" + SegmentCompletionProtocol.MSG_TYPE_COMMIT_END})
  public Representation get() {
    SegmentCompletionProtocol.Request.Params requestParams = SegmentCompletionUtils.extractParams(getReference());
    if (requestParams == null) {
      return new StringRepresentation(SegmentCompletionProtocol.RESP_FAILED.toJsonString());
    }
    LOGGER.info(requestParams.toString());

    SegmentCompletionProtocol.Response response = SegmentCompletionManager.getInstance().segmentCommitEnd(requestParams, true, true, requestParams.getSegmentLocation());

    LOGGER.info(response.toJsonString());
    return new StringRepresentation(response.getStatus().toString());
  }
}
