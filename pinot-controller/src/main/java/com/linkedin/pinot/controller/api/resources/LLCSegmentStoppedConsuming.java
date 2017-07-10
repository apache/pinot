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

import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.restlet.swagger.Description;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.controller.helix.core.realtime.SegmentCompletionManager;
import java.io.IOException;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LLCSegmentStoppedConsuming extends PinotSegmentUploadRestletResource {
  private static Logger LOGGER = LoggerFactory.getLogger(
      LLCSegmentStoppedConsuming.class);

  public LLCSegmentStoppedConsuming() throws IOException {
  }

  @Override
  @HttpVerb("get")
  @Description("Receives indication from server that it has stopped consuming")
  @Summary("Receives indication from server that it has stopped consuming")
  @Paths({"/" + SegmentCompletionProtocol.MSG_TYPE_STOPPED_CONSUMING})
  public Representation get() {
    SegmentCompletionProtocol.Request.Params requestParams = SegmentCompletionUtils.extractParams(getReference());
    if (requestParams == null) {
      return new StringRepresentation(SegmentCompletionProtocol.RESP_FAILED.toJsonString());
    }
    LOGGER.info(requestParams.toString());
    SegmentCompletionProtocol.Response response = SegmentCompletionManager.getInstance().segmentStoppedConsuming(requestParams);
    LOGGER.info(response.toJsonString());
    return new StringRepresentation(response.toJsonString());
  }

}
