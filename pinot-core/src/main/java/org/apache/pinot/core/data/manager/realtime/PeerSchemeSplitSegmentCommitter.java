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
package org.apache.pinot.core.data.manager.realtime;

import java.io.File;
import java.net.URI;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;


public class PeerSchemeSplitSegmentCommitter extends SplitSegmentCommitter {
  public PeerSchemeSplitSegmentCommitter(Logger segmentLogger, ServerSegmentCompletionProtocolHandler protocolHandler,
      SegmentCompletionProtocol.Request.Params params, SegmentUploader segmentUploader) {
    super(segmentLogger, protocolHandler, params, segmentUploader);
  }

  // Always return a uri string even if the segment upload fails and returns a null uri.
  // If the segment upload fails, put peer:///segment_name in the segment location to notify the controller it is a
  // peer download scheme.
  protected String uploadSegment(File segmentTarFile, SegmentUploader segmentUploader,
      SegmentCompletionProtocol.Request.Params params) {
    URI segmentLocation = segmentUploader.uploadSegment(segmentTarFile, new LLCSegmentName(params.getSegmentName()));
    if (segmentLocation == null) {
      return StringUtil.join("/", CommonConstants.Segment.PEER_SEGMENT_DOWNLOAD_SCHEME, params.getSegmentName());
    }
    return segmentLocation.toString();
  }
}
