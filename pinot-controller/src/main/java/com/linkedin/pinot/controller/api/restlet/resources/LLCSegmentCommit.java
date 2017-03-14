/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.io.FileUtils;
import org.restlet.ext.fileupload.RestletFileUpload;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.restlet.swagger.Description;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.controller.helix.core.realtime.SegmentCompletionManager;


/**
 * A resource class that fields the segmentCommit() message (and the upload of a realtime segment)
 * from the servers. Servers go through {@link SegmentCompletionProtocol} to complete a segment and
 * one of the servers commit the segment that has been completed.
 */
public class LLCSegmentCommit extends PinotSegmentUploadRestletResource {
  private static Logger LOGGER = LoggerFactory.getLogger(LLCSegmentCommit.class);
  long _offset;
  String _segmentNameStr;
  String _instanceId;

  public LLCSegmentCommit() throws IOException {
  }

  @Override
  @HttpVerb("post")
  @Description("Uploads an LLC segment coming in from a server")
  @Summary("Uploads an LLC segment coming in from a server")
  @Paths({"/" + SegmentCompletionProtocol.MSG_TYPE_COMMMIT})
  public Representation post(Representation entity) {
    if (!extractParams()) {
      return new StringRepresentation(SegmentCompletionProtocol.RESP_FAILED.toJsonString());
    }
    LOGGER.info("segment={} offset={} instance={} ", _segmentNameStr, _offset, _instanceId);
    final SegmentCompletionManager segmentCompletionManager = getSegmentCompletionManager();

    final SegmentCompletionProtocol.Request.Params reqParams = new SegmentCompletionProtocol.Request.Params();
    reqParams.withInstanceId(_instanceId).withSegmentName(_segmentNameStr).withOffset(_offset);
    SegmentCompletionProtocol.Response response = segmentCompletionManager.segmentCommitStart(reqParams);
    if (response.equals(SegmentCompletionProtocol.RESP_COMMIT_CONTINUE)) {

      // Get the segment and put it in the right place.
      boolean success = uploadSegment(_instanceId, _segmentNameStr);

      response = segmentCompletionManager.segmentCommitEnd(reqParams, success);
    }

    LOGGER.info("Response: instance={}  segment={} status={} offset={}", _instanceId, _segmentNameStr, response.getStatus(), response.getOffset());
    return new StringRepresentation(response.toJsonString());
  }

  boolean extractParams() {
    final String offsetStr = getReference().getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_OFFSET);
    final String segmentName = getReference().getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_SEGMENT_NAME);
    final String instanceId = getReference().getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_INSTANCE_ID);

    if (offsetStr == null || segmentName == null || instanceId == null) {
      LOGGER.error("Invalid call: offset={}, segmentName={}, instanceId={}", offsetStr, segmentName, instanceId);
      return false;
    }
    _segmentNameStr = segmentName;
    _instanceId = instanceId;
    try {
      _offset = Long.valueOf(offsetStr);
    } catch (NumberFormatException e) {
      LOGGER.error("Invalid offset {} for segment {} from instance {}", offsetStr, segmentName, instanceId);
      return false;
    }
    return true;
  }

  boolean uploadSegment(final String instanceId, final String segmentNameStr) {
    // 1/ Create a factory for disk-based file items
    final DiskFileItemFactory factory = new DiskFileItemFactory();

    // 2/ Create a new file upload handler based on the Restlet
    // FileUpload extension that will parse Restlet requests and
    // generates FileItems.
    final RestletFileUpload upload = new RestletFileUpload(factory);
    final List<FileItem> items;

    try {
      // The following statement blocks until the entire segment is read into memory.
      items = upload.parseRequest(getRequest());

      boolean found = false;
      File dataFile = null;

      for (final Iterator<FileItem> it = items.iterator(); it.hasNext() && !found; ) {
        final FileItem fi = it.next();
        if (fi.getFieldName() != null && fi.getFieldName().equals(segmentNameStr)) {
          found = true;
          dataFile = new File(tempDir, segmentNameStr);
          fi.write(dataFile);
        }
      }

      if (!found) {
        LOGGER.error("Segment not included in request. Instance {}, segment {}", instanceId, segmentNameStr);
        return false;
      }
      // We will not check for quota here. Instead, committed segments will count towards the quota of a
      // table
      LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
      final String rawTableName = segmentName.getTableName();
      final File tableDir = new File(baseDataDir, rawTableName);
      final File segmentFile = new File(tableDir, segmentNameStr);

      synchronized (_pinotHelixResourceManager) {
        if (segmentFile.exists()) {
          LOGGER.warn("Segment file {} exists. Replacing with upload from {}", segmentNameStr, instanceId);
          FileUtils.deleteQuietly(segmentFile);
        }
        FileUtils.moveFile(dataFile, segmentFile);
      }

      return true;
    } catch (Exception e) {
      LOGGER.error("File upload exception from instance {} for segment {}", instanceId, segmentNameStr, e);
    }
    return false;
  }
}
