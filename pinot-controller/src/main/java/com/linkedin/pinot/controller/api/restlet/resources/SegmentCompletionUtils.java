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
import java.io.File;
import java.io.FileFilter;
import java.util.UUID;
import java.util.regex.Pattern;
import org.restlet.data.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentCompletionUtils {
  private static Logger LOGGER = LoggerFactory.getLogger(SegmentCompletionUtils.class);

  static SegmentCompletionProtocol.Request.Params extractParams(Reference reference) {
    final String offsetStr = reference.getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_OFFSET);
    final String segmentName = reference.getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_SEGMENT_NAME);
    final String instanceId = reference.getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_INSTANCE_ID);
    final String segmentLocation = reference.getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_SEGMENT_LOCATION);

    if (offsetStr == null || segmentName == null || instanceId == null) {
      LOGGER.error("Invalid call: offset={}, segmentName={}, instanceId={}", offsetStr, segmentName, instanceId);
      return null;
    }
    long offset;
    try {
      offset = Long.valueOf(offsetStr);
    } catch (NumberFormatException e) {
      LOGGER.error("Invalid offset {} for segment {} from instance {}", offsetStr, segmentName, instanceId);
      return null;
    }
    return new SegmentCompletionProtocol.Request.Params()
        .withInstanceId(instanceId)
        .withOffset(offset)
        .withSegmentName(segmentName)
        .withSegmentLocation(segmentLocation);
  }

  public static String generateSegmentNamePrefix(String segmentName) {
    return segmentName + ".tmp.";
  }

  public static String generateSegmentFileName(String segmentNameStr) {
    return generateSegmentNamePrefix(segmentNameStr) + SegmentCompletionUtils.generateUUID();
  }

  public static String generateUUID() {
    return UUID.randomUUID().toString();
  }

  public static File[] listFilesMatching(File root, String segmentName) {
    if(!root.isDirectory()) {
      throw new IllegalArgumentException(root+" is no directory.");
    }
    final Pattern p = Pattern.compile(segmentName + "*");
    return root.listFiles(new FileFilter(){
      @Override
      public boolean accept(File file) {
        return p.matcher(file.getName()).matches();
      }
    });
  }
}
