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

package com.linkedin.pinot.core.segment.store;

import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentDirectoryPaths {
  public static final String V3_SUBDIRECTORY_NAME = "v3";
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDirectoryPaths.class);


  public static File segmentDirectoryFor(File segmentIndexDirectory, SegmentVersion version) {
    switch(version) {
      case v1:
      case v2:
        return segmentIndexDirectory;
      case v3:
        return new File(segmentIndexDirectory, V3_SUBDIRECTORY_NAME);
      default:
        throw new UnsupportedOperationException("Segment path for version: " + version +
            " and segmentIndexDirectory: " + segmentIndexDirectory + " can not be determined ");
    }
  }
}
