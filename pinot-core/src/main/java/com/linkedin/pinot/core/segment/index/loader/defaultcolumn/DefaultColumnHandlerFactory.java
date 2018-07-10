/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.index.loader.defaultcolumn;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import java.io.File;


public class DefaultColumnHandlerFactory {
  private DefaultColumnHandlerFactory() {
  }

  public static DefaultColumnHandler getDefaultColumnHandler(File indexDir, Schema schema,
      SegmentMetadataImpl segmentMetadata, SegmentDirectory.Writer segmentWriter) {
    if (SegmentVersion.valueOf(segmentMetadata.getVersion()) == SegmentVersion.v3) {
      return new V3DefaultColumnHandler(indexDir, schema, segmentMetadata, segmentWriter);
    } else {
      return new V1DefaultColumnHandler(indexDir, schema, segmentMetadata);
    }
  }
}
