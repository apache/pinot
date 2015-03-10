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
package com.linkedin.pinot.core.indexsegment.columnar;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.loader.Loaders;


/**
 *
 * @author Dhaval Patel<dpatel@linkedin.com
 * July 19, 2014
 */
public class ColumnarSegmentLoader {
  public static IndexSegment load(File indexDir, ReadMode mode) throws Exception {
    switch (mode) {
      case heap:
        return loadHeap(indexDir);
      case mmap:
        return loadMmap(indexDir);
    }
    return null;
  }

  public static IndexSegment loadMmap(File indexDir) throws Exception {
    return Loaders.IndexSegment.load(indexDir, ReadMode.mmap);
  }

  public static IndexSegment loadHeap(File indexDir) throws Exception {
    return Loaders.IndexSegment.load(indexDir, ReadMode.heap);
  }

  public IndexSegment loadSegment(SegmentMetadata segmentMetadata) throws Exception {
    return Loaders.IndexSegment.load(new File(segmentMetadata.getIndexDir()), ReadMode.heap);
  }

  public static IndexSegment loadSegment(SegmentMetadata segmentMetadata, ReadMode _readMode) throws Exception {
    return Loaders.IndexSegment.load(new File(segmentMetadata.getIndexDir()), _readMode);
  }
}
