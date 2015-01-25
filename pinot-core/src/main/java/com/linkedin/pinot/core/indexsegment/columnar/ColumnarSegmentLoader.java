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