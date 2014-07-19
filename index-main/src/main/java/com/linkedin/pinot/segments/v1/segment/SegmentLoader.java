package com.linkedin.pinot.segments.v1.segment;

import java.io.File;
import java.io.IOException;

import org.apache.commons.configuration.ConfigurationException;

import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.segments.v1.segment.utils.IntArray;


public class SegmentLoader {

  public enum IO_MODE {
    heap,
    mmap;
  }

  public static IndexSegment load(File indexDir, IO_MODE mode) throws ConfigurationException, IOException {
    switch (mode) {
      case heap:
        return loadHeap(indexDir);
      case mmap:
        return loadMmap(indexDir);
    }
    return null;
  }

  public static IndexSegment loadMmap(File indexDir) throws ConfigurationException, IOException {
    return new ColumnarSegment(indexDir, IO_MODE.mmap);
  }

  public static IndexSegment loadHeap(File indexDir) throws ConfigurationException, IOException {
    return new ColumnarSegment(indexDir, IO_MODE.heap);
  }
}
