package com.linkedin.pinot.core.indexsegment.columnar;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.core.indexsegment.columnar.SegmentLoader.IO_MODE;

public class IntArrayInvertedIndexLoader {

  public static IntArrayInvertedIndex load(File file, IO_MODE mode, ColumnMetadata metadata) throws IOException {
    switch (mode) {
      case heap:
        return loadHeap(file, metadata);
      case mmap:
      default:
        return loadMmap(file, metadata);
    }
  }

  private static IntArrayInvertedIndex loadMmap(File file, ColumnMetadata metadata) throws IOException {
    return new IntArrayInvertedIndex(file, IO_MODE.mmap, metadata);
  }

  private static IntArrayInvertedIndex loadHeap(File file, ColumnMetadata metadata) throws IOException {
    return new IntArrayInvertedIndex(file, IO_MODE.heap, metadata);
  }

}
