package com.linkedin.pinot.core.indexsegment.columnar;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.common.segment.ReadMode;
/**
 * 
 * @author Dhaval Patel<dpatel@linkedin.com
 * Aug 19, 2014
 */
public class IntArrayInvertedIndexLoader {

  public static IntArrayInvertedIndex load(File file, ReadMode mode, ColumnMetadata metadata) throws IOException {
    switch (mode) {
      case heap:
        return loadHeap(file, metadata);
      case mmap:
      default:
        return loadMmap(file, metadata);
    }
  }

  private static IntArrayInvertedIndex loadMmap(File file, ColumnMetadata metadata) throws IOException {
    return new IntArrayInvertedIndex(file, ReadMode.mmap, metadata);
  }

  private static IntArrayInvertedIndex loadHeap(File file, ColumnMetadata metadata) throws IOException {
    return new IntArrayInvertedIndex(file, ReadMode.heap, metadata);
  }

}
