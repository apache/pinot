package com.linkedin.pinot.core.indexsegment.columnar;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.common.segment.ReadMode;


/**
 * @author Dhaval Patel<dpatel@linkedin.com
 * Aug 10, 2014
 */
public class BitmapInvertedIndexLoader {
  public static BitmapInvertedIndex load(File file, ReadMode mode, ColumnMetadata metadata) throws IOException {
    switch (mode) {
      case heap:
        return loadHeap(file, metadata);
      case mmap:
      default:
        return loadMmap(file, metadata);
    }
  }

  private static BitmapInvertedIndex loadMmap(File file, ColumnMetadata metadata) throws IOException {
    return null;
  }

  private static BitmapInvertedIndex loadHeap(File file, ColumnMetadata metadata) throws IOException {
    return null;
  }
}
