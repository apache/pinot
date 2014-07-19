package com.linkedin.pinot.segments.v1.segment;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.apache.log4j.Logger;

import com.linkedin.pinot.segments.v1.segment.SegmentLoader.IO_MODE;
import com.linkedin.pinot.segments.v1.segment.utils.BitUtils;
import com.linkedin.pinot.segments.v1.segment.utils.HeapCompressedIntArray;
import com.linkedin.pinot.segments.v1.segment.utils.IntArray;
import com.linkedin.pinot.segments.v1.segment.utils.OffHeapCompressedIntArray;


public class IntArrayLoader {
  private static Logger logger = Logger.getLogger(IntArrayLoader.class);

  public static IntArray load(IO_MODE mode, File indexFile, ColumnMetadata metadata) throws IOException {
    switch (mode) {
      case mmap:
        return LoadMmap(indexFile, metadata);
      default:
        return loadHeap(indexFile, metadata);
    }
  }

  public static IntArray LoadMmap(File indexFile, ColumnMetadata metadata) throws IOException {
    RandomAccessFile randomAccessIdxFile = new RandomAccessFile(indexFile, "r");
    ByteBuffer byteBuffer =
        randomAccessIdxFile.getChannel().map(MapMode.READ_ONLY, 0, metadata.getBitsPerElementInForwardIndex());

    return new OffHeapCompressedIntArray(metadata.getTotalDocs(), OffHeapCompressedIntArray.getNumOfBits(metadata
        .getDictionarySize()), byteBuffer);
  }

  public static IntArray loadHeap(File indexFile, ColumnMetadata metadata) throws IOException {
    if (metadata.isSingleValued() && !metadata.isSorted()) {
      logger.info("found an unsorted single valued column, will use Heap Compressed IntArray To Load it");
      RandomAccessFile randomAccessIdxFile = new RandomAccessFile(indexFile, "r");
      int byteSize =
          OffHeapCompressedIntArray.getRequiredBufferSize(metadata.getTotalDocs(),
              OffHeapCompressedIntArray.getNumOfBits(metadata.getDictionarySize()));
      ByteBuffer byteBuffer =
          randomAccessIdxFile.getChannel().map(MapMode.READ_ONLY, 0, byteSize);

      HeapCompressedIntArray heapCompressedIntArray =
          new HeapCompressedIntArray(metadata.getTotalDocs(), OffHeapCompressedIntArray.getNumOfBits(metadata
              .getDictionarySize()));

      for (int i = 0; i < heapCompressedIntArray.getBlocks().length; i++) {
        heapCompressedIntArray.getBlocks()[i] = BitUtils.getLong(byteBuffer, i);
      }
      
      randomAccessIdxFile.close();
      return heapCompressedIntArray;
    }

    // no other case for now
    return null;
  }
}
