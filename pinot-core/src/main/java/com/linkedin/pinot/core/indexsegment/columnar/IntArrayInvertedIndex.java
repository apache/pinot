package com.linkedin.pinot.core.indexsegment.columnar;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.differential.IntegratedBinaryPacking;
import me.lemire.integercompression.differential.IntegratedComposition;
import me.lemire.integercompression.differential.IntegratedIntegerCODEC;
import me.lemire.integercompression.differential.IntegratedVariableByte;

import org.apache.log4j.Logger;

import com.linkedin.pinot.common.segment.ReadMode;


/**
 * 
 * @author Dhaval Patel<dpatel@linkedin.com
 * Aug 19, 2014
 */
public class IntArrayInvertedIndex {
  public static final Logger logger = Logger.getLogger(IntArrayInvertedIndex.class);

  final int numberOfLists;
  final IntBuffer[] p4dLists;
  final IntegratedIntegerCODEC codec;
  IntBuffer dataSizes;

  public IntArrayInvertedIndex(File file, ReadMode mode, ColumnMetadata metadata) throws IOException {
    numberOfLists = metadata.getDictionarySize();
    p4dLists = new IntBuffer[numberOfLists];
    codec = new IntegratedComposition(new IntegratedBinaryPacking(), new IntegratedVariableByte());
    logger.info("start to load CompressedIntArray inverted index for column: " + metadata.getName() + " in " + mode
        + "mode.");
    load(file, mode);
  }

  private void load(File file, ReadMode mode) throws IOException {
    DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
    // load offset and number of integers of each posting list
    int[] offsets = new int[numberOfLists + 1];
    for (int i = 0; i <= numberOfLists; ++i) {
      offsets[i] = in.readInt();
    }
    // load compressed posting list
    loadMmap(offsets, file);
    if (mode == ReadMode.heap) {
      loadHeap();
    }
    in.close();
  }

  /**
   * Returns the uncompressed integer array at the specified index.
   * NOTE: This is not a thread-safe method.
   * @param idx the index of the integer array.
   * @return the uncompressed integer array at the specified index.
   */
  public int[] get(int idx) {
    if (p4dLists[idx].hasArray()) {
      return decompressList(p4dLists[idx].array(), dataSizes.get(idx));
    } else {
      final int[] buffer = new int[p4dLists[idx].limit()];
      p4dLists[idx].rewind();
      p4dLists[idx].get(buffer);
      return decompressList(buffer, dataSizes.get(idx));
    }
  }

  private void loadMmap(int[] offsets, File file) throws IOException {
    @SuppressWarnings("resource")
    final FileChannel channel = new RandomAccessFile(file, "r").getChannel();
    dataSizes = channel.map(MapMode.READ_ONLY, 4 * offsets.length, 4 * (offsets.length - 1)).asIntBuffer();
    for (int i = 0; i < numberOfLists; ++i) {
      p4dLists[i] = channel.map(MapMode.READ_ONLY, offsets[i], offsets[i + 1] - offsets[i]).asIntBuffer();
    }
  }

  /**
   * Brings all memory mapped data into heap. Invoke loadMmap before invoking this method.
   */
  private void loadHeap() {
    {
      final IntBuffer heapBuffer = IntBuffer.allocate(dataSizes.limit());
      final int[] backingArray = heapBuffer.array();
      dataSizes.rewind();
      dataSizes.get(backingArray);
      dataSizes = heapBuffer;
    }
    for (int i = 0; i < numberOfLists; ++i) {
      final IntBuffer heapBuffer = IntBuffer.allocate(p4dLists[i].limit());
      final int[] backingArray = heapBuffer.array();
      p4dLists[i].rewind();
      p4dLists[i].get(backingArray);
      p4dLists[i] = heapBuffer;
    }
  }

  private int[] decompressList(int[] compressed, int dataSize) {
    final int[] recovered = new int[dataSize];
    final IntWrapper compOffset = new IntWrapper(0);
    final IntWrapper recOffset = new IntWrapper(0);
    codec.uncompress(compressed, compOffset, compressed.length - compOffset.get(), recovered, recOffset);
    return recovered;
  }
}
