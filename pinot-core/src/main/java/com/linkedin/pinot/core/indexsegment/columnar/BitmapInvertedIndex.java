package com.linkedin.pinot.core.indexsegment.columnar;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.apache.log4j.Logger;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.core.indexsegment.columnar.SegmentLoader.IO_MODE;

public class BitmapInvertedIndex {
  public static final Logger logger = Logger.getLogger(BitmapInvertedIndex.class);

  final private int numberOfBitmaps;
  final private ImmutableRoaringBitmap[] bitmaps;

  /**
   * Constructs an inverted index with the specified size.
   * @param size the number of bitmaps in the inverted index, which should be the same as the number of values in
   * the dictionary.
   * @throws IOException
   */
  public BitmapInvertedIndex(File file, IO_MODE mode, ColumnMetadata metadata) throws IOException {
    numberOfBitmaps = metadata.getDictionarySize();
    bitmaps = new ImmutableRoaringBitmap[numberOfBitmaps];
    logger.info("start to load bitmap inverted index for column: " + metadata.getName() + " in " + mode + "mode.");
    load(file, mode);
  }

  /**
   * Returns the immutable bitmap at the specified index.
   * @param idx the index
   * @return the immutable bitmap at the specified index.
   */
  public ImmutableRoaringBitmap getImmutable(int idx) {
    return bitmaps[idx];
  }

  /**
   * Copies and returns the content of the specified immutable bitmap to a bitmap that can be modified.
   * @param idx the index
   * @return the copy of the specified bitmap.
   */
  public MutableRoaringBitmap getMutable(int idx) {
    return bitmaps[idx].toMutableRoaringBitmap();
  }

  private void load(File file, IO_MODE mode) throws IOException {
    // read offsets
    int[] offsets = new int[numberOfBitmaps+1];
    DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
    for (int i = 0; i <= numberOfBitmaps; ++i) {
      offsets[i] = dis.readInt();
    }
    dis.close();
    // read in bitmaps
    @SuppressWarnings("resource")
    final FileChannel channel = new RandomAccessFile(file, "r").getChannel();
    for (int k = 0; k < numberOfBitmaps; ++k) {
      bitmaps[k] = new ImmutableRoaringBitmap(channel.map(MapMode.READ_ONLY,
          offsets[k], offsets[k+1] - offsets[k]));
      if (mode == IO_MODE.heap) {
        bitmaps[k] = bitmaps[k].toMutableRoaringBitmap();
      }
    }
  }
}
