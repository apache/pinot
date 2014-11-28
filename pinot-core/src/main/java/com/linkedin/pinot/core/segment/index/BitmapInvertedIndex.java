package com.linkedin.pinot.core.segment.index;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.apache.log4j.Logger;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * @author Dhaval Patel<dpatel@linkedin.com
 * Aug 10, 2014
 */

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
  public BitmapInvertedIndex(File file, int cardinality, boolean isMmap) throws IOException {
    numberOfBitmaps = cardinality;
    bitmaps = new ImmutableRoaringBitmap[numberOfBitmaps];
    logger.info("start to load bitmap inverted index for column: " + cardinality + " where isMmap is " + isMmap);
    load(file, isMmap);
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

  private void loadMmap(File file) throws IOException {
    final int[] offsets = new int[numberOfBitmaps + 1];
    final DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
    for (int i = 0; i <= numberOfBitmaps; ++i) {
      offsets[i] = dis.readInt();
    }
    dis.close();

    final int lastOffset = offsets[numberOfBitmaps];

    @SuppressWarnings("resource")
    final RandomAccessFile rndFile = new RandomAccessFile(file, "r");
    final MappedByteBuffer mbb = rndFile.getChannel().map(MapMode.READ_ONLY, 0, lastOffset);

    for (int i = 0; i < numberOfBitmaps; i++) {
      mbb.position(offsets[i]);
      final ByteBuffer bb = mbb.slice();
      final long offsetLimit = i < 199 ? offsets[i + 1] : lastOffset;
      bb.limit((int) (offsetLimit - offsets[i]));
      bitmaps[i] = new ImmutableRoaringBitmap(bb);
    }
  }

  private void load(File file, boolean isMmap) throws IOException {
    if (isMmap == true) {
      loadMmap(file);
      return;
    }

    final int[] offsets = new int[numberOfBitmaps + 1];
    final DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
    for (int i = 0; i <= numberOfBitmaps; ++i) {
      offsets[i] = dis.readInt();
    }
    dis.close();
    @SuppressWarnings("resource")
    final FileChannel channel = new RandomAccessFile(file, "r").getChannel();
    for (int k = 0; k < numberOfBitmaps; ++k) {
      bitmaps[k] = new ImmutableRoaringBitmap(channel.map(MapMode.READ_ONLY, offsets[k], offsets[k + 1] - offsets[k]));
      if (!isMmap) {
        bitmaps[k] = bitmaps[k].toMutableRoaringBitmap();
      }
    }
  }
}
