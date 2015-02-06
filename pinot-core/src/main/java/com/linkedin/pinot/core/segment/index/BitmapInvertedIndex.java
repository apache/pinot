package com.linkedin.pinot.core.segment.index;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
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

  private RandomAccessFile _rndFile;

  /**
   * Constructs an inverted index with the specified size.
   * @param size the number of bitmaps in the inverted index, which should be the same as the number of values in
   * the dictionary.
   * @throws IOException
   */
  public BitmapInvertedIndex(File file, int cardinality, boolean isMmap) throws IOException {
    numberOfBitmaps = cardinality;
    bitmaps = new ImmutableRoaringBitmap[numberOfBitmaps];
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

  private void load(File file, boolean isMmap) throws IOException {

    final int[] offsets = new int[numberOfBitmaps + 1];
    final DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
    for (int i = 0; i <= numberOfBitmaps; ++i) {
      offsets[i] = dis.readInt();
    }
    dis.close();

    ByteBuffer buffer;
    final int lastOffset = offsets[numberOfBitmaps];

    _rndFile = new RandomAccessFile(file, "r");
    if (isMmap) {
      buffer = _rndFile.getChannel().map(MapMode.READ_ONLY, 0, lastOffset);
    } else {
      buffer = ByteBuffer.allocate(lastOffset);
      _rndFile.getChannel().read(buffer);
    }

    for (int i = 0; i < numberOfBitmaps; i++) {
      buffer.position(offsets[i]);
      final ByteBuffer bb = buffer.slice();
      final long offsetLimit = i < 199 ? offsets[i + 1] : lastOffset;
      bb.limit((int) (offsetLimit - offsets[i]));
      bitmaps[i] = new ImmutableRoaringBitmap(bb);
    }
  }

  public void close() throws IOException {
    for (int i = 0; i < bitmaps.length; ++i) {
      bitmaps[i] = null;
    }
    if (_rndFile != null) {
      _rndFile.close();
    }
  }
}
