package com.linkedin.pinot;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;

import org.apache.commons.configuration.ConfigurationException;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 27, 2014
 */

/*
 * variant:[180, 8384, 15442, 40038, 43040, 47584, 49672, 52498, 55398, 60470, 64468, 80868, 83722, 86906, 97060, 105264, 113468, 114556, 116438, 120064, 128268, 136472, 145238, 149886, 156762, 164966, 173170, 175682, 176052, 176182, 176592, 177862, 179226, 180774, 182646, 182846, 183346, 183606, 183728, 188676, 196880, 205084, 213288, 222044, 230248]
    230248
    230248
 * */
public class TestBitmapTwo {

  public static void mmap(String column, File file, ColumnMetadata m) throws IOException {

    if (!column.equals("variant")) {
      return;
    }

    final int cardinality = m.getCardinality();
    final int[] offsets = new int[cardinality + 1];
    final DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
    for (int i = 0; i <= cardinality; ++i) {
      offsets[i] = dis.readInt();
    }
    dis.close();

    final int lastOffset = offsets[cardinality];

    System.out.println(column + " : " + cardinality + " : " + file.length() + " : " + lastOffset);
    System.out.println(column + ":" + Arrays.toString(offsets));

    System.out.println("************ ");

    final ImmutableRoaringBitmap[] invertedIndexes = new ImmutableRoaringBitmap[cardinality];

    final RandomAccessFile rndFile = new RandomAccessFile(file, "r");
    final MappedByteBuffer buf = rndFile.getChannel().map(MapMode.READ_ONLY, 0, lastOffset);
    for (int i = 0; i < cardinality; i++) {
      buf.position(offsets[i]);
      final ByteBuffer bb = buf.slice();
      final long offsetLimit = i < 199 ? offsets[i+1] : lastOffset;
      bb.limit((int) (offsetLimit-offsets[i]));
      final ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(bb);
      invertedIndexes[i] = irb;
    }

    for (int i = 0; i < invertedIndexes.length; i++) {
      System.out.println(i + " : " + Arrays.toString(invertedIndexes[i].toArray()));
    }
  }

  public static void heap(String column, File file, ColumnMetadata m) throws IOException {
    if (!column.equals("variant")) {
      return;
    }

    final int cardinality = m.getCardinality();
    final int[] offsets = new int[cardinality + 1];
    final DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
    for (int i = 0; i <= cardinality; ++i) {
      offsets[i] = dis.readInt();
    }
    dis.close();

    final int lastOffset = offsets[cardinality];

    System.out.println(column + " : " + cardinality + " : " + file.length() + " : " + lastOffset);
    System.out.println(column + ":" + Arrays.toString(offsets));

    System.out.println("************ ");

    final ImmutableRoaringBitmap[] invertedIndexes = new ImmutableRoaringBitmap[cardinality];

    final RandomAccessFile rndFile = new RandomAccessFile(file, "r");
    final ByteBuffer buf = ByteBuffer.allocate(lastOffset);
    rndFile.getChannel().read(buf);
    for (int i = 0; i < cardinality; i++) {
      buf.position(offsets[i]);
      final ByteBuffer bb = buf.slice();
      final long offsetLimit = i < 199 ? offsets[i+1] : lastOffset;
      bb.limit((int) (offsetLimit-offsets[i]));
      final ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(bb);
      invertedIndexes[i] = irb;
    }

    for (int i = 0; i < invertedIndexes.length; i++) {
      System.out.println(i + " : " + Arrays.toString(invertedIndexes[i].toArray()));
    }
  }

  public static void main(String[] args) throws ConfigurationException, IOException {
    final File indexDir = new File("/export/content/data/pinot/dataDir/xlntBeta/xlntBeta_product_email_3");

    final SegmentMetadataImpl metadata = new SegmentMetadataImpl(new File(indexDir, "metadata.properties"));

    for (final String column : metadata.getAllColumns()) {

      final File invFile = new File(indexDir, column + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
      mmap(column, invFile, metadata.getColumnMetadataFor(column));
    }
  }
}
