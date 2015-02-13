package com.linkedin.pinot.core.indexsegment.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;


/**
 * Tests for SearchableByteBufferUtil
 *
 * @author jfim
 */
public class SearchableByteBufferUtilTest {
  private static final int DISTINCT_VALUES = 10000;
  private short[] _shorts = new short[DISTINCT_VALUES];
  private int[] _ints = new int[DISTINCT_VALUES];
  private long[] _longs = new long[DISTINCT_VALUES];
  private float[] _floats = new float[DISTINCT_VALUES];
  private double[] _doubles = new double[DISTINCT_VALUES];
  
  private SearchableByteBufferUtil _searchableByteBufferUtil;
  private GenericRowColumnDataFileReader _genericRowColumnDataFileReader;

  @BeforeSuite
  public void setup() throws Exception {
    // Generate DISTINCT_VALUES shorts, ints, longs, floats and doubles
    Random random = new Random();
    SortedSet<Short> shortValues = new TreeSet<Short>();
    SortedSet<Integer> intValues = new TreeSet<Integer>();
    SortedSet<Long> longValues = new TreeSet<Long>();
    SortedSet<Float> floatValues = new TreeSet<Float>();
    SortedSet<Double> doubleValues = new TreeSet<Double>();

    // Add boundary conditions
    shortValues.add(Short.MIN_VALUE);
    shortValues.add(Short.MAX_VALUE);
    intValues.add(Integer.MIN_VALUE);
    intValues.add(Integer.MAX_VALUE);
    longValues.add(Long.MIN_VALUE);
    longValues.add(Long.MAX_VALUE);
    floatValues.add(Float.MIN_VALUE);
    floatValues.add(Float.MAX_VALUE);
    // floatValues.add(Float.NaN);
    floatValues.add(Float.NEGATIVE_INFINITY);
    floatValues.add(Float.POSITIVE_INFINITY);
    doubleValues.add(Double.MIN_VALUE);
    doubleValues.add(Double.MAX_VALUE);
    // doubleValues.add(Double.NaN);
    doubleValues.add(Double.NEGATIVE_INFINITY);
    doubleValues.add(Double.POSITIVE_INFINITY);

    while (shortValues.size() < DISTINCT_VALUES) {
      shortValues.add((short) random.nextInt());
    }

    while (intValues.size() < DISTINCT_VALUES) {
      intValues.add(random.nextInt());
    }

    while (longValues.size() < DISTINCT_VALUES) {
      longValues.add(random.nextLong());
    }

    while(floatValues.size() < DISTINCT_VALUES) {
      floatValues.add(random.nextFloat());
    }

    while(doubleValues.size() < DISTINCT_VALUES) {
      doubleValues.add(random.nextDouble());
    }

    // Copy these values into arrays
    int i = 0;
    for (Short shortValue : shortValues) {
      _shorts[i] = shortValue;
      ++i;
    }

    i = 0;
    for (Integer intValue : intValues) {
      _ints[i] = intValue;
      ++i;
    }

    i = 0;
    for (Long longValue : longValues) {
      _longs[i] = longValue;
      ++i;
    }

    i = 0;
    for (Float floatValue : floatValues) {
      _floats[i] = floatValue;
      ++i;
    }

    i = 0;
    for (Double doubleValue : doubleValues) {
      _doubles[i] = doubleValue;
      ++i;
    }

    // Write a file that multiplexes all these values
    final int SHORTS_OFFSET = 0;
    final int INTS_OFFSET = SHORTS_OFFSET + Short.SIZE / Byte.SIZE;
    final int LONGS_OFFSET = INTS_OFFSET + Integer.SIZE / Byte.SIZE;
    final int FLOATS_OFFSET = LONGS_OFFSET + Long.SIZE / Byte.SIZE;
    final int DOUBLES_OFFSET = FLOATS_OFFSET + Float.SIZE / Byte.SIZE;
    final int ROW_WIDTH = DOUBLES_OFFSET + Double.SIZE / Byte.SIZE;

    ByteBuffer buffer = ByteBuffer.allocate(ROW_WIDTH * DISTINCT_VALUES);

    for(i = 0; i < DISTINCT_VALUES; ++i) {
      final int baseOffset = i * ROW_WIDTH;
      buffer.putShort(baseOffset + SHORTS_OFFSET, _shorts[i]);
      buffer.putInt(baseOffset + INTS_OFFSET, _ints[i]);
      buffer.putLong(baseOffset + LONGS_OFFSET, _longs[i]);
      buffer.putFloat(baseOffset + FLOATS_OFFSET, _floats[i]);
      buffer.putDouble(baseOffset + DOUBLES_OFFSET, _doubles[i]);
    }

    File tempFile = File.createTempFile("pinot-test", ".tmp");
    tempFile.deleteOnExit();
    FileChannel channel = new FileOutputStream(tempFile).getChannel();
    // buffer.flip();
    channel.write(buffer);
    channel.close();
    
    _genericRowColumnDataFileReader = GenericRowColumnDataFileReader.forHeap(tempFile, DISTINCT_VALUES, 5, new int[] {
        Short.SIZE / Byte.SIZE,
        Integer.SIZE / Byte.SIZE,
        Long.SIZE / Byte.SIZE,
        Float.SIZE / Byte.SIZE,
        Double.SIZE / Byte.SIZE
    });
    _searchableByteBufferUtil = new SearchableByteBufferUtil(_genericRowColumnDataFileReader);
  }

  @Test
  public void testBinarySearch() {
    // Iterate through the sorted arrays, checking that their position matches with what we have
    for(int i = 0; i < DISTINCT_VALUES; ++i) {
      Assert.assertEquals(_searchableByteBufferUtil.binarySearch(0, _shorts[i]), i);
      Assert.assertEquals(_searchableByteBufferUtil.binarySearch(1, _ints[i]), i);
      Assert.assertEquals(_searchableByteBufferUtil.binarySearch(2, _longs[i]), i);
      Assert.assertEquals(_searchableByteBufferUtil.binarySearch(3, _floats[i]), i);
      Assert.assertEquals(_searchableByteBufferUtil.binarySearch(4, _doubles[i]), i);
    }
  }

  @AfterSuite
  public void destroy() {
    _genericRowColumnDataFileReader.close();
  }
}
