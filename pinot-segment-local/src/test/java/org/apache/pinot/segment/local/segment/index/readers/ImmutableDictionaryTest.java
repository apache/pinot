/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.segment.index.readers;

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.io.File;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ImmutableDictionaryTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "ImmutableDictionaryTest");
  private static final Random RANDOM = new Random();
  private static final String INT_COLUMN_NAME = "intColumn";
  private static final String LONG_COLUMN_NAME = "longColumn";
  private static final String FLOAT_COLUMN_NAME = "floatColumn";
  private static final String DOUBLE_COLUMN_NAME = "doubleColumn";
  private static final String BIG_DECIMAL_COLUMN_NAME = "bigDecimalColumn";
  private static final String STRING_COLUMN_NAME = "stringColumn";
  private static final String BYTES_COLUMN_NAME = "bytesColumn";
  private static final int NUM_VALUES = 1000;
  private static final int MAX_STRING_LENGTH = 100;
  private static final int BYTES_LENGTH = 100;

  private int[] _intValues;
  private long[] _longValues;
  private float[] _floatValues;
  private double[] _doubleValues;
  private BigDecimal[] _bigDecimalValues;
  private int _bigDecimalByteLength;
  private String[] _stringValues;
  private ByteArray[] _bytesValues;

  private int _numBytesPerStringValue;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);

    IntOpenHashSet intSet = new IntOpenHashSet();
    while (intSet.size() < NUM_VALUES) {
      intSet.add(RANDOM.nextInt());
    }
    _intValues = intSet.toIntArray();
    Arrays.sort(_intValues);

    LongOpenHashSet longSet = new LongOpenHashSet();
    while (longSet.size() < NUM_VALUES) {
      longSet.add(RANDOM.nextLong());
    }
    _longValues = longSet.toLongArray();
    Arrays.sort(_longValues);

    FloatOpenHashSet floatSet = new FloatOpenHashSet();
    while (floatSet.size() < NUM_VALUES) {
      floatSet.add(RANDOM.nextFloat());
    }
    _floatValues = floatSet.toFloatArray();
    Arrays.sort(_floatValues);

    DoubleOpenHashSet doubleSet = new DoubleOpenHashSet();
    while (doubleSet.size() < NUM_VALUES) {
      doubleSet.add(RANDOM.nextDouble());
    }
    _doubleValues = doubleSet.toDoubleArray();
    Arrays.sort(_doubleValues);

    TreeSet<BigDecimal> bigDecimalSet = new TreeSet<>();
    while (bigDecimalSet.size() < NUM_VALUES) {
      BigDecimal bigDecimal = BigDecimal.valueOf(RANDOM.nextDouble());
      _bigDecimalByteLength = Math.max(_bigDecimalByteLength, BigDecimalUtils.byteSize(bigDecimal));
      bigDecimalSet.add(bigDecimal);
    }
    _bigDecimalValues = bigDecimalSet.toArray(new BigDecimal[0]);
    Arrays.sort(_bigDecimalValues);

    Set<String> stringSet = new HashSet<>();
    while (stringSet.size() < NUM_VALUES) {
      stringSet.add(RandomStringUtils.random(RANDOM.nextInt(MAX_STRING_LENGTH)).replace('\0', ' '));
    }
    _stringValues = stringSet.toArray(new String[NUM_VALUES]);
    Arrays.sort(_stringValues);

    Set<ByteArray> bytesSet = new HashSet<>();
    while (bytesSet.size() < NUM_VALUES) {
      byte[] bytes = new byte[BYTES_LENGTH];
      RANDOM.nextBytes(bytes);
      bytesSet.add(new ByteArray(bytes));
    }
    _bytesValues = bytesSet.toArray(new ByteArray[NUM_VALUES]);
    Arrays.sort(_bytesValues);

    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(
        new DimensionFieldSpec(INT_COLUMN_NAME, DataType.INT, true), TEMP_DIR)) {
      dictionaryCreator.build(_intValues);
    }

    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(
        new DimensionFieldSpec(LONG_COLUMN_NAME, DataType.LONG, true), TEMP_DIR)) {
      dictionaryCreator.build(_longValues);
    }

    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(
        new DimensionFieldSpec(FLOAT_COLUMN_NAME, DataType.FLOAT, true), TEMP_DIR)) {
      dictionaryCreator.build(_floatValues);
    }

    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(
        new DimensionFieldSpec(DOUBLE_COLUMN_NAME, DataType.DOUBLE, true), TEMP_DIR)) {
      dictionaryCreator.build(_doubleValues);
    }

    // Note: BigDecimalDictionary requires setting useVarLengthDictionary to true.
    boolean useVarLengthDictionary = true;
    MetricFieldSpec bigDecimalMetricField = new MetricFieldSpec(BIG_DECIMAL_COLUMN_NAME, DataType.BIG_DECIMAL);
    bigDecimalMetricField.setSingleValueField(true);
    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(bigDecimalMetricField, TEMP_DIR,
        useVarLengthDictionary)) {
      dictionaryCreator.build(_bigDecimalValues);
    }

    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(
        new DimensionFieldSpec(STRING_COLUMN_NAME, DataType.STRING, true), TEMP_DIR)) {
      dictionaryCreator.build(_stringValues);
      _numBytesPerStringValue = dictionaryCreator.getNumBytesPerEntry();
    }

    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(
        new DimensionFieldSpec(BYTES_COLUMN_NAME, DataType.BYTES, true), TEMP_DIR)) {
      dictionaryCreator.build(_bytesValues);
      assertEquals(dictionaryCreator.getNumBytesPerEntry(), BYTES_LENGTH);
    }
  }

  @Test
  public void testIntDictionary()
      throws Exception {
    try (IntDictionary intDictionary = new IntDictionary(
        PinotDataBuffer.mapReadOnlyBigEndianFile(new File(TEMP_DIR, INT_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)),
        NUM_VALUES)) {
      testIntDictionary(intDictionary);
    }
  }

  @Test
  public void testOnHeapIntDictionary()
      throws Exception {
    try (OnHeapIntDictionary onHeapIntDictionary = new OnHeapIntDictionary(
        PinotDataBuffer.mapReadOnlyBigEndianFile(new File(TEMP_DIR, INT_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)),
        NUM_VALUES)) {
      testIntDictionary(onHeapIntDictionary);
    }
  }

  private void testIntDictionary(BaseImmutableDictionary intDictionary) {
    for (int i = 0; i < NUM_VALUES; i++) {
      assertEquals(intDictionary.get(i), _intValues[i]);
      assertEquals(intDictionary.getIntValue(i), _intValues[i]);
      assertEquals(intDictionary.getLongValue(i), _intValues[i]);
      assertEquals(intDictionary.getFloatValue(i), (float) _intValues[i]);
      assertEquals(intDictionary.getDoubleValue(i), (double) _intValues[i]);
      Assert.assertEquals(Integer.parseInt(intDictionary.getStringValue(i)), _intValues[i]);

      assertEquals(intDictionary.indexOf(String.valueOf(_intValues[i])), i);

      int randomInt = RANDOM.nextInt();
      assertEquals(intDictionary.insertionIndexOf(String.valueOf(randomInt)),
          Arrays.binarySearch(_intValues, randomInt));
    }
  }

  @Test
  public void testLongDictionary()
      throws Exception {
    try (LongDictionary longDictionary = new LongDictionary(PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(TEMP_DIR, LONG_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES)) {
      testLongDictionary(longDictionary);
    }
  }

  @Test
  public void testOnHeapLongDictionary()
      throws Exception {
    try (OnHeapLongDictionary onHeapLongDictionary = new OnHeapLongDictionary(PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(TEMP_DIR, LONG_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES)) {
      testLongDictionary(onHeapLongDictionary);
    }
  }

  private void testLongDictionary(BaseImmutableDictionary longDictionary) {
    for (int i = 0; i < NUM_VALUES; i++) {
      assertEquals(longDictionary.get(i), _longValues[i]);
      assertEquals(longDictionary.getIntValue(i), (int) _longValues[i]);
      assertEquals(longDictionary.getLongValue(i), _longValues[i]);
      assertEquals(longDictionary.getFloatValue(i), (float) _longValues[i]);
      assertEquals(longDictionary.getDoubleValue(i), (double) _longValues[i]);
      Assert.assertEquals(Long.parseLong(longDictionary.getStringValue(i)), _longValues[i]);

      assertEquals(longDictionary.indexOf(String.valueOf(_longValues[i])), i);

      long randomLong = RANDOM.nextLong();
      assertEquals(longDictionary.insertionIndexOf(String.valueOf(randomLong)),
          Arrays.binarySearch(_longValues, randomLong));
    }
  }

  @Test
  public void testFloatDictionary()
      throws Exception {
    try (FloatDictionary floatDictionary = new FloatDictionary(PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(TEMP_DIR, FLOAT_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES)) {
      testFloatDictionary(floatDictionary);
    }
  }

  @Test
  public void testOnHeapFloatDictionary()
      throws Exception {
    try (OnHeapFloatDictionary onHeapFloatDictionary = new OnHeapFloatDictionary(
        PinotDataBuffer.mapReadOnlyBigEndianFile(
            new File(TEMP_DIR, FLOAT_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES)) {
      testFloatDictionary(onHeapFloatDictionary);
    }
  }

  private void testFloatDictionary(BaseImmutableDictionary floatDictionary) {
    for (int i = 0; i < NUM_VALUES; i++) {
      assertEquals(floatDictionary.get(i), _floatValues[i]);
      assertEquals(floatDictionary.getIntValue(i), (int) _floatValues[i]);
      assertEquals(floatDictionary.getLongValue(i), (long) _floatValues[i]);
      assertEquals(floatDictionary.getFloatValue(i), _floatValues[i]);
      assertEquals(floatDictionary.getDoubleValue(i), Double.parseDouble(Float.valueOf(_floatValues[i]).toString()));
      Assert.assertEquals(Float.parseFloat(floatDictionary.getStringValue(i)), _floatValues[i], 0.0f);

      assertEquals(floatDictionary.indexOf(String.valueOf(_floatValues[i])), i);

      float randomFloat = RANDOM.nextFloat();
      assertEquals(floatDictionary.insertionIndexOf(String.valueOf(randomFloat)),
          Arrays.binarySearch(_floatValues, randomFloat));
    }
  }

  @Test
  public void testDoubleDictionary()
      throws Exception {
    try (DoubleDictionary doubleDictionary = new DoubleDictionary(PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(TEMP_DIR, DOUBLE_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES)) {
      testDoubleDictionary(doubleDictionary);
    }
  }

  @Test
  public void testOnHeapDoubleDictionary()
      throws Exception {
    try (OnHeapDoubleDictionary onHeapDoubleDictionary = new OnHeapDoubleDictionary(
        PinotDataBuffer.mapReadOnlyBigEndianFile(
            new File(TEMP_DIR, DOUBLE_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES)) {
      testDoubleDictionary(onHeapDoubleDictionary);
    }
  }

  private void testDoubleDictionary(BaseImmutableDictionary doubleDictionary) {
    for (int i = 0; i < NUM_VALUES; i++) {
      assertEquals(doubleDictionary.get(i), _doubleValues[i]);
      assertEquals(doubleDictionary.getIntValue(i), (int) _doubleValues[i]);
      assertEquals(doubleDictionary.getLongValue(i), (long) _doubleValues[i]);
      assertEquals(doubleDictionary.getFloatValue(i), (float) _doubleValues[i]);
      assertEquals(doubleDictionary.getDoubleValue(i), _doubleValues[i]);
      Assert.assertEquals(Double.parseDouble(doubleDictionary.getStringValue(i)), _doubleValues[i], 0.0);

      assertEquals(doubleDictionary.indexOf(String.valueOf(_doubleValues[i])), i);

      double randomDouble = RANDOM.nextDouble();
      assertEquals(doubleDictionary.insertionIndexOf(String.valueOf(randomDouble)),
          Arrays.binarySearch(_doubleValues, randomDouble));
    }
  }

  @Test
  public void testBigDecimalDictionary()
      throws Exception {
    try (BigDecimalDictionary bigDecimalDictionary = new BigDecimalDictionary(PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(TEMP_DIR, BIG_DECIMAL_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES,
        _bigDecimalByteLength)) {
      testBigDecimalDictionary(bigDecimalDictionary);
    }
  }

  @Test
  public void testOnHeapBigDecimalDictionary()
      throws Exception {
    try (OnHeapBigDecimalDictionary onHeapBigDecimalDictionary = new OnHeapBigDecimalDictionary(
        PinotDataBuffer.mapReadOnlyBigEndianFile(
            new File(TEMP_DIR, BIG_DECIMAL_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES,
        _bigDecimalByteLength)) {
      testBigDecimalDictionary(onHeapBigDecimalDictionary);
    }
  }

  private void testBigDecimalDictionary(BaseImmutableDictionary bigDecimalDictionary) {
    for (int i = 0; i < NUM_VALUES; i++) {
      assertEquals(bigDecimalDictionary.get(i), _bigDecimalValues[i]);
      assertEquals(bigDecimalDictionary.getIntValue(i), _bigDecimalValues[i].intValue());
      assertEquals(bigDecimalDictionary.getLongValue(i), _bigDecimalValues[i].longValue());
      assertEquals(bigDecimalDictionary.getFloatValue(i), _bigDecimalValues[i].floatValue());
      assertEquals(bigDecimalDictionary.getDoubleValue(i), _bigDecimalValues[i].doubleValue());
      assertEquals(bigDecimalDictionary.getBigDecimalValue(i), _bigDecimalValues[i]);
      Assert.assertEquals(new BigDecimal(bigDecimalDictionary.getStringValue(i)), _bigDecimalValues[i]);

      assertEquals(bigDecimalDictionary.indexOf(String.valueOf(_bigDecimalValues[i])), i);

      BigDecimal randomBigDecimal = BigDecimal.valueOf(RANDOM.nextDouble());
      assertEquals(bigDecimalDictionary.insertionIndexOf(String.valueOf(randomBigDecimal)),
          Arrays.binarySearch(_bigDecimalValues, randomBigDecimal));
    }
  }

  @Test
  public void testStringDictionary()
      throws Exception {
    try (StringDictionary stringDictionary = new StringDictionary(PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(TEMP_DIR, STRING_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES,
        _numBytesPerStringValue)) {
      testStringDictionary(stringDictionary);
    }
  }

  @Test
  public void testOnHeapStringDictionary()
      throws Exception {
    try (OnHeapStringDictionary onHeapStringDictionary = new OnHeapStringDictionary(
        PinotDataBuffer.mapReadOnlyBigEndianFile(
            new File(TEMP_DIR, STRING_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES,
        _numBytesPerStringValue)) {
      testStringDictionary(onHeapStringDictionary);
    }
  }

  private void testStringDictionary(BaseImmutableDictionary stringDictionary) {
    for (int i = 0; i < NUM_VALUES; i++) {
      assertEquals(stringDictionary.get(i), _stringValues[i]);
      assertEquals(stringDictionary.getStringValue(i), _stringValues[i]);

      assertEquals(stringDictionary.indexOf(_stringValues[i]), i);

      // Test String longer than MAX_STRING_LENGTH
      String randomString = RandomStringUtils.random(RANDOM.nextInt(2 * MAX_STRING_LENGTH)).replace('\0', ' ');
      assertEquals(stringDictionary.insertionIndexOf(randomString), Arrays.binarySearch(_stringValues, randomString));
    }
  }

  @Test
  public void testBytesDictionary()
      throws Exception {
    try (BytesDictionary bytesDictionary = new BytesDictionary(PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(TEMP_DIR, BYTES_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES, BYTES_LENGTH)) {
      testBytesDictionary(bytesDictionary);
    }
  }

  @Test
  public void testOnHeapBytesDictionary()
      throws Exception {
    try (OnHeapBytesDictionary onHeapBytesDictionary = new OnHeapBytesDictionary(
        PinotDataBuffer.mapReadOnlyBigEndianFile(
            new File(TEMP_DIR, BYTES_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES, BYTES_LENGTH)) {
      testBytesDictionary(onHeapBytesDictionary);
    }
  }

  private void testBytesDictionary(BaseImmutableDictionary bytesDictionary) {
    for (int i = 0; i < NUM_VALUES; i++) {
      assertEquals(bytesDictionary.get(i), _bytesValues[i].getBytes());
      assertEquals(bytesDictionary.getStringValue(i), _bytesValues[i].toHexString());
      assertEquals(bytesDictionary.getBytesValue(i), _bytesValues[i].getBytes());

      assertEquals(bytesDictionary.indexOf(_bytesValues[i].toHexString()), i);

      byte[] randomBytes = new byte[BYTES_LENGTH];
      RANDOM.nextBytes(randomBytes);
      assertEquals(bytesDictionary.insertionIndexOf(BytesUtils.toHexString(randomBytes)),
          Arrays.binarySearch(_bytesValues, new ByteArray(randomBytes)));
    }
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }
}
