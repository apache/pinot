/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.index.readers;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.utils.primitive.ByteArray;
import com.linkedin.pinot.core.segment.creator.impl.SegmentDictionaryCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ImmutableDictionaryReaderTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "ImmutableDictionaryReaderTest");
  private static final Random RANDOM = new Random();
  private static final String INT_COLUMN_NAME = "intColumn";
  private static final String LONG_COLUMN_NAME = "longColumn";
  private static final String FLOAT_COLUMN_NAME = "floatColumn";
  private static final String DOUBLE_COLUMN_NAME = "doubleColumn";
  private static final String STRING_COLUMN_NAME = "stringColumn";
  private static final String BYTES_COLUMN_NAME = "bytesColumn";
  private static final int NUM_VALUES = 1000;
  private static final int MAX_STRING_LENGTH = 100;
  private static final int BYTES_LENGTH = 100;

  private int[] _intValues;
  private long[] _longValues;
  private float[] _floatValues;
  private double[] _doubleValues;
  private String[] _stringValues;
  private ByteArray[] _bytesValues;

  private int _numBytesPerStringValue;

  @BeforeClass
  public void setUp() throws Exception {
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

    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(_intValues,
        new DimensionFieldSpec(INT_COLUMN_NAME, FieldSpec.DataType.INT, true), TEMP_DIR)) {
      dictionaryCreator.build();
    }

    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(_longValues,
        new DimensionFieldSpec(LONG_COLUMN_NAME, FieldSpec.DataType.LONG, true), TEMP_DIR)) {
      dictionaryCreator.build();
    }

    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(_floatValues,
        new DimensionFieldSpec(FLOAT_COLUMN_NAME, FieldSpec.DataType.FLOAT, true), TEMP_DIR)) {
      dictionaryCreator.build();
    }

    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(_doubleValues,
        new DimensionFieldSpec(DOUBLE_COLUMN_NAME, FieldSpec.DataType.DOUBLE, true), TEMP_DIR)) {
      dictionaryCreator.build();
    }

    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(_stringValues,
        new DimensionFieldSpec(STRING_COLUMN_NAME, FieldSpec.DataType.STRING, true), TEMP_DIR)) {
      dictionaryCreator.build();
      _numBytesPerStringValue = dictionaryCreator.getNumBytesPerEntry();
    }

    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(_bytesValues,
        new DimensionFieldSpec(BYTES_COLUMN_NAME, FieldSpec.DataType.BYTES, true), TEMP_DIR)) {
      dictionaryCreator.build();
      assertEquals(dictionaryCreator.getNumBytesPerEntry(), BYTES_LENGTH);
    }
  }

  @Test
  public void testIntDictionary() throws Exception {
    try (IntDictionary intDictionary = new IntDictionary(
        PinotDataBuffer.mapReadOnlyBigEndianFile(new File(TEMP_DIR, INT_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)),
        NUM_VALUES)) {
      for (int i = 0; i < NUM_VALUES; i++) {
        assertEquals(intDictionary.get(i).intValue(), _intValues[i]);
        assertEquals(intDictionary.getIntValue(i), _intValues[i]);
        assertEquals(intDictionary.getLongValue(i), _intValues[i]);
        assertEquals(intDictionary.getFloatValue(i), _intValues[i], 0.0f);
        assertEquals(intDictionary.getDoubleValue(i), _intValues[i], 0.0);
        assertEquals(Integer.parseInt(intDictionary.getStringValue(i)), _intValues[i]);

        assertEquals(intDictionary.indexOf(_intValues[i]), i);

        int randomInt = RANDOM.nextInt();
        assertEquals(intDictionary.insertionIndexOf(randomInt), Arrays.binarySearch(_intValues, randomInt));
      }
    }
  }

  @Test
  public void testLongDictionary() throws Exception {
    try (LongDictionary longDictionary = new LongDictionary(PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(TEMP_DIR, LONG_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES)) {
      for (int i = 0; i < NUM_VALUES; i++) {
        assertEquals(longDictionary.get(i).longValue(), _longValues[i]);
        assertEquals(longDictionary.getIntValue(i), (int) _longValues[i]);
        assertEquals(longDictionary.getLongValue(i), _longValues[i]);
        assertEquals(longDictionary.getFloatValue(i), _longValues[i], 0.0f);
        assertEquals(longDictionary.getDoubleValue(i), _longValues[i], 0.0);
        assertEquals(Long.parseLong(longDictionary.getStringValue(i)), _longValues[i]);

        assertEquals(longDictionary.indexOf(_longValues[i]), i);

        long randomLong = RANDOM.nextLong();
        assertEquals(longDictionary.insertionIndexOf(randomLong), Arrays.binarySearch(_longValues, randomLong));
      }
    }
  }

  @Test
  public void testFloatDictionary() throws Exception {
    try (FloatDictionary floatDictionary = new FloatDictionary(PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(TEMP_DIR, FLOAT_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES)) {
      for (int i = 0; i < NUM_VALUES; i++) {
        assertEquals(floatDictionary.get(i), _floatValues[i], 0.0f);
        assertEquals(floatDictionary.getIntValue(i), (int) _floatValues[i]);
        assertEquals(floatDictionary.getLongValue(i), (long) _floatValues[i]);
        assertEquals(floatDictionary.getFloatValue(i), _floatValues[i], 0.0f);
        assertEquals(floatDictionary.getDoubleValue(i), _floatValues[i], 0.0);
        assertEquals(Float.parseFloat(floatDictionary.getStringValue(i)), _floatValues[i], 0.0f);

        assertEquals(floatDictionary.indexOf(_floatValues[i]), i);

        float randomFloat = RANDOM.nextFloat();
        assertEquals(floatDictionary.insertionIndexOf(randomFloat), Arrays.binarySearch(_floatValues, randomFloat));
      }
    }
  }

  @Test
  public void testDoubleDictionary() throws Exception {
    try (DoubleDictionary doubleDictionary = new DoubleDictionary(PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(TEMP_DIR, DOUBLE_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES)) {
      for (int i = 0; i < NUM_VALUES; i++) {
        assertEquals(doubleDictionary.get(i), _doubleValues[i], 0.0);
        assertEquals(doubleDictionary.getIntValue(i), (int) _doubleValues[i]);
        assertEquals(doubleDictionary.getLongValue(i), (long) _doubleValues[i]);
        assertEquals(doubleDictionary.getFloatValue(i), (float) _doubleValues[i], 0.0f);
        assertEquals(doubleDictionary.getDoubleValue(i), _doubleValues[i], 0.0);
        assertEquals(Double.parseDouble(doubleDictionary.getStringValue(i)), _doubleValues[i], 0.0);

        assertEquals(doubleDictionary.indexOf(_doubleValues[i]), i);

        double randomDouble = RANDOM.nextDouble();
        assertEquals(doubleDictionary.insertionIndexOf(randomDouble), Arrays.binarySearch(_doubleValues, randomDouble));
      }
    }
  }

  @Test
  public void testStringDictionary() throws Exception {
    try (StringDictionary stringDictionary = new StringDictionary(PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(TEMP_DIR, STRING_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES, _numBytesPerStringValue,
        (byte) 0)) {
      testStringDictionary(stringDictionary);
    }
  }

  @Test
  public void testOnHeapStringDictionary() throws Exception {
    try (OnHeapStringDictionary onHeapStringDictionary = new OnHeapStringDictionary(
        PinotDataBuffer.mapReadOnlyBigEndianFile(
            new File(TEMP_DIR, STRING_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES,
        _numBytesPerStringValue, (byte) 0)) {
      testStringDictionary(onHeapStringDictionary);
    }
  }

  private void testStringDictionary(ImmutableDictionaryReader stringDictionary) {
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
  public void testBytesDictionary() throws Exception {
    try (BytesDictionary bytesDictionary = new BytesDictionary(PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(TEMP_DIR, BYTES_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES, BYTES_LENGTH)) {
      for (int i = 0; i < NUM_VALUES; i++) {
        assertEquals(new ByteArray(bytesDictionary.get(i)), _bytesValues[i]);
        assertEquals(new ByteArray(bytesDictionary.getBytesValue(i)), _bytesValues[i]);

        assertEquals(bytesDictionary.indexOf(_bytesValues[i].getBytes()), i);

        byte[] randomBytes = new byte[BYTES_LENGTH];
        RANDOM.nextBytes(randomBytes);
        assertEquals(bytesDictionary.insertionIndexOf(randomBytes),
            Arrays.binarySearch(_bytesValues, new ByteArray(randomBytes)));
      }
    }
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }
}
