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

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.io.File;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.utils.ArrayCopyUtils;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.FALFInterner;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;


public class ImmutableDictionaryTypeConversionTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "ImmutableDictionaryTypeConversionTest");
  private static final Random RANDOM = new Random();
  private static final String INT_COLUMN_NAME = "intColumn";
  private static final String LONG_COLUMN_NAME = "longColumn";
  private static final String FLOAT_COLUMN_NAME = "floatColumn";
  private static final String DOUBLE_COLUMN_NAME = "doubleColumn";
  private static final String BIG_DECIMAL_COLUMN_NAME = "bigDecimalColumn";
  private static final String STRING_COLUMN_NAME = "stringColumn";
  private static final String BYTES_COLUMN_NAME = "bytesColumn";
  private static final int NUM_VALUES = 1000;
  // For BytesDictionary, length need to be fixed
  private static final int MIN_VALUE = 100000;
  private static final int MAX_VALUE = 1000000;
  private static final int STRING_LENGTH = 6;
  private static final int BYTES_LENGTH = STRING_LENGTH / 2;

  private int[] _intValues;
  private long[] _longValues;
  private float[] _floatValues;
  private double[] _doubleValues;
  private BigDecimal[] _bigDecimalValues;
  private int _bigDecimalByteLength;
  private String[] _stringValues;
  private ByteArray[] _bytesValues;
  private ByteArray[] _utf8BytesValues;

  private int[] _dictIds;
  private int[] _intValuesBuffer;
  private long[] _longValuesBuffer;
  private float[] _floatValuesBuffer;
  private double[] _doubleValuesBuffer;
  private BigDecimal[] _bigDecimalValuesBuffer;
  private String[] _stringValuesBuffer;
  private byte[][] _bytesValuesBuffer;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);

    IntOpenHashSet intSet = new IntOpenHashSet();
    while (intSet.size() < NUM_VALUES) {
      intSet.add(RANDOM.nextInt(MAX_VALUE - MIN_VALUE) + MIN_VALUE);
    }
    _intValues = intSet.toIntArray();
    Arrays.sort(_intValues);

    _longValues = new long[NUM_VALUES];
    ArrayCopyUtils.copy(_intValues, _longValues, NUM_VALUES);

    _floatValues = new float[NUM_VALUES];
    ArrayCopyUtils.copy(_intValues, _floatValues, NUM_VALUES);

    _doubleValues = new double[NUM_VALUES];
    ArrayCopyUtils.copy(_intValues, _doubleValues, NUM_VALUES);

    _bigDecimalValues = new BigDecimal[NUM_VALUES];
    ArrayCopyUtils.copy(_intValues, _bigDecimalValues, NUM_VALUES);
    for (BigDecimal bigDecimal : _bigDecimalValues) {
      _bigDecimalByteLength = Math.max(_bigDecimalByteLength, BigDecimalUtils.byteSize(bigDecimal));
    }

    _stringValues = new String[NUM_VALUES];
    ArrayCopyUtils.copy(_intValues, _stringValues, NUM_VALUES);

    _bytesValues = new ByteArray[NUM_VALUES];
    _utf8BytesValues = new ByteArray[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      _bytesValues[i] = BytesUtils.toByteArray(_stringValues[i]);
      _utf8BytesValues[i] = new ByteArray(_stringValues[i].getBytes(UTF_8));
    }

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

    MetricFieldSpec bigDecimalMetricField = new MetricFieldSpec(BIG_DECIMAL_COLUMN_NAME, DataType.BIG_DECIMAL);
    bigDecimalMetricField.setSingleValueField(true);
    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(bigDecimalMetricField, TEMP_DIR)) {
      dictionaryCreator.build(_bigDecimalValues);
    }

    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(
        new DimensionFieldSpec(STRING_COLUMN_NAME, DataType.STRING, true), TEMP_DIR)) {
      dictionaryCreator.build(_stringValues);
      assertEquals(dictionaryCreator.getNumBytesPerEntry(), STRING_LENGTH);
    }

    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(
        new DimensionFieldSpec(BYTES_COLUMN_NAME, DataType.BYTES, true), TEMP_DIR)) {
      dictionaryCreator.build(_bytesValues);
      assertEquals(dictionaryCreator.getNumBytesPerEntry(), BYTES_LENGTH);
    }

    _dictIds = new int[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      _dictIds[i] = i;
    }
    _intValuesBuffer = new int[NUM_VALUES];
    _longValuesBuffer = new long[NUM_VALUES];
    _floatValuesBuffer = new float[NUM_VALUES];
    _doubleValuesBuffer = new double[NUM_VALUES];
    _bigDecimalValuesBuffer = new BigDecimal[NUM_VALUES];
    _stringValuesBuffer = new String[NUM_VALUES];
    _bytesValuesBuffer = new byte[NUM_VALUES][];
  }

  @Test
  public void testIntDictionary()
      throws Exception {
    try (IntDictionary intDictionary = new IntDictionary(
        PinotDataBuffer.mapReadOnlyBigEndianFile(new File(TEMP_DIR, INT_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)),
        NUM_VALUES)) {
      testNumericDictionary(intDictionary);
    }
  }

  @Test
  public void testOnHeapIntDictionary()
      throws Exception {
    try (OnHeapIntDictionary onHeapIntDictionary = new OnHeapIntDictionary(
        PinotDataBuffer.mapReadOnlyBigEndianFile(new File(TEMP_DIR, INT_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)),
        NUM_VALUES)) {
      testNumericDictionary(onHeapIntDictionary);
    }
  }

  @Test
  public void testLongDictionary()
      throws Exception {
    try (LongDictionary longDictionary = new LongDictionary(PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(TEMP_DIR, LONG_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES)) {
      testNumericDictionary(longDictionary);
    }
  }

  @Test
  public void testOnHeapLongDictionary()
      throws Exception {
    try (OnHeapLongDictionary onHeapLongDictionary = new OnHeapLongDictionary(PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(TEMP_DIR, LONG_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES)) {
      testNumericDictionary(onHeapLongDictionary);
    }
  }

  @Test
  public void testFloatDictionary()
      throws Exception {
    try (FloatDictionary floatDictionary = new FloatDictionary(PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(TEMP_DIR, FLOAT_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES)) {
      testNumericDictionary(floatDictionary);
    }
  }

  @Test
  public void testOnHeapFloatDictionary()
      throws Exception {
    try (OnHeapFloatDictionary onHeapFloatDictionary = new OnHeapFloatDictionary(
        PinotDataBuffer.mapReadOnlyBigEndianFile(
            new File(TEMP_DIR, FLOAT_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES)) {
      testNumericDictionary(onHeapFloatDictionary);
    }
  }

  @Test
  public void testDoubleDictionary()
      throws Exception {
    try (DoubleDictionary doubleDictionary = new DoubleDictionary(PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(TEMP_DIR, DOUBLE_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES)) {
      testNumericDictionary(doubleDictionary);
    }
  }

  @Test
  public void testOnHeapDoubleDictionary()
      throws Exception {
    try (OnHeapDoubleDictionary onHeapDoubleDictionary = new OnHeapDoubleDictionary(
        PinotDataBuffer.mapReadOnlyBigEndianFile(
            new File(TEMP_DIR, DOUBLE_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES)) {
      testNumericDictionary(onHeapDoubleDictionary);
    }
  }

  @Test
  public void testBigDecimalDictionary()
      throws Exception {
    try (BigDecimalDictionary bigDecimalDictionary = new BigDecimalDictionary(PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(TEMP_DIR, BIG_DECIMAL_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES,
        _bigDecimalByteLength)) {
      testNumericDictionary(bigDecimalDictionary);
    }
  }

  @Test
  public void testOnHeapBigDecimalDictionary()
      throws Exception {
    try (OnHeapBigDecimalDictionary onHeapBigDecimalDictionary = new OnHeapBigDecimalDictionary(
        PinotDataBuffer.mapReadOnlyBigEndianFile(
            new File(TEMP_DIR, BIG_DECIMAL_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES,
        _bigDecimalByteLength)) {
      testNumericDictionary(onHeapBigDecimalDictionary);
    }
  }

  private void testNumericDictionary(BaseImmutableDictionary dictionary) {
    for (int i = 0; i < NUM_VALUES; i++) {
      Assert.assertEquals(((Number) dictionary.get(i)).intValue(), _intValues[i]);
      assertEquals(dictionary.getIntValue(i), _intValues[i]);
      assertEquals(dictionary.getLongValue(i), _longValues[i]);
      assertEquals(dictionary.getFloatValue(i), _floatValues[i]);
      assertEquals(dictionary.getDoubleValue(i), _doubleValues[i]);
      Assert.assertEquals(Double.parseDouble(dictionary.getStringValue(i)), _doubleValues[i]);
    }
    dictionary.readIntValues(_dictIds, NUM_VALUES, _intValuesBuffer);
    Assert.assertEquals(_intValuesBuffer, _intValues);
    dictionary.readLongValues(_dictIds, NUM_VALUES, _longValuesBuffer);
    Assert.assertEquals(_longValuesBuffer, _longValues);
    dictionary.readFloatValues(_dictIds, NUM_VALUES, _floatValuesBuffer);
    Assert.assertEquals(_floatValuesBuffer, _floatValues);
    dictionary.readDoubleValues(_dictIds, NUM_VALUES, _doubleValuesBuffer);
    Assert.assertEquals(_doubleValuesBuffer, _doubleValues);
    dictionary.readBigDecimalValues(_dictIds, NUM_VALUES, _bigDecimalValuesBuffer);
    for (int i = 0; i < _bigDecimalValuesBuffer.length; i++) {
      Assert.assertEquals(_bigDecimalValuesBuffer[i].compareTo(_bigDecimalValues[i]), 0);
    }
    dictionary.readStringValues(_dictIds, NUM_VALUES, _stringValuesBuffer);
    for (int i = 0; i < NUM_VALUES; i++) {
      Assert.assertEquals(Double.parseDouble(_stringValuesBuffer[i]), _doubleValues[i]);
    }

    try {
      dictionary.getBytesValue(0);
      if (dictionary.getValueType() != DataType.BIG_DECIMAL) {
        Assert.fail();
      }
    } catch (UnsupportedOperationException e) {
      // Expected
    }
    try {
      dictionary.readBytesValues(_dictIds, NUM_VALUES, _bytesValuesBuffer);
      if (dictionary.getValueType() != DataType.BIG_DECIMAL) {
        Assert.fail();
      }
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testStringDictionary()
      throws Exception {
    try (StringDictionary stringDictionary = new StringDictionary(PinotDataBuffer.mapReadOnlyBigEndianFile(
        new File(TEMP_DIR, STRING_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES, STRING_LENGTH)) {
      testStringDictionary(stringDictionary);
    }
  }

  @Test
  public void testOnHeapStringDictionary()
      throws Exception {
    try (OnHeapStringDictionary onHeapStringDictionary = new OnHeapStringDictionary(
        PinotDataBuffer.mapReadOnlyBigEndianFile(
            new File(TEMP_DIR, STRING_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES, STRING_LENGTH,
        null, null)) {
      testStringDictionary(onHeapStringDictionary);
    }
  }

  @Test
  public void testOnHeapStringDictionaryWithInterner()
      throws Exception {
    FALFInterner<String> strInterner = new FALFInterner<>(128);
    FALFInterner<byte[]> byteInterner = new FALFInterner<>(128, Arrays::hashCode);

    try (OnHeapStringDictionary onHeapStringDictionary = new OnHeapStringDictionary(
        PinotDataBuffer.mapReadOnlyBigEndianFile(
            new File(TEMP_DIR, STRING_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES, STRING_LENGTH,
        strInterner, byteInterner)) {
      testStringDictionary(onHeapStringDictionary);
    }

    try (OnHeapStringDictionary onHeapStringDictionary = new OnHeapStringDictionary(
        PinotDataBuffer.mapReadOnlyBigEndianFile(
            new File(TEMP_DIR, STRING_COLUMN_NAME + V1Constants.Dict.FILE_EXTENSION)), NUM_VALUES, STRING_LENGTH,
        strInterner, byteInterner)) {
      testStringDictionary(onHeapStringDictionary);
    }
  }

  private void testStringDictionary(BaseImmutableDictionary dictionary) {
    for (int i = 0; i < NUM_VALUES; i++) {
      assertEquals(dictionary.get(i), _stringValues[i]);
      assertEquals(dictionary.getIntValue(i), _intValues[i]);
      assertEquals(dictionary.getLongValue(i), _longValues[i]);
      assertEquals(dictionary.getFloatValue(i), _floatValues[i]);
      assertEquals(dictionary.getDoubleValue(i), _doubleValues[i]);
      assertEquals(dictionary.getStringValue(i), _stringValues[i]);
      assertEquals(dictionary.getBytesValue(i), _utf8BytesValues[i].getBytes());
    }
    dictionary.readIntValues(_dictIds, NUM_VALUES, _intValuesBuffer);
    Assert.assertEquals(_intValuesBuffer, _intValues);
    dictionary.readLongValues(_dictIds, NUM_VALUES, _longValuesBuffer);
    Assert.assertEquals(_longValuesBuffer, _longValues);
    dictionary.readFloatValues(_dictIds, NUM_VALUES, _floatValuesBuffer);
    Assert.assertEquals(_floatValuesBuffer, _floatValues);
    dictionary.readDoubleValues(_dictIds, NUM_VALUES, _doubleValuesBuffer);
    Assert.assertEquals(_doubleValuesBuffer, _doubleValues);
    dictionary.readStringValues(_dictIds, NUM_VALUES, _stringValuesBuffer);
    Assert.assertEquals(_stringValuesBuffer, _stringValues);
    dictionary.readBytesValues(_dictIds, NUM_VALUES, _bytesValuesBuffer);
    for (int i = 0; i < NUM_VALUES; i++) {
      Assert.assertEquals(_bytesValuesBuffer[i], _utf8BytesValues[i].getBytes());
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

  private void testBytesDictionary(BaseImmutableDictionary dictionary) {
    for (int i = 0; i < NUM_VALUES; i++) {
      assertEquals(dictionary.get(i), _bytesValues[i].getBytes());
      assertEquals(dictionary.getStringValue(i), _stringValues[i]);
      assertEquals(dictionary.getBytesValue(i), _bytesValues[i].getBytes());
    }
    dictionary.readStringValues(_dictIds, NUM_VALUES, _stringValuesBuffer);
    Assert.assertEquals(_stringValuesBuffer, _stringValues);
    dictionary.readBytesValues(_dictIds, NUM_VALUES, _bytesValuesBuffer);
    for (int i = 0; i < NUM_VALUES; i++) {
      Assert.assertEquals(_bytesValuesBuffer[i], _bytesValues[i].getBytes());
    }

    try {
      dictionary.getIntValue(0);
      Assert.fail();
    } catch (UnsupportedOperationException e) {
      // Expected
    }
    try {
      dictionary.getLongValue(0);
      Assert.fail();
    } catch (UnsupportedOperationException e) {
      // Expected
    }
    try {
      dictionary.getFloatValue(0);
      Assert.fail();
    } catch (UnsupportedOperationException e) {
      // Expected
    }
    try {
      dictionary.getDoubleValue(0);
      Assert.fail();
    } catch (UnsupportedOperationException e) {
      // Expected
    }
    try {
      dictionary.readIntValues(_dictIds, NUM_VALUES, _intValuesBuffer);
      Assert.fail();
    } catch (UnsupportedOperationException e) {
      // Expected
    }
    try {
      dictionary.readLongValues(_dictIds, NUM_VALUES, _longValuesBuffer);
      Assert.fail();
    } catch (UnsupportedOperationException e) {
      // Expected
    }
    try {
      dictionary.readFloatValues(_dictIds, NUM_VALUES, _floatValuesBuffer);
      Assert.fail();
    } catch (UnsupportedOperationException e) {
      // Expected
    }
    try {
      dictionary.readDoubleValues(_dictIds, NUM_VALUES, _doubleValuesBuffer);
      Assert.fail();
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }
}
