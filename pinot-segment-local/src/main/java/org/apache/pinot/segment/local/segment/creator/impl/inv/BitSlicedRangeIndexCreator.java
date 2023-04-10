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
package org.apache.pinot.segment.local.segment.creator.impl.inv;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import org.apache.pinot.segment.local.utils.FPOrdering;
import org.apache.pinot.segment.spi.index.creator.CombinedInvertedIndexCreator;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RangeBitmap;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION;
import static org.apache.pinot.spi.data.FieldSpec.DataType.DOUBLE;
import static org.apache.pinot.spi.data.FieldSpec.DataType.FLOAT;
import static org.apache.pinot.spi.data.FieldSpec.DataType.INT;
import static org.apache.pinot.spi.data.FieldSpec.DataType.LONG;


public class BitSlicedRangeIndexCreator implements CombinedInvertedIndexCreator {

  public static final int VERSION = 2;

  private final RangeBitmap.Appender _appender;
  private final File _rangeIndexFile;
  private final long _minValue;
  private final FieldSpec.DataType _valueType;

  private BitSlicedRangeIndexCreator(File indexDir, FieldSpec fieldSpec, long minValue, long maxValue,
      FieldSpec.DataType valueType) {
    Preconditions.checkArgument(fieldSpec.isSingleValueField(), "MV columns not supported");
    _rangeIndexFile = new File(indexDir, fieldSpec.getName() + BITMAP_RANGE_INDEX_FILE_EXTENSION);
    _appender = RangeBitmap.appender(maxValue);
    _minValue = minValue;
    _valueType = valueType;
  }

  /**
   * For dictionarized columns
   * @param indexDir the directory for the index
   * @param fieldSpec the specification of the field
   * @param cardinality the cardinality of the dictionary
   */
  public BitSlicedRangeIndexCreator(File indexDir, FieldSpec fieldSpec, int cardinality) {
    this(indexDir, fieldSpec, 0, cardinality - 1, fieldSpec.getDataType());
  }

  /**
   * For raw columns
   * @param indexDir the directory for the index
   * @param fieldSpec the specification of the field
   * @param minValue the minimum value
   * @param maxValue the maximum value
   */
  public BitSlicedRangeIndexCreator(File indexDir, FieldSpec fieldSpec, Comparable<?> minValue,
      Comparable<?> maxValue) {
    this(indexDir, fieldSpec, minValue(fieldSpec, minValue), maxValue(fieldSpec, minValue, maxValue),
        fieldSpec.getDataType());
  }

  @Override
  public FieldSpec.DataType getDataType() {
    return _valueType;
  }

  @Override
  public void add(int value) {
    _appender.add(value - _minValue);
  }

  @Override
  public void add(int[] values, int length) {
    throw new UnsupportedOperationException("MV not supported");
  }

  @Override
  public void add(long value) {
    _appender.add(value - _minValue);
  }

  @Override
  public void add(long[] values, int length) {
    throw new UnsupportedOperationException("MV not supported");
  }

  @Override
  public void add(float value) {
    _appender.add(FPOrdering.ordinalOf(value));
  }

  @Override
  public void add(float[] values, int length) {
    throw new UnsupportedOperationException("MV not supported");
  }

  @Override
  public void add(double value) {
    _appender.add(FPOrdering.ordinalOf(value));
  }

  @Override
  public void add(double[] values, int length) {
    throw new UnsupportedOperationException("MV not supported");
  }

  @Override
  public void seal()
      throws IOException {
    int headerSize = Integer.BYTES + Long.BYTES;
    int serializedSize = _appender.serializedSizeInBytes();
    try (MmapFileWriter writer = new MmapFileWriter(_rangeIndexFile, headerSize + serializedSize)) {
      writer.write(buf -> {
        buf.putInt(VERSION);
        buf.putLong(_minValue);
        _appender.serialize(buf);
      });
    }
    _appender.clear();
  }

  @Override
  public void close()
      throws IOException {
  }

  private static long maxValue(FieldSpec fieldSpec, Comparable<?> minValue, Comparable<?> maxValue) {
    FieldSpec.DataType storedType = fieldSpec.getDataType().getStoredType();
    if (storedType == INT || storedType == LONG) {
      return ((Number) maxValue).longValue() - ((Number) minValue).longValue();
    }
    if (storedType == FLOAT) {
      return 0xFFFFFFFFL;
    }
    if (storedType == DOUBLE) {
      return 0xFFFFFFFFFFFFFFFFL;
    }
    throw new IllegalArgumentException("Unsupported data type: " + fieldSpec.getDataType());
  }

  private static long minValue(FieldSpec fieldSpec, Comparable<?> minValue) {
    FieldSpec.DataType storedType = fieldSpec.getDataType().getStoredType();
    if (storedType == INT || storedType == LONG) {
      return ((Number) minValue).longValue();
    }
    return 0L;
  }
}
