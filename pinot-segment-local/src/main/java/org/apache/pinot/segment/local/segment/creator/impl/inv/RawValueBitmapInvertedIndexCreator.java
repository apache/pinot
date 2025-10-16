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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.creator.RawValueBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * Creator for raw value bitmap inverted index.
 * File format:
 * <ul>
 *   <li>Header (44 bytes):</li>
 *   <ul>
 *     <li>version (4 bytes)</li>
 *     <li>cardinality (4 bytes)</li>
 *     <li>maxLength (4 bytes) - for string/bytes data types</li>
 *     <li>dictionaryOffset (8 bytes)</li>
 *     <li>dictionaryLength (8 bytes)</li>
 *     <li>invertedIndexOffset (8 bytes)</li>
 *     <li>invertedIndexLength (8 bytes)</li>
 *   </ul>
 *   <li>Dictionary data</li>
 *   <li>Inverted index data</li>
 * </ul>
 */
public class RawValueBitmapInvertedIndexCreator implements RawValueBasedInvertedIndexCreator {
  private static final int VERSION = 1;
  private static final int HEADER_LENGTH = 44;

  private final File _indexFile;
  private final DataType _dataType;
  private final String _columnName;
  private final TreeMap<Object, MutableRoaringBitmap> _valueToDocIds;
  private SegmentDictionaryCreator _dictionaryCreator;
  private int _maxLength; // Only used for STRING and BYTES

  private int _nextDocId = 0;
  private boolean _isClosed = false;

  public RawValueBitmapInvertedIndexCreator(IndexCreationContext context)
      throws IOException {
    this(context.getFieldSpec().getDataType().getStoredType(), context.getFieldSpec().getName(),
        context.getIndexDir());
  }

  public RawValueBitmapInvertedIndexCreator(DataType dataType, String columnName, File indexDir) {
    _dataType = dataType;
    _columnName = columnName;
    _indexFile = new File(indexDir, columnName + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    _valueToDocIds = new TreeMap<>();
  }

  @Override
  public void add(int value) {
    addValue(value);
  }

  @Override
  public void add(int[] values, int length) {
    Integer[] boxedValues = new Integer[length];
    for (int i = 0; i < length; i++) {
      boxedValues[i] = values[i];
    }
    addValues(boxedValues, length);
  }

  @Override
  public void add(long value) {
    addValue(value);
  }

  @Override
  public void add(long[] values, int length) {
    Long[] boxedValues = new Long[length];
    for (int i = 0; i < length; i++) {
      boxedValues[i] = values[i];
    }
    addValues(boxedValues, length);
  }

  @Override
  public void add(float value) {
    addValue(value);
  }

  @Override
  public void add(float[] values, int length) {
    Float[] boxedValues = new Float[length];
    for (int i = 0; i < length; i++) {
      boxedValues[i] = values[i];
    }
    addValues(boxedValues, length);
  }

  @Override
  public void add(double value) {
    addValue(value);
  }

  @Override
  public void add(double[] values, int length) {
    Double[] boxedValues = new Double[length];
    for (int i = 0; i < length; i++) {
      boxedValues[i] = values[i];
    }
    addValues(boxedValues, length);
  }

  @Override
  public void add(String value) {
    if (value != null) {
      _maxLength = Math.max(_maxLength, value.length());
      addValue(value);
    }
  }

  @Override
  public void add(String[] values, int length) {
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        _maxLength = Math.max(_maxLength, values[i].length());
      }
    }
    addValues(values, length);
  }

  @Override
  public void add(byte[] value) {
    if (value != null) {
      _maxLength = Math.max(_maxLength, value.length);
      addValue(value);
    }
  }

  @Override
  public void add(byte[][] values, int length) {
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        _maxLength = Math.max(_maxLength, values[i].length);
      }
    }
    addValues(values, length);
  }
  @Override
  public void add(Object value, int dictId)
      throws IOException {
  }

  @Override
  public void add(Object[] values, @Nullable int[] dictIds)
      throws IOException {
  }

  private void addValue(Object value) {
    if (value != null) {
      MutableRoaringBitmap bitmap = _valueToDocIds.get(value);
      if (bitmap == null) {
        bitmap = new MutableRoaringBitmap();
        _valueToDocIds.put(value, bitmap);
      }
      bitmap.add(_nextDocId);
    }
    _nextDocId++;
  }

  private void addValues(Object[] values, int length) {
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        MutableRoaringBitmap bitmap = _valueToDocIds.get(values[i]);
        if (bitmap == null) {
          bitmap = new MutableRoaringBitmap();
          _valueToDocIds.put(values[i], bitmap);
        }
        bitmap.add(_nextDocId);
      }
    }
    _nextDocId++;
  }

  @Override
  public void seal()
      throws IOException {
    if (_isClosed) {
      return;
    }

    // Create dictionary from unique values
    boolean useVarLengthDictionary = _dataType == DataType.STRING;
    FieldSpec fieldSpec = new DimensionFieldSpec(_columnName, _dataType, true);
    _dictionaryCreator = new SegmentDictionaryCreator(fieldSpec, new File(_indexFile.getParent()),
        useVarLengthDictionary);

    // Convert values to type-specific array
    Object[] rawValues = _valueToDocIds.keySet().toArray();
    int numValues = rawValues.length;
    Object typedValues;
    switch (_dataType.getStoredType()) {
      case INT:
        int[] intValues = new int[numValues];
        for (int i = 0; i < numValues; i++) {
          intValues[i] = (Integer) rawValues[i];
        }
        typedValues = intValues;
        break;
      case LONG:
        long[] longValues = new long[numValues];
        for (int i = 0; i < numValues; i++) {
          longValues[i] = (Long) rawValues[i];
        }
        typedValues = longValues;
        break;
      case FLOAT:
        float[] floatValues = new float[numValues];
        for (int i = 0; i < numValues; i++) {
          floatValues[i] = (Float) rawValues[i];
        }
        typedValues = floatValues;
        break;
      case DOUBLE:
        double[] doubleValues = new double[numValues];
        for (int i = 0; i < numValues; i++) {
          doubleValues[i] = (Double) rawValues[i];
        }
        typedValues = doubleValues;
        break;
      case STRING:
        String[] stringValues = new String[numValues];
        for (int i = 0; i < numValues; i++) {
          stringValues[i] = (String) rawValues[i];
        }
        typedValues = stringValues;
        break;
      case BYTES:
        byte[][] bytesValues = new byte[numValues][];
        for (int i = 0; i < numValues; i++) {
          bytesValues[i] = (byte[]) rawValues[i];
        }
        typedValues = bytesValues;
        break;
      default:
        throw new IllegalStateException("Unsupported data type: " + _dataType);
    }
    _dictionaryCreator.build(typedValues);

    // Write header placeholder
    ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_LENGTH).order(ByteOrder.BIG_ENDIAN);
    headerBuffer.putInt(VERSION);
    headerBuffer.putInt(numValues);
    headerBuffer.putInt(_maxLength);
    headerBuffer.putLong(0L); // dictionaryOffset placeholder
    headerBuffer.putLong(0L); // dictionaryLength placeholder
    headerBuffer.putLong(0L); // invertedIndexOffset placeholder
    headerBuffer.putLong(0L); // invertedIndexLength placeholder
    try (FileOutputStream outputStream = new FileOutputStream(_indexFile)) {
      outputStream.write(headerBuffer.array());
    }

    // Write dictionary
    long dictionaryOffset = HEADER_LENGTH;
    File dictionaryFile = _dictionaryCreator.getDictionaryFile();
    long dictionaryLength = dictionaryFile.length();
    FileUtils.writeByteArrayToFile(_indexFile, FileUtils.readFileToByteArray(dictionaryFile), true);

    // Write inverted index
    long invertedIndexOffset = dictionaryOffset + dictionaryLength;
    try (FileChannel channel = new RandomAccessFile(_indexFile, "rw").getChannel()) {
      channel.position(invertedIndexOffset);
      try (BitmapInvertedIndexWriter writer = new BitmapInvertedIndexWriter(channel, numValues, false)) {
        for (Object value : rawValues) {
          writer.add(_valueToDocIds.get(value).toRoaringBitmap());
        }
      }
    }
    long invertedIndexLength = _indexFile.length() - invertedIndexOffset;

    // Update header with actual offsets and lengths
    try (PinotDataBuffer buffer = PinotDataBuffer.mapFile(_indexFile, false, 0, HEADER_LENGTH, ByteOrder.BIG_ENDIAN,
        getClass().getSimpleName())) {
      buffer.putInt(0, VERSION);
      buffer.putInt(4, numValues);
      buffer.putInt(8, _maxLength);
      buffer.putLong(12, dictionaryOffset);
      buffer.putLong(20, dictionaryLength);
      buffer.putLong(28, invertedIndexOffset);
      buffer.putLong(36, invertedIndexLength);
    }

    _isClosed = true;
  }

  @Override
  public void close()
      throws IOException {
    if (!_isClosed) {
      seal();
    }
    if (_dictionaryCreator != null) {
      _dictionaryCreator.close();
    }
  }
}
