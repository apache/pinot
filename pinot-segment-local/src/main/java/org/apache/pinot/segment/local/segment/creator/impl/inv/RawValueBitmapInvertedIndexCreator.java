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
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.UuidUtils;
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
      addValue(new ByteArray(value));
    }
  }

  @Override
  public void add(byte[][] values, int length) {
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        _maxLength = Math.max(_maxLength, values[i].length);
      }
    }
    ByteArray[] wrappedValues = new ByteArray[length];
    for (int i = 0; i < length; i++) {
      wrappedValues[i] = values[i] != null ? new ByteArray(values[i]) : null;
    }
    addValues(wrappedValues, length);
  }

  @Override
  public void add(Object value, int dictId)
      throws IOException {
    if (value == null) {
      _nextDocId++;
      return;
    }
    switch (_dataType.getStoredType()) {
      case INT:
        add((Integer) value);
        break;
      case LONG:
        add((Long) value);
        break;
      case FLOAT:
        add((Float) value);
        break;
      case DOUBLE:
        add((Double) value);
        break;
      case STRING:
        add((String) value);
        break;
      case BYTES:
        if (_dataType == DataType.UUID) {
          add(UuidUtils.toBytes(value));
        } else if (value instanceof ByteArray) {
          add(((ByteArray) value).getBytes());
        } else {
          add((byte[]) value);
        }
        break;
      default:
        throw new IllegalStateException("Unsupported data type for raw inverted index: " + _dataType);
    }
  }

  @Override
  public void add(Object[] values, @Nullable int[] dictIds)
      throws IOException {
    switch (_dataType.getStoredType()) {
      case INT:
        int[] intValues = new int[values.length];
        for (int i = 0; i < values.length; i++) {
          intValues[i] = (Integer) values[i];
        }
        add(intValues, values.length);
        break;
      case LONG:
        long[] longValues = new long[values.length];
        for (int i = 0; i < values.length; i++) {
          longValues[i] = (Long) values[i];
        }
        add(longValues, values.length);
        break;
      case FLOAT:
        float[] floatValues = new float[values.length];
        for (int i = 0; i < values.length; i++) {
          floatValues[i] = (Float) values[i];
        }
        add(floatValues, values.length);
        break;
      case DOUBLE:
        double[] doubleValues = new double[values.length];
        for (int i = 0; i < values.length; i++) {
          doubleValues[i] = (Double) values[i];
        }
        add(doubleValues, values.length);
        break;
      case STRING:
        String[] stringValues = new String[values.length];
        for (int i = 0; i < values.length; i++) {
          stringValues[i] = (String) values[i];
        }
        add(stringValues, values.length);
        break;
      case BYTES:
        byte[][] bytesValues = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
          Object value = values[i];
          if (value == null) {
            bytesValues[i] = null;
          } else if (_dataType == DataType.UUID) {
            bytesValues[i] = UuidUtils.toBytes(value);
          } else if (value instanceof ByteArray) {
            bytesValues[i] = ((ByteArray) value).getBytes();
          } else {
            bytesValues[i] = (byte[]) value;
          }
        }
        add(bytesValues, values.length);
        break;
      default:
        throw new IllegalStateException("Unsupported data type for raw inverted index: " + _dataType);
    }
  }

  private void addValue(Object value) {
    if (value != null) {
      _valueToDocIds.computeIfAbsent(value, k -> new MutableRoaringBitmap()).add(_nextDocId);
    }
    _nextDocId++;
  }

  private void addValues(Object[] values, int length) {
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        _valueToDocIds.computeIfAbsent(values[i], k -> new MutableRoaringBitmap()).add(_nextDocId);
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

    File temporaryDictionaryFile = File.createTempFile(_columnName + ".raw-inv-dict-", ".tmp",
        _indexFile.getParentFile());
    try {
      // Create dictionary from unique values.
      // This dictionary is only embedded inside the raw-value bitmap index and must not be persisted as a segment
      // dictionary file, otherwise later reloads will mis-detect the column as dictionary-encoded.
      boolean useVarLengthDictionary = _dataType.getStoredType() == DataType.STRING;
      _dictionaryCreator =
          new SegmentDictionaryCreator(_columnName, _dataType.getStoredType(), temporaryDictionaryFile,
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
          // Values are stored as ByteArray in the map (add(byte[]) always wraps via new ByteArray()).
          // This holds for UUID columns too: both add(Object, int) and add(Object[], int[]) route UUID
          // values through add(byte[]) which performs the wrapping.
          ByteArray[] bytesValues = new ByteArray[numValues];
          for (int i = 0; i < numValues; i++) {
            bytesValues[i] = (ByteArray) rawValues[i];
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
      try (PinotDataBuffer buffer =
          PinotDataBuffer.mapFile(_indexFile, false, 0, HEADER_LENGTH, ByteOrder.BIG_ENDIAN,
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
    } finally {
      FileUtils.deleteQuietly(temporaryDictionaryFile);
    }
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
