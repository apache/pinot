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
package org.apache.pinot.core.segment.creator.impl;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.core.io.util.VarLengthValueWriter;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentDictionaryCreator implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDictionaryCreator.class);

  private final Object _sortedValues;
  private final FieldSpec _fieldSpec;
  private final File _dictionaryFile;
  private final boolean _useVarLengthDictionary;

  private Int2IntOpenHashMap _intValueToIndexMap;
  private Long2IntOpenHashMap _longValueToIndexMap;
  private Float2IntOpenHashMap _floatValueToIndexMap;
  private Double2IntOpenHashMap _doubleValueToIndexMap;
  private Object2IntOpenHashMap<String> _stringValueToIndexMap;
  private Object2IntOpenHashMap<ByteArray> _bytesValueToIndexMap;
  private int _numBytesPerEntry = 0;

  public SegmentDictionaryCreator(Object sortedValues, FieldSpec fieldSpec, File indexDir,
      boolean useVarLengthDictionary)
      throws IOException {
    _sortedValues = sortedValues;
    _fieldSpec = fieldSpec;
    _dictionaryFile = new File(indexDir, fieldSpec.getName() + V1Constants.Dict.FILE_EXTENSION);
    FileUtils.touch(_dictionaryFile);
    _useVarLengthDictionary = useVarLengthDictionary;
  }

  public SegmentDictionaryCreator(Object sortedValues, FieldSpec fieldSpec, File indexDir)
      throws IOException {
    this(sortedValues, fieldSpec, indexDir, false);
  }

  public void build()
      throws IOException {
    switch (_fieldSpec.getDataType()) {
      case INT:
        int[] sortedInts = (int[]) _sortedValues;
        int numValues = sortedInts.length;
        Preconditions.checkState(numValues > 0);
        _intValueToIndexMap = new Int2IntOpenHashMap(numValues);

        // Backward-compatible: index file is always big-endian
        try (PinotDataBuffer dataBuffer = PinotDataBuffer
            .mapFile(_dictionaryFile, false, 0, (long) numValues * Integer.BYTES, ByteOrder.BIG_ENDIAN,
                getClass().getSimpleName());
            FixedByteValueReaderWriter writer = new FixedByteValueReaderWriter(dataBuffer)) {
          for (int i = 0; i < numValues; i++) {
            int value = sortedInts[i];
            _intValueToIndexMap.put(value, i);
            writer.writeInt(i, value);
          }
        }
        LOGGER.info("Created dictionary for INT column: {} with cardinality: {}, range: {} to {}", _fieldSpec.getName(),
            numValues, sortedInts[0], sortedInts[numValues - 1]);
        return;
      case LONG:
        long[] sortedLongs = (long[]) _sortedValues;
        numValues = sortedLongs.length;
        Preconditions.checkState(numValues > 0);
        _longValueToIndexMap = new Long2IntOpenHashMap(numValues);

        // Backward-compatible: index file is always big-endian
        try (PinotDataBuffer dataBuffer = PinotDataBuffer
            .mapFile(_dictionaryFile, false, 0, (long) numValues * Long.BYTES, ByteOrder.BIG_ENDIAN,
                getClass().getSimpleName());
            FixedByteValueReaderWriter writer = new FixedByteValueReaderWriter(dataBuffer)) {
          for (int i = 0; i < numValues; i++) {
            long value = sortedLongs[i];
            _longValueToIndexMap.put(value, i);
            writer.writeLong(i, value);
          }
        }
        LOGGER
            .info("Created dictionary for LONG column: {} with cardinality: {}, range: {} to {}", _fieldSpec.getName(),
                numValues, sortedLongs[0], sortedLongs[numValues - 1]);
        return;
      case FLOAT:
        float[] sortedFloats = (float[]) _sortedValues;
        numValues = sortedFloats.length;
        Preconditions.checkState(numValues > 0);
        _floatValueToIndexMap = new Float2IntOpenHashMap(numValues);

        // Backward-compatible: index file is always big-endian
        try (PinotDataBuffer dataBuffer = PinotDataBuffer
            .mapFile(_dictionaryFile, false, 0, (long) numValues * Float.BYTES, ByteOrder.BIG_ENDIAN,
                getClass().getSimpleName());
            FixedByteValueReaderWriter writer = new FixedByteValueReaderWriter(dataBuffer)) {
          for (int i = 0; i < numValues; i++) {
            float value = sortedFloats[i];
            _floatValueToIndexMap.put(value, i);
            writer.writeFloat(i, value);
          }
        }
        LOGGER
            .info("Created dictionary for FLOAT column: {} with cardinality: {}, range: {} to {}", _fieldSpec.getName(),
                numValues, sortedFloats[0], sortedFloats[numValues - 1]);
        return;
      case DOUBLE:
        double[] sortedDoubles = (double[]) _sortedValues;
        numValues = sortedDoubles.length;
        Preconditions.checkState(numValues > 0);
        _doubleValueToIndexMap = new Double2IntOpenHashMap(numValues);

        // Backward-compatible: index file is always big-endian
        try (PinotDataBuffer dataBuffer = PinotDataBuffer
            .mapFile(_dictionaryFile, false, 0, (long) numValues * Double.BYTES, ByteOrder.BIG_ENDIAN,
                getClass().getSimpleName());
            FixedByteValueReaderWriter writer = new FixedByteValueReaderWriter(dataBuffer)) {
          for (int i = 0; i < numValues; i++) {
            double value = sortedDoubles[i];
            _doubleValueToIndexMap.put(value, i);
            writer.writeDouble(i, value);
          }
        }
        LOGGER.info("Created dictionary for DOUBLE column: {} with cardinality: {}, range: {} to {}",
            _fieldSpec.getName(), numValues, sortedDoubles[0], sortedDoubles[numValues - 1]);
        return;
      case STRING:
        String[] sortedStrings = (String[]) _sortedValues;
        numValues = sortedStrings.length;
        Preconditions.checkState(numValues > 0);
        _stringValueToIndexMap = new Object2IntOpenHashMap<>(numValues);

        // Get the maximum length of all entries
        byte[][] sortedStringBytes = new byte[numValues][];
        for (int i = 0; i < numValues; i++) {
          String value = sortedStrings[i];
          _stringValueToIndexMap.put(value, i);
          byte[] valueBytes = StringUtil.encodeUtf8(value);
          sortedStringBytes[i] = valueBytes;
          _numBytesPerEntry = Math.max(_numBytesPerEntry, valueBytes.length);
        }

        writeBytesValueDictionary(sortedStringBytes);
        LOGGER.info(
            "Created dictionary for STRING column: {} with cardinality: {}, max length in bytes: {}, range: {} to {}",
            _fieldSpec.getName(), numValues, _numBytesPerEntry, sortedStrings[0], sortedStrings[numValues - 1]);
        return;

      case BYTES:
        ByteArray[] sortedBytes = (ByteArray[]) _sortedValues;
        numValues = sortedBytes.length;

        Preconditions.checkState(numValues > 0);
        _bytesValueToIndexMap = new Object2IntOpenHashMap<>(numValues);

        byte[][] sortedByteArrays = new byte[sortedBytes.length][];
        for (int i = 0; i < numValues; i++) {
          ByteArray value = sortedBytes[i];
          sortedByteArrays[i] = value.getBytes();
          _bytesValueToIndexMap.put(value, i);
          _numBytesPerEntry = Math.max(_numBytesPerEntry, value.getBytes().length);
        }

        writeBytesValueDictionary(sortedByteArrays);
        LOGGER.info(
            "Created dictionary for BYTES column: {} with cardinality: {}, max length in bytes: {}, range: {} to {}",
            _fieldSpec.getName(), numValues, _numBytesPerEntry, sortedBytes[0], sortedBytes[numValues - 1]);
        return;

      default:
        throw new UnsupportedOperationException("Unsupported data type: " + _fieldSpec.getDataType());
    }
  }

  /**
   * Helper method to write the given sorted byte[][] to an immutable bytes value dictionary.
   * The dictionary implementation is chosen based on configuration at column level.
   *
   * @param bytesValues The actual sorted byte arrays to be written to the store.
   */
  private void writeBytesValueDictionary(byte[][] bytesValues)
      throws IOException {
    if (_useVarLengthDictionary) {
      try (VarLengthValueWriter writer = new VarLengthValueWriter(_dictionaryFile, bytesValues.length)) {
        for (byte[] value : bytesValues) {
          writer.add(value);
        }
      }
      LOGGER.info("Using variable length dictionary for column: {}, size: {}", _fieldSpec.getName(),
          _dictionaryFile.length());
    } else {
      // Backward-compatible: index file is always big-endian
      int numValues = bytesValues.length;
      try (PinotDataBuffer dataBuffer = PinotDataBuffer
          .mapFile(_dictionaryFile, false, 0, (long) numValues * _numBytesPerEntry, ByteOrder.BIG_ENDIAN,
              getClass().getSimpleName());
          FixedByteValueReaderWriter writer = new FixedByteValueReaderWriter(dataBuffer)) {
        for (int i = 0; i < bytesValues.length; i++) {
          writer.writeBytes(i, _numBytesPerEntry, bytesValues[i]);
        }
      }
      LOGGER.info("Using fixed length dictionary for column: {}, size: {}", _fieldSpec.getName(),
          (long) numValues * _numBytesPerEntry);
    }
  }

  public int getNumBytesPerEntry() {
    return _numBytesPerEntry;
  }

  public int indexOfSV(Object value) {
    switch (_fieldSpec.getDataType()) {
      case INT:
        return _intValueToIndexMap.get((int) value);
      case LONG:
        return _longValueToIndexMap.get((long) value);
      case FLOAT:
        return _floatValueToIndexMap.get((float) value);
      case DOUBLE:
        return _doubleValueToIndexMap.get((double) value);
      case STRING:
        return _stringValueToIndexMap.getInt(value);
      case BYTES:
        return _bytesValueToIndexMap.get(new ByteArray((byte[]) value));
      default:
        throw new UnsupportedOperationException("Unsupported data type : " + _fieldSpec.getDataType());
    }
  }

  public int[] indexOfMV(Object value) {
    Object[] multiValues = (Object[]) value;
    int[] indexes = new int[multiValues.length];

    switch (_fieldSpec.getDataType()) {
      case INT:
        for (int i = 0; i < multiValues.length; i++) {
          indexes[i] = _intValueToIndexMap.get((int) multiValues[i]);
        }
        break;
      case LONG:
        for (int i = 0; i < multiValues.length; i++) {
          indexes[i] = _longValueToIndexMap.get((long) multiValues[i]);
        }
        break;
      case FLOAT:
        for (int i = 0; i < multiValues.length; i++) {
          indexes[i] = _floatValueToIndexMap.get((float) multiValues[i]);
        }
        break;
      case DOUBLE:
        for (int i = 0; i < multiValues.length; i++) {
          indexes[i] = _doubleValueToIndexMap.get((double) multiValues[i]);
        }
        break;
      case STRING:
        for (int i = 0; i < multiValues.length; i++) {
          indexes[i] = _stringValueToIndexMap.getInt(multiValues[i]);
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported data type : " + _fieldSpec.getDataType());
    }
    return indexes;
  }

  @Override
  public void close() {
  }
}
