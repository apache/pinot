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
package org.apache.pinot.segment.local.segment.creator.impl;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.segment.local.io.util.VarLengthValueWriter;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;


public class SegmentDictionaryCreator implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDictionaryCreator.class);

  private final String _columnName;
  private final DataType _storedType;
  private final File _dictionaryFile;
  private final boolean _useVarLengthDictionary;

  private int[] _sortedInts;
  private long[] _sortedLongs;
  private float[] _sortedFloats;
  private double[] _sortedDoubles;
  private Object[] _sortedObjects;
  private int _numBytesPerEntry = 0;

  public SegmentDictionaryCreator(FieldSpec fieldSpec, File indexDir, boolean useVarLengthDictionary) {
    _columnName = fieldSpec.getName();
    _storedType = fieldSpec.getDataType().getStoredType();
    _dictionaryFile = new File(indexDir, _columnName + V1Constants.Dict.FILE_EXTENSION);
    _useVarLengthDictionary = useVarLengthDictionary;
  }

  public SegmentDictionaryCreator(FieldSpec fieldSpec, File indexDir) {
    this(fieldSpec, indexDir, false);
  }

  public void build(Object sortedValues)
      throws IOException {
    FileUtils.touch(_dictionaryFile);

    switch (_storedType) {
      case INT:
        _sortedInts = (int[]) sortedValues;
        int numValues = _sortedInts.length;
        Preconditions.checkState(numValues > 0);

        // Backward-compatible: index file is always big-endian
        try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapFile(_dictionaryFile, false, 0,
            (long) numValues * Integer.BYTES, ByteOrder.BIG_ENDIAN, getClass().getSimpleName());
            FixedByteValueReaderWriter writer = new FixedByteValueReaderWriter(dataBuffer)) {
          for (int i = 0; i < numValues; i++) {
            writer.writeInt(i, _sortedInts[i]);
          }
        }
        LOGGER.info("Created dictionary for INT column: {} with cardinality: {}, range: {} to {}", _columnName,
            numValues, _sortedInts[0], _sortedInts[numValues - 1]);
        return;

      case LONG:
        _sortedLongs = (long[]) sortedValues;
        numValues = _sortedLongs.length;
        Preconditions.checkState(numValues > 0);

        // Backward-compatible: index file is always big-endian
        try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapFile(_dictionaryFile, false, 0,
            (long) numValues * Long.BYTES, ByteOrder.BIG_ENDIAN, getClass().getSimpleName());
            FixedByteValueReaderWriter writer = new FixedByteValueReaderWriter(dataBuffer)) {
          for (int i = 0; i < numValues; i++) {
            writer.writeLong(i, _sortedLongs[i]);
          }
        }
        LOGGER.info("Created dictionary for LONG column: {} with cardinality: {}, range: {} to {}", _columnName,
            numValues, _sortedLongs[0], _sortedLongs[numValues - 1]);
        return;

      case FLOAT:
        _sortedFloats = (float[]) sortedValues;
        numValues = _sortedFloats.length;
        Preconditions.checkState(numValues > 0);

        // Backward-compatible: index file is always big-endian
        try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapFile(_dictionaryFile, false, 0,
            (long) numValues * Float.BYTES, ByteOrder.BIG_ENDIAN, getClass().getSimpleName());
            FixedByteValueReaderWriter writer = new FixedByteValueReaderWriter(dataBuffer)) {
          for (int i = 0; i < numValues; i++) {
            writer.writeFloat(i, _sortedFloats[i]);
          }
        }
        LOGGER.info("Created dictionary for FLOAT column: {} with cardinality: {}, range: {} to {}", _columnName,
            numValues, _sortedFloats[0], _sortedFloats[numValues - 1]);
        return;

      case DOUBLE:
        _sortedDoubles = (double[]) sortedValues;
        numValues = _sortedDoubles.length;
        Preconditions.checkState(numValues > 0);

        // Backward-compatible: index file is always big-endian
        try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapFile(_dictionaryFile, false, 0,
            (long) numValues * Double.BYTES, ByteOrder.BIG_ENDIAN, getClass().getSimpleName());
            FixedByteValueReaderWriter writer = new FixedByteValueReaderWriter(dataBuffer)) {
          for (int i = 0; i < numValues; i++) {
            writer.writeDouble(i, _sortedDoubles[i]);
          }
        }
        LOGGER.info("Created dictionary for DOUBLE column: {} with cardinality: {}, range: {} to {}", _columnName,
            numValues, _sortedDoubles[0], _sortedDoubles[numValues - 1]);
        return;

      case BIG_DECIMAL:
        BigDecimal[] sortedBigDecimals = (BigDecimal[]) sortedValues;
        _sortedObjects = sortedBigDecimals;
        numValues = sortedBigDecimals.length;
        Preconditions.checkState(numValues > 0);

        for (int i = 0; i < numValues; i++) {
          _numBytesPerEntry = Math.max(_numBytesPerEntry, BigDecimalUtils.byteSize(sortedBigDecimals[i]));
        }

        writeBytesValueDictionary(sortedBigDecimals);
        LOGGER.info("Created dictionary for BIG_DECIMAL column: {}"
                + " with cardinality: {}, max length in bytes: {}, range: {} to {}", _columnName, numValues,
            _numBytesPerEntry, sortedBigDecimals[0], sortedBigDecimals[numValues - 1]);
        return;

      case STRING:
        String[] sortedStrings = (String[]) sortedValues;
        _sortedObjects = sortedStrings;
        numValues = sortedStrings.length;
        Preconditions.checkState(numValues > 0);

        // Get the maximum length of all entries
        byte[][] sortedStringBytes = new byte[numValues][];
        for (int i = 0; i < numValues; i++) {
          byte[] valueBytes = sortedStrings[i].getBytes(UTF_8);
          sortedStringBytes[i] = valueBytes;
          _numBytesPerEntry = Math.max(_numBytesPerEntry, valueBytes.length);
        }

        writeBytesValueDictionary(sortedStringBytes);
        LOGGER.info(
            "Created dictionary for STRING column: {} with cardinality: {}, max length in bytes: {}, range: {} to {}",
            _columnName, numValues, _numBytesPerEntry, sortedStrings[0], sortedStrings[numValues - 1]);
        return;

      case BYTES:
        ByteArray[] sortedByteArrays = (ByteArray[]) sortedValues;
        _sortedObjects = sortedByteArrays;
        numValues = sortedByteArrays.length;
        Preconditions.checkState(numValues > 0);

        byte[][] sortedBytes = new byte[numValues][];
        for (int i = 0; i < numValues; i++) {
          byte[] valueBytes = sortedByteArrays[i].getBytes();
          sortedBytes[i] = valueBytes;
          _numBytesPerEntry = Math.max(_numBytesPerEntry, valueBytes.length);
        }

        writeBytesValueDictionary(sortedBytes);
        LOGGER.info(
            "Created dictionary for BYTES column: {} with cardinality: {}, max length in bytes: {}, range: {} to {}",
            _columnName, numValues, _numBytesPerEntry, sortedByteArrays[0], sortedByteArrays[numValues - 1]);
        return;

      default:
        throw new UnsupportedOperationException("Unsupported data type: " + _storedType);
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
      LOGGER.info("Using variable length dictionary for column: {}, size: {}", _columnName, _dictionaryFile.length());
    } else {
      // Backward-compatible: index file is always big-endian
      int numValues = bytesValues.length;
      try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapFile(_dictionaryFile, false, 0,
          (long) numValues * _numBytesPerEntry, ByteOrder.BIG_ENDIAN, getClass().getSimpleName());
          FixedByteValueReaderWriter writer = new FixedByteValueReaderWriter(dataBuffer)) {
        for (int i = 0; i < bytesValues.length; i++) {
          writer.writeBytes(i, _numBytesPerEntry, bytesValues[i]);
        }
      }
      LOGGER.info("Using fixed length dictionary for column: {}, size: {}", _columnName,
          (long) numValues * _numBytesPerEntry);
    }
  }

  private void writeBytesValueDictionary(BigDecimal[] bigDecimalValues)
      throws IOException {
    if (_useVarLengthDictionary) {
      try (VarLengthValueWriter writer = new VarLengthValueWriter(_dictionaryFile, bigDecimalValues.length)) {
        for (BigDecimal value : bigDecimalValues) {
          writer.add(BigDecimalUtils.serialize(value));
        }
      }
      LOGGER.info("Using variable length dictionary for column: {}, size: {}", _columnName, _dictionaryFile.length());
    } else {
      // Backward-compatible: index file is always big-endian
      int numValues = bigDecimalValues.length;
      try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapFile(_dictionaryFile, false, 0,
          (long) numValues * _numBytesPerEntry, ByteOrder.BIG_ENDIAN, getClass().getSimpleName());
          FixedByteValueReaderWriter writer = new FixedByteValueReaderWriter(dataBuffer)) {
        for (int i = 0; i < bigDecimalValues.length; i++) {
          writer.writeBytes(i, _numBytesPerEntry, BigDecimalUtils.serialize(bigDecimalValues[i]));
        }
      }
      LOGGER.info("Using fixed length dictionary for column: {}, size: {}", _columnName,
          (long) numValues * _numBytesPerEntry);
    }
  }

  public int getNumBytesPerEntry() {
    return _numBytesPerEntry;
  }

  public int indexOfSV(Object value) {
    switch (_storedType) {
      case INT:
        return indexOf((int) value);
      case LONG:
        return indexOf((long) value);
      case FLOAT:
        return indexOf((float) value);
      case DOUBLE:
        return indexOf((double) value);
      case BIG_DECIMAL:
      case STRING:
        return indexOf(value);
      case BYTES:
        return indexOf(new ByteArray((byte[]) value));
      default:
        throw new UnsupportedOperationException("Unsupported SV data type: " + _storedType);
    }
  }

  public int[] indexOfMV(Object value) {
    Object[] multiValues = (Object[]) value;
    int[] indexes = new int[multiValues.length];

    switch (_storedType) {
      case INT:
        for (int i = 0; i < multiValues.length; i++) {
          indexes[i] = indexOf((int) multiValues[i]);
        }
        break;
      case LONG:
        for (int i = 0; i < multiValues.length; i++) {
          indexes[i] = indexOf((long) multiValues[i]);
        }
        break;
      case FLOAT:
        for (int i = 0; i < multiValues.length; i++) {
          indexes[i] = indexOf((float) multiValues[i]);
        }
        break;
      case DOUBLE:
        for (int i = 0; i < multiValues.length; i++) {
          indexes[i] = indexOf((double) multiValues[i]);
        }
        break;
      case STRING:
        for (int i = 0; i < multiValues.length; i++) {
          indexes[i] = indexOf(multiValues[i]);
        }
        break;
      case BYTES:
        for (int i = 0; i < multiValues.length; i++) {
          indexes[i] = indexOf(new ByteArray((byte[]) multiValues[i]));
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported MV data type: " + _storedType);
    }
    return indexes;
  }

  private int indexOf(int value) {
    return Arrays.binarySearch(_sortedInts, value);
  }

  private int indexOf(long value) {
    return Arrays.binarySearch(_sortedLongs, value);
  }

  private int indexOf(float value) {
    return Arrays.binarySearch(_sortedFloats, value);
  }

  private int indexOf(double value) {
    return Arrays.binarySearch(_sortedDoubles, value);
  }

  private int indexOf(Object value) {
    return Arrays.binarySearch(_sortedObjects, value);
  }

  @Override
  public void close() {
  }
}
