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
import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteOrder;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.segment.local.io.util.VarLengthValueWriter;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The IndexCreator for dictionaries.
 *
 * Although this class implements {@link IndexCreator}, it is not intended to be used as a normal IndexCreator.
 * Specifically, neither {@link #add(Object, int)} or {@link #add(Object[], int[])} should be called on this object.
 * In order to make sure these methods are not being called, they throw exceptions in this class.
 *
 * This requirement is a corollary from the fact that the {@link IndexCreator} contract assumes the dictionary id can be
 * calculated before calling {@code add} methods.
 */
public class SegmentDictionaryCreator implements IndexCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDictionaryCreator.class);

  private final String _columnName;
  private final DataType _storedType;
  private final File _dictionaryFile;
  private final boolean _useVarLengthDictionary;

  private Int2IntOpenHashMap _intValueToIndexMap;
  private Long2IntOpenHashMap _longValueToIndexMap;
  private Float2IntOpenHashMap _floatValueToIndexMap;
  private Double2IntOpenHashMap _doubleValueToIndexMap;
  private Object2IntOpenHashMap<Object> _objectValueToIndexMap;
  private int _numBytesPerEntry = 0;

  public SegmentDictionaryCreator(FieldSpec fieldSpec, File indexDir, boolean useVarLengthDictionary) {
    _columnName = fieldSpec.getName();
    _storedType = fieldSpec.getDataType().getStoredType();
    _dictionaryFile = new File(indexDir, _columnName + DictionaryIndexType.getFileExtension());
    _useVarLengthDictionary = useVarLengthDictionary;
  }
  @Override
  public void add(@Nonnull Object value, int dictId)
      throws IOException {
    throw new UnsupportedOperationException("Dictionaries should not be built as a normal index");
  }

  @Override
  public void add(@Nonnull Object[] values, @Nullable int[] dictIds)
      throws IOException {
    throw new UnsupportedOperationException("Dictionaries should not be built as a normal index");
  }

  public SegmentDictionaryCreator(FieldSpec fieldSpec, File indexDir) {
    this(fieldSpec, indexDir, false);
  }

  public void build(Object sortedValues)
      throws IOException {
    FileUtils.touch(_dictionaryFile);

    switch (_storedType) {
      case INT:
        int[] sortedInts = (int[]) sortedValues;
        int numValues = sortedInts.length;
        Preconditions.checkState(numValues > 0);
        _intValueToIndexMap = new Int2IntOpenHashMap(numValues);

        // Backward-compatible: index file is always big-endian
        try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapFile(_dictionaryFile, false, 0,
            (long) numValues * Integer.BYTES, ByteOrder.BIG_ENDIAN, getClass().getSimpleName());
            FixedByteValueReaderWriter writer = new FixedByteValueReaderWriter(dataBuffer)) {
          for (int i = 0; i < numValues; i++) {
            int value = sortedInts[i];
            _intValueToIndexMap.put(value, i);
            writer.writeInt(i, value);
          }
        }
        LOGGER.info("Created dictionary for INT column: {} with cardinality: {}, range: {} to {}", _columnName,
            numValues, sortedInts[0], sortedInts[numValues - 1]);
        return;

      case LONG:
        long[] sortedLongs = (long[]) sortedValues;
        numValues = sortedLongs.length;
        Preconditions.checkState(numValues > 0);
        _longValueToIndexMap = new Long2IntOpenHashMap(numValues);

        // Backward-compatible: index file is always big-endian
        try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapFile(_dictionaryFile, false, 0,
            (long) numValues * Long.BYTES, ByteOrder.BIG_ENDIAN, getClass().getSimpleName());
            FixedByteValueReaderWriter writer = new FixedByteValueReaderWriter(dataBuffer)) {
          for (int i = 0; i < numValues; i++) {
            long value = sortedLongs[i];
            _longValueToIndexMap.put(value, i);
            writer.writeLong(i, value);
          }
        }
        LOGGER.info("Created dictionary for LONG column: {} with cardinality: {}, range: {} to {}", _columnName,
            numValues, sortedLongs[0], sortedLongs[numValues - 1]);
        return;

      case FLOAT:
        float[] sortedFloats = (float[]) sortedValues;
        numValues = sortedFloats.length;
        Preconditions.checkState(numValues > 0);
        _floatValueToIndexMap = new Float2IntOpenHashMap(numValues);

        // Backward-compatible: index file is always big-endian
        try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapFile(_dictionaryFile, false, 0,
            (long) numValues * Float.BYTES, ByteOrder.BIG_ENDIAN, getClass().getSimpleName());
            FixedByteValueReaderWriter writer = new FixedByteValueReaderWriter(dataBuffer)) {
          for (int i = 0; i < numValues; i++) {
            float value = sortedFloats[i];
            _floatValueToIndexMap.put(value, i);
            writer.writeFloat(i, value);
          }
        }
        LOGGER.info("Created dictionary for FLOAT column: {} with cardinality: {}, range: {} to {}", _columnName,
            numValues, sortedFloats[0], sortedFloats[numValues - 1]);
        return;

      case DOUBLE:
        double[] sortedDoubles = (double[]) sortedValues;
        numValues = sortedDoubles.length;
        Preconditions.checkState(numValues > 0);
        _doubleValueToIndexMap = new Double2IntOpenHashMap(numValues);

        // Backward-compatible: index file is always big-endian
        try (PinotDataBuffer dataBuffer = PinotDataBuffer.mapFile(_dictionaryFile, false, 0,
            (long) numValues * Double.BYTES, ByteOrder.BIG_ENDIAN, getClass().getSimpleName());
            FixedByteValueReaderWriter writer = new FixedByteValueReaderWriter(dataBuffer)) {
          for (int i = 0; i < numValues; i++) {
            double value = sortedDoubles[i];
            _doubleValueToIndexMap.put(value, i);
            writer.writeDouble(i, value);
          }
        }
        LOGGER.info("Created dictionary for DOUBLE column: {} with cardinality: {}, range: {} to {}", _columnName,
            numValues, sortedDoubles[0], sortedDoubles[numValues - 1]);
        return;

      case BIG_DECIMAL:
        BigDecimal[] sortedBigDecimals = (BigDecimal[]) sortedValues;
        numValues = sortedBigDecimals.length;
        Preconditions.checkState(numValues > 0);
        _objectValueToIndexMap = new Object2IntOpenHashMap<>(numValues);

        // Get the maximum length of all entries
        byte[][] sortedBigDecimalBytes = new byte[numValues][];
        for (int i = 0; i < numValues; i++) {
          BigDecimal value = sortedBigDecimals[i];
          _objectValueToIndexMap.put(value, i);
          byte[] valueBytes = BigDecimalUtils.serialize(value);
          sortedBigDecimalBytes[i] = valueBytes;
          _numBytesPerEntry = Math.max(_numBytesPerEntry, valueBytes.length);
        }

        writeBytesValueDictionary(sortedBigDecimalBytes);
        LOGGER.info("Created dictionary for BIG_DECIMAL column: {} with cardinality: {}, max length in bytes: {}, "
                + "range: {} to {}", _columnName, numValues, _numBytesPerEntry, sortedBigDecimals[0],
            sortedBigDecimals[numValues - 1]);
        return;

      case STRING:
        String[] sortedStrings = (String[]) sortedValues;
        numValues = sortedStrings.length;
        Preconditions.checkState(numValues > 0);
        _objectValueToIndexMap = new Object2IntOpenHashMap<>(numValues);

        // Get the maximum length of all entries
        byte[][] sortedStringBytes = new byte[numValues][];
        for (int i = 0; i < numValues; i++) {
          String value = sortedStrings[i];
          _objectValueToIndexMap.put(value, i);
          byte[] valueBytes = value.getBytes(UTF_8);
          sortedStringBytes[i] = valueBytes;
          _numBytesPerEntry = Math.max(_numBytesPerEntry, valueBytes.length);
        }

        writeBytesValueDictionary(sortedStringBytes);
        LOGGER.info(
            "Created dictionary for STRING column: {} with cardinality: {}, max length in bytes: {}, range: {} to {}",
            _columnName, numValues, _numBytesPerEntry, sortedStrings[0], sortedStrings[numValues - 1]);
        return;

      case BYTES:
        ByteArray[] sortedBytes = (ByteArray[]) sortedValues;
        numValues = sortedBytes.length;
        Preconditions.checkState(numValues > 0);
        _objectValueToIndexMap = new Object2IntOpenHashMap<>(numValues);

        // Get the maximum length of all entries
        byte[][] sortedByteArrays = new byte[numValues][];
        for (int i = 0; i < numValues; i++) {
          ByteArray value = sortedBytes[i];
          sortedByteArrays[i] = value.getBytes();
          _objectValueToIndexMap.put(value, i);
          _numBytesPerEntry = Math.max(_numBytesPerEntry, value.getBytes().length);
        }

        writeBytesValueDictionary(sortedByteArrays);
        LOGGER.info(
            "Created dictionary for BYTES column: {} with cardinality: {}, max length in bytes: {}, range: {} to {}",
            _columnName, numValues, _numBytesPerEntry, sortedBytes[0], sortedBytes[numValues - 1]);
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

  public int getNumBytesPerEntry() {
    return _numBytesPerEntry;
  }

  public int indexOfSV(Object value) {
    switch (_storedType) {
      case INT:
        return _intValueToIndexMap.get((int) value);
      case LONG:
        return _longValueToIndexMap.get((long) value);
      case FLOAT:
        return _floatValueToIndexMap.get((float) value);
      case DOUBLE:
        return _doubleValueToIndexMap.get((double) value);
      case STRING:
      case BIG_DECIMAL:
        return _objectValueToIndexMap.getInt(value);
      case BYTES:
        return _objectValueToIndexMap.getInt(new ByteArray((byte[]) value));
      default:
        throw new UnsupportedOperationException("Unsupported data type : " + _storedType);
    }
  }

  public int[] indexOfMV(Object value) {
    Object[] multiValues = (Object[]) value;
    int[] indexes = new int[multiValues.length];

    switch (_storedType) {
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
          indexes[i] = _objectValueToIndexMap.getInt(multiValues[i]);
        }
        break;
      case BYTES:
        for (int i = 0; i < multiValues.length; i++) {
          indexes[i] = _objectValueToIndexMap.getInt(new ByteArray((byte[]) multiValues[i]));
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported data type : " + _storedType);
    }
    return indexes;
  }

  /**
   * Cleans up the no longer needed objects after all the indexing is done to free up some memory.
   */
  public void postIndexingCleanup() {
    _intValueToIndexMap = null;
    _longValueToIndexMap = null;
    _floatValueToIndexMap = null;
    _doubleValueToIndexMap = null;
    _objectValueToIndexMap = null;
  }

  @Override
  public void seal() {
    postIndexingCleanup();
  }

  @Override
  public void close() {
  }
}
