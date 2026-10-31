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
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.segment.local.io.util.VarLengthValueWriter;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.Utf8Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

/// The IndexCreator for dictionaries.
///
/// Although this class implements [IndexCreator], it is not intended to be used as a normal IndexCreator.
/// Specifically, neither [#add(Object, int)] or [#add(Object[], int[])] should be called on this object.
/// In order to make sure these methods are not being called, they throw exceptions in this class.
///
/// This requirement is a corollary from the fact that the [IndexCreator] contract assumes the dictionary id can
/// be calculated before calling `add` methods.
public class SegmentDictionaryCreator implements IndexCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDictionaryCreator.class);
  private static final int MAX_CACHED_SERIALIZED_LENGTHS = 4 * 1024 * 1024 / Integer.BYTES;

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
  private final boolean _trackUncompressedValueBytes;
  // Cache lengths computed while serializing the dictionary, capped at 4 MiB per column to avoid cardinality-sized
  // allocations for very large dictionaries.
  @Nullable
  private int[] _serializedValueLengthByDictId;
  /// Accumulated uncompressed serialized column-value bytes across all rows indexed. Populated only when compression
  /// stats are enabled
  /// and for variable-length types (STRING, BYTES, BIG_DECIMAL); 0 for fixed-width types.
  private long _totalUncompressedValueSizeInBytes;

  /// Controls whether dictionary creation tracks uncompressed column-value bytes.
  public enum UncompressedValueSizeTracking {
    ENABLED,
    DISABLED;

    public static UncompressedValueSizeTracking fromEnabled(boolean enabled) {
      return enabled ? ENABLED : DISABLED;
    }
  }

  public SegmentDictionaryCreator(String columnName, DataType storedType, File indexFile,
      boolean useVarLengthDictionary) {
    this(columnName, storedType, indexFile, useVarLengthDictionary, UncompressedValueSizeTracking.DISABLED);
  }

  public SegmentDictionaryCreator(String columnName, DataType storedType, File indexFile,
      boolean useVarLengthDictionary, UncompressedValueSizeTracking tracking) {
    _columnName = columnName;
    _storedType = storedType;
    _dictionaryFile = indexFile;
    _useVarLengthDictionary = useVarLengthDictionary;
    _trackUncompressedValueBytes = tracking == UncompressedValueSizeTracking.ENABLED;
  }

  public SegmentDictionaryCreator(FieldSpec fieldSpec, File indexDir, boolean useVarLengthDictionary) {
    this(fieldSpec, indexDir, useVarLengthDictionary, UncompressedValueSizeTracking.DISABLED);
  }

  public SegmentDictionaryCreator(FieldSpec fieldSpec, File indexDir, boolean useVarLengthDictionary,
      UncompressedValueSizeTracking tracking) {
    _columnName = fieldSpec.getName();
    _storedType = fieldSpec.getDataType().getStoredType();
    _dictionaryFile = new File(indexDir, _columnName + DictionaryIndexType.getFileExtension());
    _useVarLengthDictionary = useVarLengthDictionary;
    _trackUncompressedValueBytes = tracking == UncompressedValueSizeTracking.ENABLED;
  }

  public SegmentDictionaryCreator(IndexCreationContext context, boolean useVarLengthDictionary) {
    this(context.getFieldSpec(), context.getIndexDir(), useVarLengthDictionary,
        UncompressedValueSizeTracking.fromEnabled(context.isCompressionStatsEnabled()));
  }

  @Override
  public void add(Object value, int dictId)
      throws IOException {
    throw new UnsupportedOperationException("Dictionaries should not be built as a normal index");
  }

  @Override
  public void add(Object[] values, @Nullable int[] dictIds)
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
        _intValueToIndexMap.defaultReturnValue(-1);

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
        LOGGER.info("Created dictionary for INT column: {} with cardinality: {}", _columnName, numValues);
        return;

      case LONG:
        long[] sortedLongs = (long[]) sortedValues;
        numValues = sortedLongs.length;
        Preconditions.checkState(numValues > 0);
        _longValueToIndexMap = new Long2IntOpenHashMap(numValues);
        _longValueToIndexMap.defaultReturnValue(-1);

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
        LOGGER.info("Created dictionary for LONG column: {} with cardinality: {}", _columnName, numValues);
        return;

      case FLOAT:
        float[] sortedFloats = (float[]) sortedValues;
        numValues = sortedFloats.length;
        Preconditions.checkState(numValues > 0);
        _floatValueToIndexMap = new Float2IntOpenHashMap(numValues);
        _floatValueToIndexMap.defaultReturnValue(-1);

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
        LOGGER.info("Created dictionary for FLOAT column: {} with cardinality: {}", _columnName, numValues);
        return;

      case DOUBLE:
        double[] sortedDoubles = (double[]) sortedValues;
        numValues = sortedDoubles.length;
        Preconditions.checkState(numValues > 0);
        _doubleValueToIndexMap = new Double2IntOpenHashMap(numValues);
        _doubleValueToIndexMap.defaultReturnValue(-1);

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
        LOGGER.info("Created dictionary for DOUBLE column: {} with cardinality: {}", _columnName, numValues);
        return;

      case BIG_DECIMAL:
        BigDecimal[] sortedBigDecimals = (BigDecimal[]) sortedValues;
        numValues = sortedBigDecimals.length;
        Preconditions.checkState(numValues > 0);
        _objectValueToIndexMap = new Object2IntOpenHashMap<>(numValues);
        _objectValueToIndexMap.defaultReturnValue(-1);
        initializeSerializedLengthCache(numValues);

        // Get the maximum length of all entries
        byte[][] sortedBigDecimalBytes = new byte[numValues][];
        for (int i = 0; i < numValues; i++) {
          BigDecimal value = sortedBigDecimals[i];
          _objectValueToIndexMap.put(value, i);
          byte[] valueBytes = BigDecimalUtils.serialize(value);
          sortedBigDecimalBytes[i] = valueBytes;
          cacheSerializedLength(i, valueBytes.length);
          _numBytesPerEntry = Math.max(_numBytesPerEntry, valueBytes.length);
        }

        writeBytesValueDictionary(sortedBigDecimalBytes);
        LOGGER.info("Created dictionary for BIG_DECIMAL column: {} with cardinality: {}, max length in bytes: {}",
            _columnName, numValues, _numBytesPerEntry);
        return;

      case STRING:
        String[] sortedStrings = (String[]) sortedValues;
        numValues = sortedStrings.length;
        Preconditions.checkState(numValues > 0);
        _objectValueToIndexMap = new Object2IntOpenHashMap<>(numValues);
        _objectValueToIndexMap.defaultReturnValue(-1);
        initializeSerializedLengthCache(numValues);

        // Get the maximum length of all entries
        byte[][] sortedStringBytes = new byte[numValues][];
        for (int i = 0; i < numValues; i++) {
          String value = sortedStrings[i];
          _objectValueToIndexMap.put(value, i);
          byte[] valueBytes = value.getBytes(UTF_8);
          sortedStringBytes[i] = valueBytes;
          cacheSerializedLength(i, valueBytes.length);
          _numBytesPerEntry = Math.max(_numBytesPerEntry, valueBytes.length);
        }

        writeBytesValueDictionary(sortedStringBytes);
        LOGGER.info("Created dictionary for STRING column: {} with cardinality: {}, max length in bytes: {}",
            _columnName, numValues, _numBytesPerEntry);
        return;

      case BYTES:
        ByteArray[] sortedBytes = (ByteArray[]) sortedValues;
        numValues = sortedBytes.length;
        Preconditions.checkState(numValues > 0);
        _objectValueToIndexMap = new Object2IntOpenHashMap<>(numValues);
        _objectValueToIndexMap.defaultReturnValue(-1);
        initializeSerializedLengthCache(numValues);

        // Get the maximum length of all entries
        byte[][] sortedByteArrays = new byte[numValues][];
        for (int i = 0; i < numValues; i++) {
          ByteArray value = sortedBytes[i];
          sortedByteArrays[i] = value.getBytes();
          _objectValueToIndexMap.put(value, i);
          cacheSerializedLength(i, value.getBytes().length);
          _numBytesPerEntry = Math.max(_numBytesPerEntry, value.getBytes().length);
        }

        writeBytesValueDictionary(sortedByteArrays);
        LOGGER.info("Created dictionary for BYTES column: {} with cardinality: {}, max length in bytes: {}",
            _columnName, numValues, _numBytesPerEntry);
        return;

      default:
        throw new UnsupportedOperationException("Unsupported data type: " + _storedType);
    }
  }

  /// Only call after build(). Used by StarTree json index.
  @SuppressWarnings({"unused"})
  public Map getValueToIndexMap() {
    switch (_storedType) {
      case INT:
        return _intValueToIndexMap;
      case LONG:
        return _longValueToIndexMap;
      case FLOAT:
        return _floatValueToIndexMap;
      case DOUBLE:
        return _doubleValueToIndexMap;
      case BIG_DECIMAL:
      case STRING:
      case BYTES:
        return _objectValueToIndexMap;
      default:
        throw new UnsupportedOperationException("Unsupported data type : " + _storedType);
    }
  }

  /// Helper method to write the given sorted byte\[\]\[\] to an immutable bytes value dictionary.
  /// The dictionary implementation is chosen based on configuration at column level.
  ///
  /// @param bytesValues The actual sorted byte arrays to be written to the store.
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

  /// Returns value bytes observed through variable-length dictionary lookups, or `0` when tracking is disabled.
  public long getTotalVariableLengthUncompressedValueSizeInBytes() {
    return _totalUncompressedValueSizeInBytes;
  }

  public int indexOfSV(Object value) {
    switch (_storedType) {
      case INT:
        return checkedDictId((int) value);
      case LONG:
        return checkedDictId((long) value);
      case FLOAT:
        return checkedDictId((float) value);
      case DOUBLE:
        return checkedDictId((double) value);
      case BIG_DECIMAL:
      case STRING:
        return indexOfVariableLengthValue(value);
      case BYTES:
        return indexOfVariableLengthValue(new ByteArray((byte[]) value));
      default:
        throw new UnsupportedOperationException("Unsupported data type : " + _storedType);
    }
  }

  /// Get dictionary index for a primitive int value without boxing.
  public int indexOfSV(int value) {
    return checkedDictId(value);
  }

  /// Get dictionary index for a primitive long value without boxing.
  public int indexOfSV(long value) {
    return checkedDictId(value);
  }

  /// Get dictionary index for a primitive float value without boxing.
  public int indexOfSV(float value) {
    return checkedDictId(value);
  }

  /// Get dictionary index for a primitive double value without boxing.
  public int indexOfSV(double value) {
    return checkedDictId(value);
  }

  /// Get dictionary index for a String value.
  public int indexOfSV(String value) {
    return indexOfVariableLengthValue(value);
  }

  /// Get dictionary index for a byte array value.
  public int indexOfSV(byte[] value) {
    return indexOfVariableLengthValue(new ByteArray(value));
  }

  public int[] indexOfMV(int[] values) {
    int[] indexes = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      indexes[i] = checkedDictId(values[i]);
    }
    return indexes;
  }

  public int[] indexOfMV(long[] values) {
    int[] indexes = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      indexes[i] = checkedDictId(values[i]);
    }
    return indexes;
  }

  public int[] indexOfMV(float[] values) {
    int[] indexes = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      indexes[i] = checkedDictId(values[i]);
    }
    return indexes;
  }

  public int[] indexOfMV(double[] values) {
    int[] indexes = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      indexes[i] = checkedDictId(values[i]);
    }
    return indexes;
  }

  public int[] indexOfMV(BigDecimal[] values) {
    int[] indexes = new int[values.length];
    if (!_trackUncompressedValueBytes) {
      for (int i = 0; i < values.length; i++) {
        indexes[i] = checkedDictId(values[i]);
      }
      return indexes;
    }
    for (int i = 0; i < values.length; i++) {
      indexes[i] = trackUncompressedValueSize(values[i], checkedDictId(values[i]));
    }
    return indexes;
  }

  public int[] indexOfMV(String[] values) {
    int[] indexes = new int[values.length];
    if (!_trackUncompressedValueBytes) {
      for (int i = 0; i < values.length; i++) {
        indexes[i] = checkedDictId(values[i]);
      }
      return indexes;
    }
    for (int i = 0; i < values.length; i++) {
      indexes[i] = trackUncompressedValueSize(values[i], checkedDictId(values[i]));
    }
    return indexes;
  }

  public int[] indexOfMV(byte[][] values) {
    int[] indexes = new int[values.length];
    if (!_trackUncompressedValueBytes) {
      for (int i = 0; i < values.length; i++) {
        indexes[i] = checkedDictId(new ByteArray(values[i]));
      }
      return indexes;
    }
    for (int i = 0; i < values.length; i++) {
      ByteArray value = new ByteArray(values[i]);
      indexes[i] = trackUncompressedValueSize(value, checkedDictId(value));
    }
    return indexes;
  }

  private int indexOfVariableLengthValue(Object value) {
    int dictId = checkedDictId(value);
    return _trackUncompressedValueBytes ? trackUncompressedValueSize(value, dictId) : dictId;
  }

  private int trackUncompressedValueSize(Object value, int dictId) {
    if (_serializedValueLengthByDictId != null) {
      _totalUncompressedValueSizeInBytes += _serializedValueLengthByDictId[dictId];
      return dictId;
    }
    switch (_storedType) {
      case BIG_DECIMAL:
        _totalUncompressedValueSizeInBytes += BigDecimalUtils.byteSize((BigDecimal) value);
        break;
      case STRING:
        _totalUncompressedValueSizeInBytes += Utf8Utils.encodedLengthWithReplacement((String) value);
        break;
      case BYTES:
        _totalUncompressedValueSizeInBytes += ((ByteArray) value).getBytes().length;
        break;
      default:
        throw new IllegalStateException("Cannot track uncompressed size for data type: " + _storedType);
    }
    return dictId;
  }

  private void initializeSerializedLengthCache(int cardinality) {
    if (_trackUncompressedValueBytes && cardinality <= MAX_CACHED_SERIALIZED_LENGTHS) {
      _serializedValueLengthByDictId = new int[cardinality];
    }
  }

  private void cacheSerializedLength(int dictId, int length) {
    if (_serializedValueLengthByDictId != null) {
      _serializedValueLengthByDictId[dictId] = length;
    }
  }

  public int[] indexOfMV(Object value) {
    Object[] multiValues = (Object[]) value;
    int[] indexes = new int[multiValues.length];

    switch (_storedType) {
      case INT:
        for (int i = 0; i < multiValues.length; i++) {
          indexes[i] = checkedDictId((int) multiValues[i]);
        }
        break;
      case LONG:
        for (int i = 0; i < multiValues.length; i++) {
          indexes[i] = checkedDictId((long) multiValues[i]);
        }
        break;
      case FLOAT:
        for (int i = 0; i < multiValues.length; i++) {
          indexes[i] = checkedDictId((float) multiValues[i]);
        }
        break;
      case DOUBLE:
        for (int i = 0; i < multiValues.length; i++) {
          indexes[i] = checkedDictId((double) multiValues[i]);
        }
        break;
      case BIG_DECIMAL:
        if (!_trackUncompressedValueBytes) {
          for (int i = 0; i < multiValues.length; i++) {
            indexes[i] = checkedDictId(multiValues[i]);
          }
          break;
        }
        for (int i = 0; i < multiValues.length; i++) {
          indexes[i] = trackUncompressedValueSize(multiValues[i], checkedDictId(multiValues[i]));
        }
        break;
      case STRING:
        if (!_trackUncompressedValueBytes) {
          for (int i = 0; i < multiValues.length; i++) {
            indexes[i] = checkedDictId(multiValues[i]);
          }
          break;
        }
        for (int i = 0; i < multiValues.length; i++) {
          indexes[i] = trackUncompressedValueSize(multiValues[i], checkedDictId(multiValues[i]));
        }
        break;
      case BYTES:
        if (!_trackUncompressedValueBytes) {
          for (int i = 0; i < multiValues.length; i++) {
            indexes[i] = checkedDictId(new ByteArray((byte[]) multiValues[i]));
          }
          break;
        }
        for (int i = 0; i < multiValues.length; i++) {
          ByteArray bytes = new ByteArray((byte[]) multiValues[i]);
          indexes[i] = trackUncompressedValueSize(bytes, checkedDictId(bytes));
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported data type : " + _storedType);
    }
    return indexes;
  }

  /// Cleans up the no longer needed objects after all the indexing is done to free up some memory.
  public void postIndexingCleanup() {
    _intValueToIndexMap = null;
    _longValueToIndexMap = null;
    _floatValueToIndexMap = null;
    _doubleValueToIndexMap = null;
    _objectValueToIndexMap = null;
    _serializedValueLengthByDictId = null;
  }

  @Override
  public void seal() {
    postIndexingCleanup();
  }

  @Override
  public void close() {
    postIndexingCleanup();
  }

  private int checkedDictId(int value) {
    int dictId = _intValueToIndexMap.get(value);
    if (dictId < 0) {
      throw valueNotInDictionary(value);
    }
    return dictId;
  }

  private int checkedDictId(long value) {
    int dictId = _longValueToIndexMap.get(value);
    if (dictId < 0) {
      throw valueNotInDictionary(value);
    }
    return dictId;
  }

  private int checkedDictId(float value) {
    int dictId = _floatValueToIndexMap.get(value);
    if (dictId < 0) {
      throw valueNotInDictionary(value);
    }
    return dictId;
  }

  private int checkedDictId(double value) {
    int dictId = _doubleValueToIndexMap.get(value);
    if (dictId < 0) {
      throw valueNotInDictionary(value);
    }
    return dictId;
  }

  private int checkedDictId(Object value) {
    int dictId = _objectValueToIndexMap.getInt(value);
    if (dictId < 0) {
      throw valueNotInDictionary(value);
    }
    return dictId;
  }

  /// A miss means the index pass saw a value the stats pass did not. Mapping it to id 0 would corrupt the index.
  private IllegalStateException valueNotInDictionary(Object value) {
    return new IllegalStateException(
        "Value: " + value + " is not in the dictionary for column: " + _columnName + " (" + _storedType + "). The "
            + "input, or a volatile ingestion transform such as now() or rand(), gave different values on the stats "
            + "and index passes.");
  }
}
