/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.creator.impl;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.io.util.FixedByteValueReaderWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentDictionaryCreator implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDictionaryCreator.class);
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private final Object _sortedValues;
  private final FieldSpec _fieldSpec;
  private final File _dictionaryFile;

  private Int2IntOpenHashMap _intValueToIndexMap;
  private Long2IntOpenHashMap _longValueToIndexMap;
  private Float2IntOpenHashMap _floatValueToIndexMap;
  private Double2IntOpenHashMap _doubleValueToIndexMap;
  private Object2IntOpenHashMap<String> _stringValueToIndexMap;
  private int _numBytesPerString = 0;

  public SegmentDictionaryCreator(Object sortedValues, FieldSpec fieldSpec, File indexDir) throws IOException {
    _sortedValues = sortedValues;
    _fieldSpec = fieldSpec;
    _dictionaryFile = new File(indexDir, fieldSpec.getName() + V1Constants.Dict.FILE_EXTENSION);
    FileUtils.touch(_dictionaryFile);
  }

  public void build() throws IOException {
    switch (_fieldSpec.getDataType()) {
      case INT:
        int[] sortedInts = (int[]) _sortedValues;
        int numValues = sortedInts.length;
        Preconditions.checkState(numValues > 0);
        _intValueToIndexMap = new Int2IntOpenHashMap(numValues);

        try (PinotDataBuffer dataBuffer = PinotDataBuffer.fromFile(_dictionaryFile, 0,
            numValues * V1Constants.Numbers.INTEGER_SIZE, ReadMode.mmap, FileChannel.MapMode.READ_WRITE,
            _dictionaryFile.getName());
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

        try (PinotDataBuffer dataBuffer = PinotDataBuffer.fromFile(_dictionaryFile, 0,
            numValues * V1Constants.Numbers.LONG_SIZE, ReadMode.mmap, FileChannel.MapMode.READ_WRITE,
            _dictionaryFile.getName());
            FixedByteValueReaderWriter writer = new FixedByteValueReaderWriter(dataBuffer)) {
          for (int i = 0; i < numValues; i++) {
            long value = sortedLongs[i];
            _longValueToIndexMap.put(value, i);
            writer.writeLong(i, value);
          }
        }
        LOGGER.info("Created dictionary for LONG column: {} with cardinality: {}, range: {} to {}",
            _fieldSpec.getName(), numValues, sortedLongs[0], sortedLongs[numValues - 1]);
        return;
      case FLOAT:
        float[] sortedFloats = (float[]) _sortedValues;
        numValues = sortedFloats.length;
        Preconditions.checkState(numValues > 0);
        _floatValueToIndexMap = new Float2IntOpenHashMap(numValues);

        try (PinotDataBuffer dataBuffer = PinotDataBuffer.fromFile(_dictionaryFile, 0,
            numValues * V1Constants.Numbers.FLOAT_SIZE, ReadMode.mmap, FileChannel.MapMode.READ_WRITE,
            _dictionaryFile.getName());
            FixedByteValueReaderWriter writer = new FixedByteValueReaderWriter(dataBuffer)) {
          for (int i = 0; i < numValues; i++) {
            float value = sortedFloats[i];
            _floatValueToIndexMap.put(value, i);
            writer.writeFloat(i, value);
          }
        }
        LOGGER.info("Created dictionary for FLOAT column: {} with cardinality: {}, range: {} to {}",
            _fieldSpec.getName(), numValues, sortedFloats[0], sortedFloats[numValues - 1]);
        return;
      case DOUBLE:
        double[] sortedDoubles = (double[]) _sortedValues;
        numValues = sortedDoubles.length;
        Preconditions.checkState(numValues > 0);
        _doubleValueToIndexMap = new Double2IntOpenHashMap(numValues);

        try (PinotDataBuffer dataBuffer = PinotDataBuffer.fromFile(_dictionaryFile, 0,
            numValues * V1Constants.Numbers.DOUBLE_SIZE, ReadMode.mmap, FileChannel.MapMode.READ_WRITE,
            _dictionaryFile.getName());
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
          byte[] valueBytes = value.getBytes(UTF_8);
          sortedStringBytes[i] = valueBytes;
          _numBytesPerString = Math.max(_numBytesPerString, valueBytes.length);
        }

        try (PinotDataBuffer dataBuffer = PinotDataBuffer.fromFile(_dictionaryFile, 0, numValues * _numBytesPerString,
            ReadMode.mmap, FileChannel.MapMode.READ_WRITE, _dictionaryFile.getName());
            FixedByteValueReaderWriter writer = new FixedByteValueReaderWriter(dataBuffer)) {
          for (int i = 0; i < numValues; i++) {
            byte[] value = sortedStringBytes[i];
            writer.writeUnpaddedString(i, _numBytesPerString, value);
          }
        }
        LOGGER.info(
            "Created dictionary for STRING column: {} with cardinality: {}, max length in bytes: {}, range: {} to {}",
            _fieldSpec.getName(), numValues, _numBytesPerString, sortedStrings[0], sortedStrings[numValues - 1]);
        return;
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + _fieldSpec.getDataType());
    }
  }

  public int getNumBytesPerString() {
    return _numBytesPerString;
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
