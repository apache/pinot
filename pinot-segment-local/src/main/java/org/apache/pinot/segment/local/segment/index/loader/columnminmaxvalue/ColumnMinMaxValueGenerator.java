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
package org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentColumnarIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.BytesDictionary;
import org.apache.pinot.segment.local.segment.index.readers.DoubleDictionary;
import org.apache.pinot.segment.local.segment.index.readers.FloatDictionary;
import org.apache.pinot.segment.local.segment.index.readers.IntDictionary;
import org.apache.pinot.segment.local.segment.index.readers.LongDictionary;
import org.apache.pinot.segment.local.segment.index.readers.StringDictionary;
import org.apache.pinot.segment.local.segment.index.readers.forward.ChunkReaderContext;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkMVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkMVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkSVForwardIndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.ByteArray;

import static org.apache.pinot.spi.data.FieldSpec.DataType;


public class ColumnMinMaxValueGenerator {
  private final SegmentMetadata _segmentMetadata;
  private final SegmentDirectory.Writer _segmentWriter;
  private final ColumnMinMaxValueGeneratorMode _columnMinMaxValueGeneratorMode;

  // NOTE: _segmentProperties shouldn't be used when checking whether min/max value need to be generated because at that
  //       time _segmentMetadata might not be loaded from a local file
  private PropertiesConfiguration _segmentProperties;

  private boolean _minMaxValueAdded;

  public ColumnMinMaxValueGenerator(SegmentMetadata segmentMetadata, SegmentDirectory.Writer segmentWriter,
      ColumnMinMaxValueGeneratorMode columnMinMaxValueGeneratorMode) {
    _segmentMetadata = segmentMetadata;
    _segmentWriter = segmentWriter;
    _columnMinMaxValueGeneratorMode = columnMinMaxValueGeneratorMode;
  }

  public boolean needAddColumnMinMaxValue() {
    for (String column : getColumnsToAddMinMaxValue()) {
      if (needAddColumnMinMaxValueForColumn(column)) {
        return true;
      }
    }
    return false;
  }

  public void addColumnMinMaxValue()
      throws Exception {
    Preconditions.checkState(_columnMinMaxValueGeneratorMode != ColumnMinMaxValueGeneratorMode.NONE);
    _segmentProperties = SegmentMetadataUtils.getPropertiesConfiguration(_segmentMetadata);
    for (String column : getColumnsToAddMinMaxValue()) {
      addColumnMinMaxValueForColumn(column);
    }
    if (_minMaxValueAdded) {
      SegmentMetadataUtils.savePropertiesConfiguration(_segmentProperties);
    }
  }

  private List<String> getColumnsToAddMinMaxValue() {
    Schema schema = _segmentMetadata.getSchema();
    List<String> columnsToAddMinMaxValue = new ArrayList<>();

    // mode ALL - use all columns
    // mode NON_METRIC - use all dimensions and time columns
    // mode TIME - use only time columns
    switch (_columnMinMaxValueGeneratorMode) {
      case ALL:
        for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
          if (!fieldSpec.isVirtualColumn()) {
            columnsToAddMinMaxValue.add(fieldSpec.getName());
          }
        }
        break;
      case NON_METRIC:
        for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
          if (!fieldSpec.isVirtualColumn() && fieldSpec.getFieldType() != FieldSpec.FieldType.METRIC) {
            columnsToAddMinMaxValue.add(fieldSpec.getName());
          }
        }
        break;
      case TIME:
        for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
          if (!fieldSpec.isVirtualColumn() && (fieldSpec.getFieldType() == FieldSpec.FieldType.TIME
              || fieldSpec.getFieldType() == FieldSpec.FieldType.DATE_TIME)) {
            columnsToAddMinMaxValue.add(fieldSpec.getName());
          }
        }
        break;
      default:
        throw new IllegalStateException("Unsupported generator mode: " + _columnMinMaxValueGeneratorMode);
    }

    return columnsToAddMinMaxValue;
  }

  private boolean needAddColumnMinMaxValueForColumn(String columnName) {
    ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(columnName);
    return columnMetadata.getMinValue() == null && columnMetadata.getMaxValue() == null
        && !columnMetadata.isMinMaxValueInvalid();
  }

  private void addColumnMinMaxValueForColumn(String columnName)
      throws Exception {
    // Skip column with min/max value already set
    ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(columnName);
    if (columnMetadata.getMinValue() != null || columnMetadata.getMaxValue() != null) {
      return;
    }

    DataType dataType = columnMetadata.getDataType().getStoredType();
    if (columnMetadata.hasDictionary()) {
      PinotDataBuffer dictionaryBuffer = _segmentWriter.getIndexFor(columnName, StandardIndexes.dictionary());
      int length = columnMetadata.getCardinality();
      switch (dataType) {
        case INT:
          try (IntDictionary intDictionary = new IntDictionary(dictionaryBuffer, length)) {
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                intDictionary.getStringValue(0), intDictionary.getStringValue(length - 1));
          }
          break;
        case LONG:
          try (LongDictionary longDictionary = new LongDictionary(dictionaryBuffer, length)) {
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                longDictionary.getStringValue(0), longDictionary.getStringValue(length - 1));
          }
          break;
        case FLOAT:
          try (FloatDictionary floatDictionary = new FloatDictionary(dictionaryBuffer, length)) {
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                floatDictionary.getStringValue(0), floatDictionary.getStringValue(length - 1));
          }
          break;
        case DOUBLE:
          try (DoubleDictionary doubleDictionary = new DoubleDictionary(dictionaryBuffer, length)) {
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                doubleDictionary.getStringValue(0), doubleDictionary.getStringValue(length - 1));
          }
          break;
        case STRING:
          try (StringDictionary stringDictionary = new StringDictionary(dictionaryBuffer, length,
              columnMetadata.getColumnMaxLength())) {
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                stringDictionary.getStringValue(0), stringDictionary.getStringValue(length - 1));
          }
          break;
        case BYTES:
          try (BytesDictionary bytesDictionary = new BytesDictionary(dictionaryBuffer, length,
              columnMetadata.getColumnMaxLength())) {
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                bytesDictionary.getStringValue(0), bytesDictionary.getStringValue(length - 1));
          }
          break;
        default:
          throw new IllegalStateException("Unsupported data type: " + dataType + " for column: " + columnName);
      }
    } else {
      // setting min/max for non-dictionary columns.
      int numDocs = columnMetadata.getTotalDocs();
      boolean isSingleValueField = _segmentMetadata.getSchema().getFieldSpecFor(columnName).isSingleValueField();
      PinotDataBuffer forwardBuffer = _segmentWriter.getIndexFor(columnName, StandardIndexes.forward());
      switch (dataType) {
        case INT: {
          int min = Integer.MAX_VALUE;
          int max = Integer.MIN_VALUE;
          if (isSingleValueField) {
            FixedByteChunkSVForwardIndexReader rawIndexReader = new FixedByteChunkSVForwardIndexReader(forwardBuffer,
                DataType.INT);
            ChunkReaderContext readerContext = rawIndexReader.createContext();
            for (int docs = 0; docs < numDocs; docs++) {
              int value = rawIndexReader.getInt(docs, readerContext);
              min = Math.min(min, value);
              max = Math.max(max, value);
            }
          } else {
            FixedByteChunkMVForwardIndexReader rawIndexReader = new FixedByteChunkMVForwardIndexReader(forwardBuffer,
                DataType.INT);
            ChunkReaderContext readerContext = rawIndexReader.createContext();
            for (int docs = 0; docs < numDocs; docs++) {
              int[] value = rawIndexReader.getIntMV(docs, readerContext);
              for (int i = 0; i < value.length; i++) {
                min = Math.min(min, value[i]);
                max = Math.max(max, value[i]);
              }
            }
          }
          SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
              String.valueOf(min), String.valueOf(max));
         }
         break;
        case LONG: {
          long min = Long.MAX_VALUE;
          long max = Long.MIN_VALUE;
          if (isSingleValueField) {
            FixedByteChunkSVForwardIndexReader rawIndexReader = new FixedByteChunkSVForwardIndexReader(forwardBuffer,
                DataType.LONG);
            ChunkReaderContext readerContext = rawIndexReader.createContext();
            for (int docs = 0; docs < numDocs; docs++) {
              long value = rawIndexReader.getLong(docs, readerContext);
              min = Math.min(min, value);
              max = Math.max(max, value);
            }
          } else {
            FixedByteChunkMVForwardIndexReader rawIndexReader = new FixedByteChunkMVForwardIndexReader(
                forwardBuffer, DataType.LONG);
            ChunkReaderContext readerContext = rawIndexReader.createContext();
            for (int docs = 0; docs < numDocs; docs++) {
              long[] value = rawIndexReader.getLongMV(docs, readerContext);
              for (int i = 0; i < value.length; i++) {
                min = Math.min(min, value[i]);
                max = Math.max(max, value[i]);
              }
            }
          }
          SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                String.valueOf(min), String.valueOf(max));
         }
         break;
        case FLOAT: {
          float min = Float.MAX_VALUE;
          float max = Float.MIN_VALUE;
          if (isSingleValueField) {
            FixedByteChunkSVForwardIndexReader rawIndexReader = new FixedByteChunkSVForwardIndexReader(forwardBuffer,
                DataType.FLOAT);
            ChunkReaderContext readerContext = rawIndexReader.createContext();
            for (int docs = 0; docs < numDocs; docs++) {
              float value = rawIndexReader.getFloat(docs, readerContext);
              min = Math.min(min, value);
              max = Math.max(max, value);
            }
          } else {
            FixedByteChunkMVForwardIndexReader rawIndexReader = new FixedByteChunkMVForwardIndexReader(
                forwardBuffer, DataType.FLOAT);
            ChunkReaderContext readerContext = rawIndexReader.createContext();
            for (int docs = 0; docs < numDocs; docs++) {
              float[] value = rawIndexReader.getFloatMV(docs, readerContext);
              for (int i = 0; i < value.length; i++) {
                min = Math.min(min, value[i]);
                max = Math.max(max, value[i]);
              }
            }
          }
          SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                String.valueOf(min), String.valueOf(max));
         }
         break;
        case DOUBLE: {
          double min = Double.MAX_VALUE;
          double max = Double.MIN_VALUE;
          if (isSingleValueField) {
            FixedByteChunkSVForwardIndexReader rawIndexReader = new FixedByteChunkSVForwardIndexReader(forwardBuffer,
                DataType.DOUBLE);
            ChunkReaderContext readerContext = rawIndexReader.createContext();
            for (int docs = 0; docs < numDocs; docs++) {
              double value = rawIndexReader.getDouble(docs, readerContext);
              min = Math.min(min, value);
              max = Math.max(max, value);
            }
          } else {
            FixedByteChunkMVForwardIndexReader rawIndexReader = new FixedByteChunkMVForwardIndexReader(
                forwardBuffer, DataType.DOUBLE);
            ChunkReaderContext readerContext = rawIndexReader.createContext();
            for (int docs = 0; docs < numDocs; docs++) {
              double[] value = rawIndexReader.getDoubleMV(docs, readerContext);
              for (int i = 0; i < value.length; i++) {
                min = Math.min(min, value[i]);
                max = Math.max(max, value[i]);
              }
            }
          }
          SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                String.valueOf(min), String.valueOf(max));
          }
          break;
        case STRING: {
          String min = null;
          String max = null;
          if (isSingleValueField) {
            VarByteChunkSVForwardIndexReader rawIndexReader = new VarByteChunkSVForwardIndexReader(forwardBuffer,
                DataType.STRING);
            ChunkReaderContext readerContext = rawIndexReader.createContext();
            for (int docs = 0; docs < numDocs; docs++) {
              String value = rawIndexReader.getString(docs, readerContext);
              if (min == null || StringUtils.compare(min, value) > 0) {
                min = value;
              }
              if (max == null || StringUtils.compare(max, value) < 0) {
                max = value;
              }
            }
          } else {
            VarByteChunkMVForwardIndexReader rawIndexReader = new VarByteChunkMVForwardIndexReader(forwardBuffer,
                DataType.STRING);
            ChunkReaderContext readerContext = rawIndexReader.createContext();
            for (int docs = 0; docs < numDocs; docs++) {
              String[] value = rawIndexReader.getStringMV(docs, readerContext);
              for (int i = 0; i < value.length; i++) {
                if (min == null || StringUtils.compare(min, value[i]) > 0) {
                  min = value[i];
                }
                if (max == null || StringUtils.compare(max, value[i]) < 0) {
                  max = value[i];
                }
              }
            }
          }
          SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName, min, max);
          }
          break;
        case BYTES: {
          byte[] min = null;
          byte[] max = null;
          if (isSingleValueField) {
            VarByteChunkSVForwardIndexReader rawIndexReader =
                new VarByteChunkSVForwardIndexReader(forwardBuffer, DataType.BYTES);
            ChunkReaderContext readerContext = rawIndexReader.createContext();
            for (int docs = 0; docs < numDocs; docs++) {
              byte[] value = rawIndexReader.getBytes(docs, readerContext);
              if (min == null || ByteArray.compare(value, min) > 0) {
                min = value;
              }
              if (max == null || ByteArray.compare(value, max) < 0) {
                max = value;
              }
            }
          } else {
            VarByteChunkMVForwardIndexReader rawIndexReader =
                new VarByteChunkMVForwardIndexReader(forwardBuffer, DataType.BYTES);
            ChunkReaderContext readerContext = rawIndexReader.createContext();
            for (int docs = 0; docs < numDocs; docs++) {
              byte[][] value = rawIndexReader.getBytesMV(docs, readerContext);
              for (int i = 0; i < value.length; i++) {
                if (min == null || ByteArray.compare(value[i], min) > 0) {
                  min = value[i];
                }
                if (max == null || ByteArray.compare(value[i], max) < 0) {
                  max = value[i];
                }
              }
            }
          }
          SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
              String.valueOf(new ByteArray(min)), String.valueOf(new ByteArray(max)));
          }
          break;
        default:
          throw new IllegalStateException("Unsupported data type: " + dataType + " for column: " + columnName);
      }
    }
    _minMaxValueAdded = true;
  }
}
