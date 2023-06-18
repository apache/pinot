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
import org.apache.pinot.segment.local.segment.creator.impl.SegmentColumnarIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.BytesDictionary;
import org.apache.pinot.segment.local.segment.index.readers.DoubleDictionary;
import org.apache.pinot.segment.local.segment.index.readers.FloatDictionary;
import org.apache.pinot.segment.local.segment.index.readers.IntDictionary;
import org.apache.pinot.segment.local.segment.index.readers.LongDictionary;
import org.apache.pinot.segment.local.segment.index.readers.StringDictionary;
import org.apache.pinot.segment.local.segment.index.readers.forward.ChunkReaderContext;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;

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
    return columnMetadata.getMinValue() == null
        && columnMetadata.getMaxValue() == null && !columnMetadata.isMinMaxValueInvalid();
  }

  private void addColumnMinMaxValueForColumn(String columnName)
      throws Exception {
    // Skip column without dictionary or with min/max value already set
    System.out.println(_segmentMetadata.getName());
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
      PinotDataBuffer forwardBuffer = _segmentWriter.getIndexFor(columnName, StandardIndexes.forward());
      switch (dataType) {
        case INT:
          try (FixedByteChunkSVForwardIndexReader rawIndexReader = new FixedByteChunkSVForwardIndexReader(forwardBuffer,
              DataType.INT); ChunkReaderContext readerContext = rawIndexReader.createContext()) {
            Integer[] minMaxValue = {Integer.MAX_VALUE, Integer.MIN_VALUE};
            for (int docs = 0; docs < columnMetadata.getTotalDocs(); docs++) {
              minMaxValue = getMinMaxValue(minMaxValue, rawIndexReader.getInt(docs, readerContext));
            }
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                String.valueOf(minMaxValue[0]), String.valueOf(minMaxValue[1]));
          }
          break;
        case LONG:
          try (FixedByteChunkSVForwardIndexReader rawIndexReader = new FixedByteChunkSVForwardIndexReader(forwardBuffer,
              DataType.LONG); ChunkReaderContext readerContext = rawIndexReader.createContext()) {
            Long[] minMaxValue = {Long.MAX_VALUE, Long.MIN_VALUE};
            for (int docs = 0; docs < columnMetadata.getTotalDocs(); docs++) {
              minMaxValue = getMinMaxValue(minMaxValue, rawIndexReader.getLong(docs, readerContext));
            }
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                String.valueOf(minMaxValue[0]), String.valueOf(minMaxValue[1]));
          }
          break;
        case FLOAT:
          try (FixedByteChunkSVForwardIndexReader rawIndexReader = new FixedByteChunkSVForwardIndexReader(forwardBuffer,
              DataType.FLOAT); ChunkReaderContext readerContext = rawIndexReader.createContext()) {
            Float[] minMaxValue = {Float.MAX_VALUE, Float.MIN_VALUE};
            for (int docs = 0; docs < columnMetadata.getTotalDocs(); docs++) {
              minMaxValue = getMinMaxValue(minMaxValue, rawIndexReader.getFloat(docs, readerContext));
            }
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                String.valueOf(minMaxValue[0]), String.valueOf(minMaxValue[1]));
          }
          break;
        case DOUBLE:
          try (FixedByteChunkSVForwardIndexReader rawIndexReader = new FixedByteChunkSVForwardIndexReader(forwardBuffer,
              DataType.DOUBLE); ChunkReaderContext readerContext = rawIndexReader.createContext()) {
            Double[] minMaxValue = {Double.MAX_VALUE, Double.MIN_VALUE};
            for (int docs = 0; docs < columnMetadata.getTotalDocs(); docs++) {
              minMaxValue = getMinMaxValue(minMaxValue, rawIndexReader.getDouble(docs, readerContext));
            }
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                String.valueOf(minMaxValue[0]), String.valueOf(minMaxValue[1]));
          }
          break;
        default:
          throw new IllegalStateException("Unsupported data type: " + dataType + " for column: " + columnName);
      }
    }
    _minMaxValueAdded = true;
  }

  private <T extends Comparable<T>> T[] getMinMaxValue(T[] minMaxValues, T val) {
    if (val.compareTo(minMaxValues[0]) < 0) {
      minMaxValues[0] = val;
    }
    if (val.compareTo(minMaxValues[1]) > 0) {
      minMaxValues[1] = val;
    }
    return minMaxValues;
  }
}
