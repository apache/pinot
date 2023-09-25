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
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexReaderFactory;
import org.apache.pinot.segment.local.segment.index.readers.BytesDictionary;
import org.apache.pinot.segment.local.segment.index.readers.DoubleDictionary;
import org.apache.pinot.segment.local.segment.index.readers.FloatDictionary;
import org.apache.pinot.segment.local.segment.index.readers.IntDictionary;
import org.apache.pinot.segment.local.segment.index.readers.LongDictionary;
import org.apache.pinot.segment.local.segment.index.readers.StringDictionary;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;

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

    DataType dataType = columnMetadata.getDataType();
    DataType storedType = dataType.getStoredType();
    if (columnMetadata.hasDictionary()) {
      PinotDataBuffer dictionaryBuffer = _segmentWriter.getIndexFor(columnName, StandardIndexes.dictionary());
      int length = columnMetadata.getCardinality();
      switch (storedType) {
        case INT:
          try (IntDictionary intDictionary = new IntDictionary(dictionaryBuffer, length)) {
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                intDictionary.getStringValue(0), intDictionary.getStringValue(length - 1), storedType);
          }
          break;
        case LONG:
          try (LongDictionary longDictionary = new LongDictionary(dictionaryBuffer, length)) {
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                longDictionary.getStringValue(0), longDictionary.getStringValue(length - 1), storedType);
          }
          break;
        case FLOAT:
          try (FloatDictionary floatDictionary = new FloatDictionary(dictionaryBuffer, length)) {
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                floatDictionary.getStringValue(0), floatDictionary.getStringValue(length - 1), storedType);
          }
          break;
        case DOUBLE:
          try (DoubleDictionary doubleDictionary = new DoubleDictionary(dictionaryBuffer, length)) {
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                doubleDictionary.getStringValue(0), doubleDictionary.getStringValue(length - 1), storedType);
          }
          break;
        case STRING:
          try (StringDictionary stringDictionary = new StringDictionary(dictionaryBuffer, length,
              columnMetadata.getColumnMaxLength())) {
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                stringDictionary.getStringValue(0), stringDictionary.getStringValue(length - 1), storedType);
          }
          break;
        case BYTES:
          try (BytesDictionary bytesDictionary = new BytesDictionary(dictionaryBuffer, length,
              columnMetadata.getColumnMaxLength())) {
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                bytesDictionary.getStringValue(0), bytesDictionary.getStringValue(length - 1), storedType);
          }
          break;
        default:
          throw new IllegalStateException("Unsupported data type: " + dataType + " for column: " + columnName);
      }
    } else {
      // setting min/max for non-dictionary columns.
      int numDocs = columnMetadata.getTotalDocs();
      PinotDataBuffer rawIndexBuffer = _segmentWriter.getIndexFor(columnName, StandardIndexes.forward());
      boolean isSingleValue = _segmentMetadata.getSchema().getFieldSpecFor(columnName).isSingleValueField();
      try (
          ForwardIndexReader rawIndexReader = ForwardIndexReaderFactory.createRawIndexReader(rawIndexBuffer, storedType,
              isSingleValue); ForwardIndexReaderContext readerContext = rawIndexReader.createContext()) {
        switch (storedType) {
          case INT: {
            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;
            if (isSingleValue) {
              for (int docId = 0; docId < numDocs; docId++) {
                int value = rawIndexReader.getInt(docId, readerContext);
                min = Math.min(min, value);
                max = Math.max(max, value);
              }
            } else {
              for (int docId = 0; docId < numDocs; docId++) {
                int[] values = rawIndexReader.getIntMV(docId, readerContext);
                for (int value : values) {
                  min = Math.min(min, value);
                  max = Math.max(max, value);
                }
              }
            }
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName, String.valueOf(min),
                String.valueOf(max), storedType);
            break;
          }
          case LONG: {
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            if (isSingleValue) {
              for (int docId = 0; docId < numDocs; docId++) {
                long value = rawIndexReader.getLong(docId, readerContext);
                min = Math.min(min, value);
                max = Math.max(max, value);
              }
            } else {
              for (int docId = 0; docId < numDocs; docId++) {
                long[] values = rawIndexReader.getLongMV(docId, readerContext);
                for (long value : values) {
                  min = Math.min(min, value);
                  max = Math.max(max, value);
                }
              }
            }
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName, String.valueOf(min),
                String.valueOf(max), storedType);
            break;
          }
          case FLOAT: {
            float min = Float.POSITIVE_INFINITY;
            float max = Float.NEGATIVE_INFINITY;
            if (isSingleValue) {
              for (int docId = 0; docId < numDocs; docId++) {
                float value = rawIndexReader.getFloat(docId, readerContext);
                min = Math.min(min, value);
                max = Math.max(max, value);
              }
            } else {
              for (int docId = 0; docId < numDocs; docId++) {
                float[] values = rawIndexReader.getFloatMV(docId, readerContext);
                for (float value : values) {
                  min = Math.min(min, value);
                  max = Math.max(max, value);
                }
              }
            }
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName, String.valueOf(min),
                String.valueOf(max), storedType);
            break;
          }
          case DOUBLE: {
            double min = Double.POSITIVE_INFINITY;
            double max = Double.NEGATIVE_INFINITY;
            if (isSingleValue) {
              for (int docId = 0; docId < numDocs; docId++) {
                double value = rawIndexReader.getDouble(docId, readerContext);
                min = Math.min(min, value);
                max = Math.max(max, value);
              }
            } else {
              for (int docId = 0; docId < numDocs; docId++) {
                double[] values = rawIndexReader.getDoubleMV(docId, readerContext);
                for (double value : values) {
                  min = Math.min(min, value);
                  max = Math.max(max, value);
                }
              }
            }
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName, String.valueOf(min),
                String.valueOf(max), storedType);
            break;
          }
          case STRING: {
            String min = null;
            String max = null;
            if (isSingleValue) {
              for (int docId = 0; docId < numDocs; docId++) {
                String value = rawIndexReader.getString(docId, readerContext);
                if (min == null || StringUtils.compare(min, value) > 0) {
                  min = value;
                }
                if (max == null || StringUtils.compare(max, value) < 0) {
                  max = value;
                }
              }
            } else {
              for (int docId = 0; docId < numDocs; docId++) {
                String[] values = rawIndexReader.getStringMV(docId, readerContext);
                for (String value : values) {
                  if (min == null || StringUtils.compare(min, value) > 0) {
                    min = value;
                  }
                  if (max == null || StringUtils.compare(max, value) < 0) {
                    max = value;
                  }
                }
              }
            }
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName, min, max, storedType);
            break;
          }
          case BYTES: {
            byte[] min = null;
            byte[] max = null;
            if (isSingleValue) {
              for (int docId = 0; docId < numDocs; docId++) {
                byte[] value = rawIndexReader.getBytes(docId, readerContext);
                if (min == null || ByteArray.compare(value, min) > 0) {
                  min = value;
                }
                if (max == null || ByteArray.compare(value, max) < 0) {
                  max = value;
                }
              }
            } else {
              for (int docId = 0; docId < numDocs; docId++) {
                byte[][] values = rawIndexReader.getBytesMV(docId, readerContext);
                for (byte[] value : values) {
                  if (min == null || ByteArray.compare(value, min) > 0) {
                    min = value;
                  }
                  if (max == null || ByteArray.compare(value, max) < 0) {
                    max = value;
                  }
                }
              }
            }
            SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName,
                BytesUtils.toHexString(min), BytesUtils.toHexString(max), storedType);
            break;
          }
          default:
            throw new IllegalStateException("Unsupported data type: " + dataType + " for column: " + columnName);
        }
      }
      _minMaxValueAdded = true;
    }
  }
}
