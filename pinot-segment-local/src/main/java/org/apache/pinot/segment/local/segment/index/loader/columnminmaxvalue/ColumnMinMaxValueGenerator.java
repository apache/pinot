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
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentColumnarIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.BytesDictionary;
import org.apache.pinot.segment.local.segment.index.readers.DoubleDictionary;
import org.apache.pinot.segment.local.segment.index.readers.FloatDictionary;
import org.apache.pinot.segment.local.segment.index.readers.IntDictionary;
import org.apache.pinot.segment.local.segment.index.readers.LongDictionary;
import org.apache.pinot.segment.local.segment.index.readers.StringDictionary;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
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

  private Set<String> getColumnsToAddMinMaxValue() {
    Schema schema = _segmentMetadata.getSchema();
    Set<String> columnsToAddMinMaxValue = new HashSet<>(schema.getPhysicalColumnNames());

    // mode ALL - use all columns
    // mode NON_METRIC - use all dimensions and time columns
    // mode TIME - use only time columns
    switch (_columnMinMaxValueGeneratorMode) {
      case TIME:
        columnsToAddMinMaxValue.removeAll(schema.getDimensionNames());
        columnsToAddMinMaxValue.removeAll(schema.getMetricNames());
        break;
      case NON_METRIC:
        columnsToAddMinMaxValue.removeAll(schema.getMetricNames());
        break;
      default:
        break;
    }
    return columnsToAddMinMaxValue;
  }

  private boolean needAddColumnMinMaxValueForColumn(String columnName) {
    ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(columnName);
    return columnMetadata.hasDictionary() && columnMetadata.getMinValue() == null
        && columnMetadata.getMaxValue() == null && !columnMetadata.isMinMaxValueInvalid();
  }

  private void addColumnMinMaxValueForColumn(String columnName)
      throws Exception {
    // Skip column without dictionary or with min/max value already set
    ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(columnName);
    if (!columnMetadata.hasDictionary() || columnMetadata.getMinValue() != null
        || columnMetadata.getMaxValue() != null) {
      return;
    }

    PinotDataBuffer dictionaryBuffer = _segmentWriter.getIndexFor(columnName, ColumnIndexType.DICTIONARY);
    DataType dataType = columnMetadata.getDataType().getStoredType();
    int length = columnMetadata.getCardinality();
    switch (dataType) {
      case INT:
        try (IntDictionary intDictionary = new IntDictionary(dictionaryBuffer, length)) {
          SegmentColumnarIndexCreator
              .addColumnMinMaxValueInfo(_segmentProperties, columnName, intDictionary.getStringValue(0),
                  intDictionary.getStringValue(length - 1));
        }
        break;
      case LONG:
        try (LongDictionary longDictionary = new LongDictionary(dictionaryBuffer, length)) {
          SegmentColumnarIndexCreator
              .addColumnMinMaxValueInfo(_segmentProperties, columnName, longDictionary.getStringValue(0),
                  longDictionary.getStringValue(length - 1));
        }
        break;
      case FLOAT:
        try (FloatDictionary floatDictionary = new FloatDictionary(dictionaryBuffer, length)) {
          SegmentColumnarIndexCreator
              .addColumnMinMaxValueInfo(_segmentProperties, columnName, floatDictionary.getStringValue(0),
                  floatDictionary.getStringValue(length - 1));
        }
        break;
      case DOUBLE:
        try (DoubleDictionary doubleDictionary = new DoubleDictionary(dictionaryBuffer, length)) {
          SegmentColumnarIndexCreator
              .addColumnMinMaxValueInfo(_segmentProperties, columnName, doubleDictionary.getStringValue(0),
                  doubleDictionary.getStringValue(length - 1));
        }
        break;
      case STRING:
        try (StringDictionary stringDictionary = new StringDictionary(dictionaryBuffer, length,
            columnMetadata.getColumnMaxLength(), (byte) columnMetadata.getPaddingCharacter())) {
          SegmentColumnarIndexCreator
              .addColumnMinMaxValueInfo(_segmentProperties, columnName, stringDictionary.getStringValue(0),
                  stringDictionary.getStringValue(length - 1));
        }
        break;
      case BYTES:
        try (BytesDictionary bytesDictionary = new BytesDictionary(dictionaryBuffer, length,
            columnMetadata.getColumnMaxLength())) {
          SegmentColumnarIndexCreator
              .addColumnMinMaxValueInfo(_segmentProperties, columnName, bytesDictionary.getStringValue(0),
                  bytesDictionary.getStringValue(length - 1));
        }
        break;
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType + " for column: " + columnName);
    }

    _minMaxValueAdded = true;
  }
}
