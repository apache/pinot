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
package org.apache.pinot.core.segment.index.loader.columnminmaxvalue;

import java.io.FileOutputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.core.segment.creator.impl.SegmentColumnarIndexCreator;
import org.apache.pinot.core.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.readers.DoubleDictionary;
import org.apache.pinot.core.segment.index.readers.FloatDictionary;
import org.apache.pinot.core.segment.index.readers.IntDictionary;
import org.apache.pinot.core.segment.index.readers.LongDictionary;
import org.apache.pinot.core.segment.index.readers.StringDictionary;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.core.segment.store.ColumnIndexType;
import org.apache.pinot.core.segment.store.SegmentDirectory;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;

import com.clearspring.analytics.util.Preconditions;


public class ColumnMinMaxValueGenerator {
  private final SegmentMetadataImpl _segmentMetadata;
  private final PropertiesConfiguration _segmentProperties;
  private final SegmentDirectory.Writer _segmentWriter;
  private final ColumnMinMaxValueGeneratorMode _columnMinMaxValueGeneratorMode;

  private boolean _minMaxValueAdded;

  public ColumnMinMaxValueGenerator(SegmentMetadataImpl segmentMetadata, SegmentDirectory.Writer segmentWriter,
      ColumnMinMaxValueGeneratorMode columnMinMaxValueGeneratorMode) {
    _segmentMetadata = segmentMetadata;
    _segmentProperties = SegmentMetadataImpl.getPropertiesConfiguration(_segmentMetadata.getIndexDir());
    _segmentWriter = segmentWriter;
    _columnMinMaxValueGeneratorMode = columnMinMaxValueGeneratorMode;
  }

  public void addColumnMinMaxValue() throws Exception {
    Preconditions.checkState(_columnMinMaxValueGeneratorMode != ColumnMinMaxValueGeneratorMode.NONE);

    Schema schema = _segmentMetadata.getSchema();
    Set<String> columnsToAddMinMaxValue = new HashSet<>(schema.getPhysicalColumnNames());

    // mode ALL - use all columns
    // mode NON_METRIC - use all dimensions and time columns 
    // mode TIME - use only time columns
    switch (_columnMinMaxValueGeneratorMode) {
      case TIME:
        columnsToAddMinMaxValue.removeAll(schema.getDimensionNames());
        // Intentionally falling through to next case
      case NON_METRIC:
        columnsToAddMinMaxValue.removeAll(schema.getMetricNames());
    }
    for (String column : columnsToAddMinMaxValue) {
      addColumnMinMaxValueForColumn(column);
    }
    saveMetadata();
  }

  private void addColumnMinMaxValueForColumn(String columnName) throws Exception {
    // Skip column without dictionary or with min/max value already set
    ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(columnName);
    if ((!columnMetadata.hasDictionary()) || (columnMetadata.getMinValue() != null)) {
      return;
    }

    PinotDataBuffer dictionaryBuffer = _segmentWriter.getIndexFor(columnName, ColumnIndexType.DICTIONARY);
    FieldSpec.DataType dataType = columnMetadata.getDataType();
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
            columnMetadata.getColumnMaxLength(), (byte) columnMetadata.getPaddingCharacter())) {
          SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(_segmentProperties, columnName, stringDictionary.get(0),
              stringDictionary.get(length - 1));
        }
        break;
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType + " for column: " + columnName);
    }

    _minMaxValueAdded = true;
  }

  private void saveMetadata() throws Exception {
    if (_minMaxValueAdded) {
      // Commons Configuration 1.10 does not support file path containing '%'. 
      // Explicitly providing the output stream for the file bypasses the problem. 
      try (FileOutputStream fileOutputStream = new FileOutputStream(_segmentProperties.getFile())) {
        _segmentProperties.save(fileOutputStream);
      }
    }
  }
}
