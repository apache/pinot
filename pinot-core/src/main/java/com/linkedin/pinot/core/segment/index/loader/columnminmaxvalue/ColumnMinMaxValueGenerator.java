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
package com.linkedin.pinot.core.segment.index.loader.columnminmaxvalue;

import com.clearspring.analytics.util.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.segment.creator.impl.SegmentColumnarIndexCreator;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.readers.DoubleDictionary;
import com.linkedin.pinot.core.segment.index.readers.FloatDictionary;
import com.linkedin.pinot.core.segment.index.readers.IntDictionary;
import com.linkedin.pinot.core.segment.index.readers.LongDictionary;
import com.linkedin.pinot.core.segment.index.readers.StringDictionary;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import org.apache.commons.configuration.PropertiesConfiguration;


public class ColumnMinMaxValueGenerator {
  private final SegmentMetadataImpl _segmentMetadata;
  private final PropertiesConfiguration _segmentProperties;
  private final SegmentDirectory.Writer _segmentWriter;
  private final ColumnMinMaxValueGeneratorMode _columnMinMaxValueGeneratorMode;

  private boolean _minMaxValueAdded;

  public ColumnMinMaxValueGenerator(SegmentMetadataImpl segmentMetadata, SegmentDirectory.Writer segmentWriter,
      ColumnMinMaxValueGeneratorMode columnMinMaxValueGeneratorMode) {
    _segmentMetadata = segmentMetadata;
    _segmentProperties = segmentMetadata.getSegmentMetadataPropertiesConfiguration();
    _segmentWriter = segmentWriter;
    _columnMinMaxValueGeneratorMode = columnMinMaxValueGeneratorMode;
  }

  public void addColumnMinMaxValue() throws Exception {
    Preconditions.checkState(_columnMinMaxValueGeneratorMode != ColumnMinMaxValueGeneratorMode.NONE);

    Schema schema = _segmentMetadata.getSchema();

    // Process time column
    String timeColumnName = schema.getTimeColumnName();
    if (timeColumnName != null) {
      addColumnMinMaxValueForColumn(timeColumnName);
    }
    if (_columnMinMaxValueGeneratorMode == ColumnMinMaxValueGeneratorMode.TIME) {
      saveMetadata();
      return;
    }

    // Process dimension columns
    for (String dimensionColumnName : schema.getDimensionNames()) {
      addColumnMinMaxValueForColumn(dimensionColumnName);
    }
    if (_columnMinMaxValueGeneratorMode == ColumnMinMaxValueGeneratorMode.NON_METRIC) {
      saveMetadata();
      return;
    }

    // Process metric columns
    for (String metricColumnName : schema.getMetricNames()) {
      addColumnMinMaxValueForColumn(metricColumnName);
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
            columnMetadata.getStringColumnMaxLength(), (byte) columnMetadata.getPaddingCharacter())) {
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
      _segmentProperties.save();
    }
  }
}
