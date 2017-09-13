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
package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.common.data.DateTimeFieldSpec;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitMultiValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.SortedIndexReader;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.DoubleDictionary;
import com.linkedin.pinot.core.segment.index.readers.FloatDictionary;
import com.linkedin.pinot.core.segment.index.readers.IntDictionary;
import com.linkedin.pinot.core.segment.index.readers.LongDictionary;
import com.linkedin.pinot.core.segment.index.readers.StringDictionary;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import com.linkedin.pinot.core.segment.store.SegmentDirectory.Reader;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.ConfigurationException;


/**
 * Record reader for Pinot segment.
 */
public class PinotSegmentRecordReader extends BaseRecordReader {
  private final SegmentMetadataImpl _segmentMetadata;
  private final int _numRows;

  private final Map<String, Dictionary> _dictionaryMap = new HashMap<>();
  private final Map<String, SingleColumnSingleValueReader> _singleValueReaderMap = new HashMap<>();
  private final Map<String, SingleColumnMultiValueReader> _multiValueReaderMap = new HashMap<>();
  private final int[] _multiValueBuffer;

  private int _nextRow;

  public PinotSegmentRecordReader(File indexDir) throws IOException, ConfigurationException {
    _segmentMetadata = new SegmentMetadataImpl(indexDir);
    _numRows = _segmentMetadata.getTotalDocs();

    SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(indexDir, _segmentMetadata, ReadMode.mmap);
    Reader reader = segmentDirectory.createReader();

    int maxNumMultiValues = 0;
    for (Entry<String, ColumnMetadata> entry : _segmentMetadata.getColumnMetadataMap().entrySet()) {
      String columnName = entry.getKey();
      ColumnMetadata columnMetadata = entry.getValue();

      PinotDataBuffer dictionaryBuffer = reader.getIndexFor(columnName, ColumnIndexType.DICTIONARY);
      int length = columnMetadata.getCardinality();
      switch (columnMetadata.getDataType()) {
        case INT:
          _dictionaryMap.put(columnName, new IntDictionary(dictionaryBuffer, length));
          break;
        case LONG:
          _dictionaryMap.put(columnName, new LongDictionary(dictionaryBuffer, length));
          break;
        case FLOAT:
          _dictionaryMap.put(columnName, new FloatDictionary(dictionaryBuffer, length));
          break;
        case DOUBLE:
          _dictionaryMap.put(columnName, new DoubleDictionary(dictionaryBuffer, length));
          break;
        case STRING:
          _dictionaryMap.put(columnName,
              new StringDictionary(dictionaryBuffer, length, columnMetadata.getStringColumnMaxLength(),
                  (byte) columnMetadata.getPaddingCharacter()));
          break;
        default:
          throw new IllegalStateException();
      }

      PinotDataBuffer forwardIndexBuffer = reader.getIndexFor(columnName, ColumnIndexType.FORWARD_INDEX);
      if (columnMetadata.isSingleValue()) {
        if (columnMetadata.isSorted()) {
          _singleValueReaderMap.put(columnName,
              new SortedIndexReader(forwardIndexBuffer, columnMetadata.getCardinality()));
        } else {
          _singleValueReaderMap.put(columnName,
              new FixedBitSingleValueReader(forwardIndexBuffer, _numRows, columnMetadata.getBitsPerElement()));
        }
      } else {
        _multiValueReaderMap.put(columnName,
            new FixedBitMultiValueReader(forwardIndexBuffer, _numRows, columnMetadata.getTotalNumberOfEntries(),
                columnMetadata.getBitsPerElement()));
        maxNumMultiValues = Math.max(maxNumMultiValues, columnMetadata.getMaxNumberOfMultiValues());
      }
    }
    _multiValueBuffer = new int[maxNumMultiValues];
  }

  @Override
  public void init() {
    _nextRow = 0;
  }

  @Override
  public void rewind() {
    _nextRow = 0;
  }

  @Override
  public boolean hasNext() {
    return _nextRow < _numRows;
  }

  @Override
  public Schema getSchema() {
    Schema schema = new Schema();
    schema.setSchemaName(_segmentMetadata.getTableName());

    for (Entry<String, ColumnMetadata> entry : _segmentMetadata.getColumnMetadataMap().entrySet()) {
      String columnName = entry.getKey();
      ColumnMetadata columnMetadata = entry.getValue();
      DataType dataType = columnMetadata.getDataType();

      switch (columnMetadata.getFieldType()) {
        case DIMENSION:
          schema.addField(new DimensionFieldSpec(columnName, dataType, columnMetadata.isSingleValue()));
          break;
        case METRIC:
          schema.addField(new MetricFieldSpec(columnName, dataType));
          break;
        case TIME:
          schema.addField(new TimeFieldSpec(columnName, dataType, columnMetadata.getTimeUnit()));
          break;
        case DATE_TIME:
          schema.addField(new DateTimeFieldSpec(columnName, dataType, columnMetadata.getDateTimeFormat(),
              columnMetadata.getDateTimeGranularity(), columnMetadata.getDateTimeType()));
          break;
        default:
          throw new IllegalStateException();
      }
    }

    return schema;
  }

  @Override
  public GenericRow next() {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow row) {
    for (Entry<String, Dictionary> entry : _dictionaryMap.entrySet()) {
      String columnName = entry.getKey();
      Dictionary dictionary = entry.getValue();
      if (_singleValueReaderMap.containsKey(columnName)) {
        row.putField(columnName, dictionary.get(_singleValueReaderMap.get(columnName).getInt(_nextRow)));
      } else {
        int numValues = _multiValueReaderMap.get(columnName).getIntArray(_nextRow, _multiValueBuffer);
        Object[] objectArray = new Object[numValues];
        for (int i = 0; i < numValues; i++) {
          objectArray[i] = dictionary.get(_multiValueBuffer[i]);
        }
        row.putField(columnName, objectArray);
      }
    }
    _nextRow++;
    return row;
  }

  @Override
  public void close() throws Exception {
    for (Dictionary dictionary : _dictionaryMap.values()) {
      dictionary.close();
    }
    for (SingleColumnSingleValueReader reader : _singleValueReaderMap.values()) {
      reader.close();
    }
    for (SingleColumnMultiValueReader reader : _multiValueReaderMap.values()) {
      reader.close();
    }
    _segmentMetadata.close();
  }
}
