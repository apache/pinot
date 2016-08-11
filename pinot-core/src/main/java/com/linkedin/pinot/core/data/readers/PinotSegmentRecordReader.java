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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.BaseRecordReader;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;
import com.linkedin.pinot.core.io.reader.impl.SortedForwardIndexReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitMultiValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.DoubleDictionary;
import com.linkedin.pinot.core.segment.index.readers.FloatDictionary;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.core.segment.index.readers.IntDictionary;
import com.linkedin.pinot.core.segment.index.readers.LongDictionary;
import com.linkedin.pinot.core.segment.index.readers.StringDictionary;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import com.linkedin.pinot.core.segment.store.SegmentDirectory.Reader;

/**
 * Record reader to read pinot segment and generate GenericRows
 */
public class PinotSegmentRecordReader extends BaseRecordReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentRecordReader.class);

  private SegmentMetadataImpl segmentMetadata;
  private int totalDocs;
  private Set<String> columns;

  private Map<String, SingleColumnSingleValueReader> singleValueReaderMap;
  private Map<String, SingleColumnMultiValueReader> multiValueReaderMap;
  private Map<String, SortedForwardIndexReader> singleValueSortedReaderMap;

  private Map<String, Dictionary> pinotDictionaryBufferMap;

  private Map<String, DataType> columnDataTypeMap;
  private Map<String, Object> multiValueArrayMap;

  private Map<String, Boolean> isSingleValueMap;
  private Map<String, Boolean> isSortedMap;

  private int docNumber;

  public PinotSegmentRecordReader(File segmentIndexDir)  throws IOException, ConfigurationException {

    segmentMetadata = new SegmentMetadataImpl(segmentIndexDir);
    SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(segmentIndexDir, segmentMetadata, ReadMode.heap);

    totalDocs = segmentMetadata.getTotalDocs();
    columns = segmentMetadata.getAllColumns();

    Reader reader = segmentDirectory.createReader();
    singleValueReaderMap = new HashMap<>();
    multiValueReaderMap = new HashMap<>();
    singleValueSortedReaderMap = new HashMap<>();

    pinotDictionaryBufferMap = new HashMap<>();
    columnDataTypeMap = new HashMap<>();
    multiValueArrayMap = new HashMap<>();

    isSingleValueMap = new HashMap<>();
    isSortedMap = new HashMap<>();

    for (String column : columns) {

      ColumnMetadata columnMetadataFor = segmentMetadata.getColumnMetadataFor(column);

      isSingleValueMap.put(column, columnMetadataFor.isSingleValue());
      isSortedMap.put(column, columnMetadataFor.isSorted());

      if (columnMetadataFor.isSingleValue() && !columnMetadataFor.isSorted()) {
        PinotDataBuffer fwdIndexBuffer = reader.getIndexFor(column, ColumnIndexType.FORWARD_INDEX);
        SingleColumnSingleValueReader fwdIndexReader =
            new FixedBitSingleValueReader(fwdIndexBuffer, columnMetadataFor.getTotalDocs(),
                columnMetadataFor.getBitsPerElement(), columnMetadataFor.hasNulls());
        singleValueReaderMap.put(column, fwdIndexReader);

      } else if (columnMetadataFor.isSingleValue() && columnMetadataFor.isSorted()) {
        PinotDataBuffer dataBuffer = reader.getIndexFor(column, ColumnIndexType.FORWARD_INDEX);
        FixedByteSingleValueMultiColReader indexReader = new FixedByteSingleValueMultiColReader(
            dataBuffer, columnMetadataFor.getCardinality(), 2, new int[] {
            4, 4
        });
        SortedForwardIndexReader fwdIndexReader = new SortedForwardIndexReader(indexReader, totalDocs);
        singleValueSortedReaderMap.put(column, fwdIndexReader);

      } else {
        PinotDataBuffer fwdIndexBuffer = reader.getIndexFor(column, ColumnIndexType.FORWARD_INDEX);
        SingleColumnMultiValueReader fwdIndexReader =
            new FixedBitMultiValueReader(fwdIndexBuffer, segmentMetadata.getTotalDocs(),
                columnMetadataFor.getTotalNumberOfEntries(), columnMetadataFor.getBitsPerElement(), false);
        multiValueReaderMap.put(column, fwdIndexReader);
      }
      DataType dataType = columnMetadataFor.getDataType();
      PinotDataBuffer dictionaryBuffer = reader.getIndexFor(column, ColumnIndexType.DICTIONARY);

      switch (dataType) {
        case BOOLEAN:
          pinotDictionaryBufferMap.put(column, new StringDictionary(dictionaryBuffer, columnMetadataFor));
          break;
        case DOUBLE:
          pinotDictionaryBufferMap.put(column, new DoubleDictionary(dictionaryBuffer, columnMetadataFor));
          break;
        case FLOAT:
          pinotDictionaryBufferMap.put(column, new FloatDictionary(dictionaryBuffer, columnMetadataFor));
          break;
        case INT:
          pinotDictionaryBufferMap.put(column, new IntDictionary(dictionaryBuffer, columnMetadataFor));
          break;
        case LONG:
          pinotDictionaryBufferMap.put(column, new LongDictionary(dictionaryBuffer, columnMetadataFor));
          break;
        case STRING:
          pinotDictionaryBufferMap.put(column, new StringDictionary(dictionaryBuffer, columnMetadataFor));
          break;
        case INT_ARRAY:
        case BYTE:
        case BYTE_ARRAY:
        case CHAR:
        case CHAR_ARRAY:
        case DOUBLE_ARRAY:
        case FLOAT_ARRAY:
        case LONG_ARRAY:
        case OBJECT:
        case SHORT:
        case SHORT_ARRAY:
        case STRING_ARRAY:
        default:
          LOGGER.error("Unsupported data type {}", dataType);
          break;
      }
      if (!isSingleValueMap.get(column)) {
        int[] intArray = new int[columnMetadataFor.getMaxNumberOfMultiValues()];
        multiValueArrayMap.put(column, intArray);
      }
      columnDataTypeMap.put(column, dataType);
    }
  }


  @Override
  public void init() throws Exception {
    docNumber = 0;
  }

  @Override
  public void rewind() throws Exception {
    init();
  }


  @Override
  public boolean hasNext() {
    return docNumber < totalDocs;
  }

  @Override
  public Schema getSchema() {

    Schema schema = new Schema();
    schema.setSchemaName(segmentMetadata.getName());

    for (String column : columns) {
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      String columnName = columnMetadata.getColumnName();
      DataType dataType = columnMetadata.getDataType();
      FieldType fieldType = columnMetadata.getFieldType();
      FieldSpec fieldSpec = null;

      switch(fieldType) {
        case DIMENSION:
          boolean isSingleValue = columnMetadata.isSingleValue();
          fieldSpec = new DimensionFieldSpec(columnName, dataType, isSingleValue);
          break;
        case METRIC:
          fieldSpec = new MetricFieldSpec(columnName, dataType);
          break;
        case TIME:
          TimeUnit timeType = columnMetadata.getTimeunit();
          TimeGranularitySpec incominGranularitySpec = new TimeGranularitySpec(dataType, timeType, columnName);
          fieldSpec = new TimeFieldSpec(incominGranularitySpec);
          break;
        default:
          break;
      }
      schema.addField(columnName, fieldSpec);
    }
    return schema;
  }

  @Override
  public GenericRow next() {
    Map<String, Object> fields = new HashMap<>();

    for (String column : columns) {

      if (isSingleValueMap.get(column)) { // Single value
        Dictionary dictionary = null;
        int dictionaryId;

        if (!isSortedMap.get(column)) {
          SingleColumnSingleValueReader singleValueReader = singleValueReaderMap.get(column);
          dictionary = pinotDictionaryBufferMap.get(column);
          dictionaryId = singleValueReader.getInt(docNumber);
        } else {
          SortedForwardIndexReader svSortedReader = singleValueSortedReaderMap.get(column);
          dictionary = pinotDictionaryBufferMap.get(column);
          dictionaryId = svSortedReader.getInt(docNumber);
        }
        if (dictionary == null) {
          throw new IllegalStateException("Dictionary not found for " + column);
        }
        fields.put(column, dictionary.get(dictionaryId));

      } else { // Multi value
        SingleColumnMultiValueReader mvReader = multiValueReaderMap.get(column);
        int[] dictionaryIdArray = (int[]) multiValueArrayMap.get(column);
        mvReader.getIntArray(docNumber, dictionaryIdArray);
        Dictionary dictionary = pinotDictionaryBufferMap.get(column);

        Object[] objectArray = new Object[dictionaryIdArray.length];
        for (int i = 0; i < dictionaryIdArray.length; i ++) {
          objectArray[i] = dictionary.get(dictionaryIdArray[i]);
        }
        fields.put(column, objectArray);
      }
    }
    GenericRow row = new GenericRow();
    row.init(fields);
    docNumber ++;

    return row;
  }

  @Override
  public void close() throws Exception {
    for (Entry<String, Dictionary> entry : pinotDictionaryBufferMap.entrySet()) {
      ImmutableDictionaryReader dictionary = (ImmutableDictionaryReader) entry.getValue();
      if (dictionary != null) {
        dictionary.close();
      }
    }
    for (Entry<String, SingleColumnSingleValueReader> entry : singleValueReaderMap.entrySet()) {
      SingleColumnSingleValueReader reader = entry.getValue();
      if (reader != null) {
        reader.close();
      }
    }
    for (Entry<String, SortedForwardIndexReader> entry : singleValueSortedReaderMap.entrySet()) {
      SortedForwardIndexReader reader = entry.getValue();
      if (reader != null) {
        reader.close();
      }
    }
    for (Entry<String, SingleColumnMultiValueReader> entry : multiValueReaderMap.entrySet()) {
      SingleColumnMultiValueReader reader = entry.getValue();
      if (reader != null) {
        reader.close();
      }
    }
    segmentMetadata.close();
  }

}
