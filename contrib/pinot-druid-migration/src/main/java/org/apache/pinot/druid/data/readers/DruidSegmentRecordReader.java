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
package org.apache.pinot.druid.data.readers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.readers.RecordReader;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.joda.time.chrono.ISOChronology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The DruidSegmentRecordReader allows us to convert all of the rows in a Druid segment file
 * into GenericRows, which are made into Pinot segments.
 *
 * Note that Druid uses LONG and not INT, so construct the Pinot schema accordingly.
 */
public class DruidSegmentRecordReader implements RecordReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(DruidSegmentRecordReader.class);

  private Schema _pinotSchema;
  private ArrayList<String> _columnNames;
  private Cursor _cursor;
  private List<BaseObjectColumnValueSelector> _selectors;
  private QueryableIndex _index;

  public DruidSegmentRecordReader(@Nonnull File indexDir, @Nullable Schema schema)
      throws IOException {
    init(indexDir, schema);
  }

  private void init(File indexDir, Schema schema)
      throws IOException {
    // Only the columns whose names are in the Pinot schema will get processed
    _pinotSchema = schema;

    ColumnConfig config = new DruidProcessingConfig() {
      @Override
      public String getFormatString() {
        return "processing-%s";
      }

      @Override
      public int intermediateComputeSizeBytes() {
        return 100 * 1024 * 1024;
      }

      @Override
      public int getNumThreads() {
        return 1;
      }

      @Override
      public int columnCacheSizeBytes() {
        return 25 * 1024 * 1024;
      }
    };

    ObjectMapper mapper = new DefaultObjectMapper();
    final IndexIO indexIO = new IndexIO(mapper, config);
    _index = indexIO.loadIndex(indexDir);
    QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(_index);

    // A Sequence "represents an iterable sequence of elements. Unlike normal Iterators however, it doesn't expose
    // a way for you to extract values from it, instead you provide it with a worker (an Accumulator) and that defines
    // what happens with the data."
    final Sequence<Cursor> cursors = adapter.makeCursors(
        Filters.toFilter(null),
        _index.getDataInterval().withChronology(ISOChronology.getInstanceUTC()),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    List<Cursor> cursorList = new ArrayList<>();
    final Sequence<Object> sequence = Sequences.map(cursors, new Function<Cursor, Object>() {
      @Override
      public Object apply(@Nullable Cursor cursor) {
        cursorList.add(cursor);
        return null;
      }
    });
    sequence.accumulate(null, (accumulated, in) -> null);

    // There should only be one single Cursor for every segment, so there should only be one Cursor in the cursorList
    Preconditions.checkArgument(cursorList.size() == 1, "There should only be one Cursor in the Sequence.");
    _cursor = cursorList.get(0);

    _columnNames = new ArrayList<>();
    _columnNames.addAll(_pinotSchema.getColumnNames());
    validateColumns(adapter);

    ColumnSelectorFactory columnSelectorFactory = _cursor.getColumnSelectorFactory();
    _selectors = _columnNames
        .stream()
        .map(columnSelectorFactory::makeColumnValueSelector)
        .collect(Collectors.toList());
  }

  //@Override
  public void init(SegmentGeneratorConfig segmentGeneratorConfig) {

  }

  @Override
  public boolean hasNext() {
    return !_cursor.isDone();
  }

  @Override
  public GenericRow next() throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) {
    // Only the columns whose names are in the Pinot schema will get processed
    for (int i = 0; i < _columnNames.size(); i++) {
      final String columnName = _columnNames.get(i);
      BaseObjectColumnValueSelector selector = _selectors.get(i);
      // If the column does not exist in the segment file, skip it.
      if (selector != null) {
        FieldSpec pinotFieldSpec = _pinotSchema.getFieldSpecFor(_columnNames.get(i));
        Object value = selector.getObject();
        if (value != null && !pinotFieldSpec.isSingleValueField()) {
          // Multi-valued dimensions in Druid are stored as Arrays.ArrayList (this has been checked)
          Preconditions.checkState(value instanceof List,
              String.format("The multi-valued dimension %s should be java.util.Arrays$ArrayList, but it is %s.", columnName, value.getClass()));
          // Store the multi-valued dimension as a String[] to follow Pinot format; null if empty
          value = ((List<String>) value).toArray(new String[0]);
        }
        reuse.putField(columnName, value);
      }
    }
    _cursor.advance();
    return reuse;
  }

  @Override
  public void rewind() {
    _cursor.reset();
  }

  @Override
  public Schema getSchema() {
    return _pinotSchema;
  }

  @Override
  public void close() {
    _index.close();
  }

  private boolean compareTypes(FieldSpec.DataType pinotColumnType, ValueType druidColumnType) {
    switch (pinotColumnType) {
      case INT:
        return false;
      case LONG:
        return druidColumnType == ValueType.LONG;
      case FLOAT:
        return druidColumnType == ValueType.FLOAT;
      case DOUBLE:
        return druidColumnType == ValueType.DOUBLE;
      case STRING:
        return druidColumnType == ValueType.STRING;
      case BOOLEAN:
      case BYTES:
      default:
        throw new UnsupportedOperationException("Unsupported Druid type: " + pinotColumnType.name());
    }
  }

  private void validateColumns(QueryableIndexStorageAdapter adapter) {
    for (int i = 0; i < _columnNames.size(); i++) {
      final String columnName = _columnNames.get(i);
      ColumnCapabilities druidColumnCapabilities = adapter.getColumnCapabilities(columnName);
      if (druidColumnCapabilities == null) {
        LOGGER.warn("Column {} is not in record", columnName);
      } else {
        if (druidColumnCapabilities.getType() == ValueType.COMPLEX) {
          throw new IllegalArgumentException(
              String.format("Column %s: DruidSegmentRecordReader does not support complex metric columns.", columnName));
        }

        FieldSpec fieldSpec = _pinotSchema.getFieldSpecFor(_columnNames.get(i));
        if (!fieldSpec.isSingleValueField() && fieldSpec.getDataType() != FieldSpec.DataType.STRING) {
          throw new IllegalArgumentException(
              String.format("Column %s: DruidSegmentRecordReader does not support non-STRING multi-value dimensions.", columnName));
        }
        if (fieldSpec.isSingleValueField() && druidColumnCapabilities.hasMultipleValues()) {
          throw new IllegalArgumentException(
              String.format("Column %s: Column in Pinot schema is single-valued, but column in record is multi-valued.", columnName));
        }
        if (!fieldSpec.isSingleValueField() && !druidColumnCapabilities.hasMultipleValues()) {
          throw new IllegalArgumentException(String.format("Column %s: Column in Pinot schema is multi-valued, but column in record is single-valued.", columnName));
        }
        if (!compareTypes(_pinotSchema.getFieldSpecFor(columnName).getDataType(), druidColumnCapabilities.getType())) {
          throw new IllegalArgumentException(
              String.format("Column %s: Type in schema (%s) does not match type in record (%s).",
                  columnName, _pinotSchema.getFieldSpecFor(columnName).getDataType(), druidColumnCapabilities.getType()));
        }
      }
    }
  }
}
