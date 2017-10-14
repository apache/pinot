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

package com.linkedin.pinot.core.segment.virtualcolumn;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.core.io.reader.BaseSingleColumnSingleValueReader;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.reader.impl.ChunkReaderContext;
import com.linkedin.pinot.core.io.reader.impl.v1.SortedIndexReader;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.readers.BaseDictionary;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import java.io.IOException;


/**
 * Virtual column provider that returns the current document id.
 */
public class DocIdVirtualColumnProvider extends BaseVirtualColumnProvider {
  @Override
  public DataFileReader buildReader(VirtualColumnContext context) {
    return new DocIdSingleValueReader();
  }

  @Override
  public Dictionary buildDictionary(VirtualColumnContext context) {
    return new DocIdDictionary(context.getTotalDocCount());
  }

  @Override
  public ColumnMetadata buildMetadata(VirtualColumnContext context) {
    ColumnMetadata.Builder columnMetadataBuilder = new ColumnMetadata.Builder()
        .setColumnName(context.getColumnName())
        .setCardinality(context.getTotalDocCount())
        .setHasDictionary(true)
        .setHasInvertedIndex(true)
        .setFieldType(FieldSpec.FieldType.DIMENSION)
        .setDataType(FieldSpec.DataType.INT)
        .setSingleValue(true)
        .setIsSorted(true)
        .setTotalDocs(context.getTotalDocCount());

    return columnMetadataBuilder.build();
  }

  @Override
  public InvertedIndexReader buildInvertedIndex(VirtualColumnContext context) {
    return new DocIdInvertedIndex();
  }

  private class DocIdSingleValueReader extends BaseSingleColumnSingleValueReader<ChunkReaderContext> {
    @Override
    public ChunkReaderContext createContext() {
      return null;
    }

    @Override
    public int getInt(int row) {
      return row;
    }

    @Override
    public int getInt(int rowId, ChunkReaderContext context) {
      return rowId;
    }

    @Override
    public long getLong(int row) {
      return row;
    }

    @Override
    public long getLong(int rowId, ChunkReaderContext context) {
      return rowId;
    }

    @Override
    public void readValues(int[] rows, int rowStartPos, int rowSize, int[] values, int valuesStartPos) {
      System.arraycopy(rows, rowStartPos, values, valuesStartPos, rowSize);
    }

    @Override
    public void close() throws IOException {
    }
  }

  private class DocIdInvertedIndex implements SortedIndexReader {
    @Override
    public Pairs.IntPair getDocIds(int dictId) {
      return new Pairs.IntPair(dictId, dictId);
    }

    @Override
    public void close() throws IOException {
    }
  }

  private class DocIdDictionary extends BaseDictionary {
    private int _length;

    public DocIdDictionary(int length) {
      _length = length;
    }

    @Override
    public int indexOf(Object rawValue) {
      if (rawValue instanceof Number) {
        return ((Number) rawValue).intValue();
      }

      if (rawValue instanceof String) {
        try {
          return Integer.parseInt((String) rawValue);
        } catch (NumberFormatException e) {
          return -1;
        }
      }

      return -1;
    }

    @Override
    public Object get(int dictId) {
      return dictId;
    }

    @Override
    public int getIntValue(int dictId) {
      return dictId;
    }

    @Override
    public long getLongValue(int dictId) {
      return dictId;
    }

    @Override
    public float getFloatValue(int dictId) {
      return dictId;
    }

    @Override
    public double getDoubleValue(int dictId) {
      return dictId;
    }

    @Override
    public String getStringValue(int dictId) {
      return Integer.toString(dictId);
    }

    @Override
    public int length() {
      return _length;
    }

    @Override
    public void close() throws IOException {
    }
  }
}
