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
package org.apache.pinot.core.segment.virtualcolumn;

import java.io.IOException;
import org.apache.pinot.common.utils.Pairs;
import org.apache.pinot.core.io.reader.BaseSingleColumnSingleValueReader;
import org.apache.pinot.core.io.reader.DataFileReader;
import org.apache.pinot.core.io.reader.impl.ChunkReaderContext;
import org.apache.pinot.core.io.reader.impl.v1.SortedIndexSingleValueReader;
import org.apache.pinot.core.io.reader.impl.v1.SortedIndexSingleValueReaderImpl;
import org.apache.pinot.core.segment.index.ColumnMetadata;
import org.apache.pinot.core.segment.index.column.BaseColumnProvider;
import org.apache.pinot.core.segment.index.column.ColumnContext;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.DocIdDictionary;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;


/**
 * Virtual column provider that returns the current document id.
 */
public class DocIdVirtualColumnProvider extends BaseColumnProvider {

  ColumnMetadata _columnMetadata;

  @Override
  public DataFileReader buildReader(ColumnContext context) {
    return new DocIdSingleValueReader();
  }

  @Override
  public Dictionary buildDictionary(ColumnContext context) {
    return new DocIdDictionary(context.getTotalDocCount());
  }

  @Override
  public ColumnMetadata buildMetadata(ColumnContext context) {
    ColumnMetadata.Builder columnMetadataBuilder = super.getColumnMetadataBuilder(context);
    columnMetadataBuilder.setCardinality(context.getTotalDocCount()).setHasDictionary(true).setHasInvertedIndex(true)
        .setSingleValue(true).setIsSorted(true);
    _columnMetadata = columnMetadataBuilder.build();
    return _columnMetadata;
  }

  @Override
  public InvertedIndexReader buildInvertedIndex(ColumnContext context) {
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
    public void close()
        throws IOException {
    }
  }

  private class DocIdInvertedIndex extends BaseSingleColumnSingleValueReader<SortedIndexSingleValueReaderImpl.Context> implements SortedIndexSingleValueReader<SortedIndexSingleValueReaderImpl.Context> {
    @Override
    public Pairs.IntPair getDocIds(int dictId) {
      return new Pairs.IntPair(dictId, dictId);
    }

    @Override
    public Pairs.IntPair getDocIds(Object value) {
      // This should not be called from anywhere. If it happens, there is a bug
      // and that's why we throw illegal state exception
      throw new IllegalStateException("sorted inverted index reader supports lookup only using dictionary id");
    }

    @Override
    public void close()
        throws IOException {
    }

    @Override
    public SortedIndexSingleValueReaderImpl.Context createContext() {
      return null;
    }

    @Override
    public int getInt(int row) {
      return row;
    }

    @Override
    public int getInt(int rowId, SortedIndexSingleValueReaderImpl.Context context) {
      return rowId;
    }
  }
}
