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
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.reader.impl.v1.SortedIndexReader;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.readers.BaseDictionary;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import java.io.IOException;


/**
 * Virtual column provider for a virtual column that contains a single string.
 */
public abstract class SingleStringVirtualColumnProvider extends BaseVirtualColumnProvider {
  protected abstract String getValue(VirtualColumnContext context);

  @Override
  public DataFileReader buildReader(VirtualColumnContext context) {
    return new IntSingleValueDataFileReader(0);
  }

  @Override
  public Dictionary buildDictionary(VirtualColumnContext context) {
    return new SingleStringDictionary(context.getTotalDocCount(), context);
  }

  @Override
  public ColumnMetadata buildMetadata(VirtualColumnContext context) {
    ColumnMetadata.Builder columnMetadataBuilder = new ColumnMetadata.Builder()
        .setColumnName(context.getColumnName())
        .setCardinality(1)
        .setHasDictionary(true)
        .setHasInvertedIndex(true)
        .setFieldType(FieldSpec.FieldType.DIMENSION)
        .setDataType(FieldSpec.DataType.STRING)
        .setSingleValue(true)
        .setIsSorted(true)
        .setTotalDocs(context.getTotalDocCount());

    return columnMetadataBuilder.build();
  }

  @Override
  public InvertedIndexReader buildInvertedIndex(VirtualColumnContext context) {
    return new SingleStringInvertedIndex(context.getTotalDocCount());
  }

  private class SingleStringInvertedIndex implements SortedIndexReader {
    private int _length;

    public SingleStringInvertedIndex(int length) {
      _length = length;
    }

    @Override
    public Pairs.IntPair getDocIds(int dictId) {
      if (dictId == 0) {
        return new Pairs.IntPair(0, _length);
      } else {
        return new Pairs.IntPair(-1, -1);
      }
    }

    @Override
    public void close() throws IOException {
    }
  }

  private class SingleStringDictionary extends BaseDictionary {
    private int _length;
    private VirtualColumnContext _context;

    public SingleStringDictionary(int length, VirtualColumnContext context) {
      _length = length;
      _context = context;
    }

    @Override
    public int getIntValue(int dictId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLongValue(int dictId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public float getFloatValue(int dictId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public double getDoubleValue(int dictId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int length() {
      return _length;
    }

    @Override
    public int indexOf(Object rawValue) {
      if (rawValue.equals(getValue(_context))) {
        return 0;
      } else {
        return -1;
      }
    }

    @Override
    public Object get(int dictId) {
      return getValue(_context);
    }

    @Override
    public String getStringValue(int dictId) {
      return getValue(_context);
    }

    @Override
    public void close() throws IOException {
    }
  }
}
