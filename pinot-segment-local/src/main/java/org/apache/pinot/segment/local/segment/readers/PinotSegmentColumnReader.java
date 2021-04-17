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
package org.apache.pinot.segment.local.segment.readers;

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;


@SuppressWarnings({"rawtypes", "unchecked"})
public class PinotSegmentColumnReader implements Closeable {
  private final ForwardIndexReader _reader;
  private final ForwardIndexReaderContext _readerContext;
  private final Dictionary _dictionary;
  private final int[] _dictIdBuffer;

  public PinotSegmentColumnReader(ImmutableSegment immutableSegment, String column) {
    DataSource dataSource = immutableSegment.getDataSource(column);
    _reader = dataSource.getForwardIndex();
    _readerContext = _reader.createContext();
    _dictionary = dataSource.getDictionary();
    if (_reader.isSingleValue()) {
      _dictIdBuffer = null;
    } else {
      _dictIdBuffer = new int[dataSource.getDataSourceMetadata().getMaxNumValuesPerMVEntry()];
    }
  }

  public PinotSegmentColumnReader(ForwardIndexReader reader, @Nullable Dictionary dictionary,
      int maxNumValuesPerMVEntry) {
    _reader = reader;
    _readerContext = _reader.createContext();
    _dictionary = dictionary;
    if (_reader.isSingleValue()) {
      _dictIdBuffer = null;
    } else {
      _dictIdBuffer = new int[maxNumValuesPerMVEntry];
    }
  }

  public boolean hasDictionary() {
    return _dictionary != null;
  }

  public int getDictId(int docId) {
    return _reader.getDictId(docId, _readerContext);
  }

  public Object getValue(int docId) {
    if (_dictionary != null) {
      if (_reader.isSingleValue()) {
        return _dictionary.get(_reader.getDictId(docId, _readerContext));
      } else {
        int numValues = _reader.getDictIdMV(docId, _dictIdBuffer, _readerContext);
        Object[] values = new Object[numValues];
        for (int i = 0; i < numValues; i++) {
          values[i] = _dictionary.get(_dictIdBuffer[i]);
        }
        return values;
      }
    } else {
      // NOTE: Only support single-value raw index
      assert _reader.isSingleValue();

      switch (_reader.getValueType()) {
        case INT:
          return _reader.getInt(docId, _readerContext);
        case LONG:
          return _reader.getLong(docId, _readerContext);
        case FLOAT:
          return _reader.getFloat(docId, _readerContext);
        case DOUBLE:
          return _reader.getDouble(docId, _readerContext);
        case STRING:
          return _reader.getString(docId, _readerContext);
        case BYTES:
          return _reader.getBytes(docId, _readerContext);
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (_readerContext != null) {
      _readerContext.close();
    }
  }
}
