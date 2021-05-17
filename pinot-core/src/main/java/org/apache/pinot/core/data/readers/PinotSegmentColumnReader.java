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
package org.apache.pinot.core.data.readers;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReaderContext;
import org.apache.pinot.core.segment.index.readers.NullValueVectorReader;


@SuppressWarnings({"rawtypes", "unchecked"})
public class PinotSegmentColumnReader implements Closeable {
  private final ForwardIndexReader _forwardIndexReader;
  private final ForwardIndexReaderContext _forwardIndexReaderContext;
  private final Dictionary _dictionary;
  private final NullValueVectorReader _nullValueVectorReader;
  private final int[] _dictIdBuffer;

  public PinotSegmentColumnReader(IndexSegment indexSegment, String column) {
    DataSource dataSource = indexSegment.getDataSource(column);
    Preconditions.checkArgument(dataSource != null, "Failed to find data source for column: %s", column);
    _forwardIndexReader = dataSource.getForwardIndex();
    _forwardIndexReaderContext = _forwardIndexReader.createContext();
    _dictionary = dataSource.getDictionary();
    _nullValueVectorReader = dataSource.getNullValueVector();
    if (_forwardIndexReader.isSingleValue()) {
      _dictIdBuffer = null;
    } else {
      _dictIdBuffer = new int[dataSource.getDataSourceMetadata().getMaxNumValuesPerMVEntry()];
    }
  }

  public PinotSegmentColumnReader(ForwardIndexReader forwardIndexReader, @Nullable Dictionary dictionary,
      @Nullable NullValueVectorReader nullValueVectorReader, int maxNumValuesPerMVEntry) {
    _forwardIndexReader = forwardIndexReader;
    _forwardIndexReaderContext = _forwardIndexReader.createContext();
    _dictionary = dictionary;
    _nullValueVectorReader = nullValueVectorReader;
    if (_forwardIndexReader.isSingleValue()) {
      _dictIdBuffer = null;
    } else {
      _dictIdBuffer = new int[maxNumValuesPerMVEntry];
    }
  }

  public boolean isSingleValue() {
    return _forwardIndexReader.isSingleValue();
  }

  public boolean hasDictionary() {
    return _dictionary != null;
  }

  public Dictionary getDictionary() {
    return _dictionary;
  }

  public int getDictId(int docId) {
    return _forwardIndexReader.getDictId(docId, _forwardIndexReaderContext);
  }

  public Object getValue(int docId) {
    if (_dictionary != null) {
      if (_forwardIndexReader.isSingleValue()) {
        return _dictionary.get(_forwardIndexReader.getDictId(docId, _forwardIndexReaderContext));
      } else {
        int numValues = _forwardIndexReader.getDictIdMV(docId, _dictIdBuffer, _forwardIndexReaderContext);
        Object[] values = new Object[numValues];
        for (int i = 0; i < numValues; i++) {
          values[i] = _dictionary.get(_dictIdBuffer[i]);
        }
        return values;
      }
    } else {
      // NOTE: Only support single-value raw index
      assert _forwardIndexReader.isSingleValue();

      switch (_forwardIndexReader.getValueType()) {
        case INT:
          return _forwardIndexReader.getInt(docId, _forwardIndexReaderContext);
        case LONG:
          return _forwardIndexReader.getLong(docId, _forwardIndexReaderContext);
        case FLOAT:
          return _forwardIndexReader.getFloat(docId, _forwardIndexReaderContext);
        case DOUBLE:
          return _forwardIndexReader.getDouble(docId, _forwardIndexReaderContext);
        case STRING:
          return _forwardIndexReader.getString(docId, _forwardIndexReaderContext);
        case BYTES:
          return _forwardIndexReader.getBytes(docId, _forwardIndexReaderContext);
        default:
          throw new IllegalStateException();
      }
    }
  }

  public boolean isNull(int docId) {
    return _nullValueVectorReader != null && _nullValueVectorReader.isNull(docId);
  }

  @Override
  public void close()
      throws IOException {
    if (_forwardIndexReaderContext != null) {
      _forwardIndexReaderContext.close();
    }
  }
}
