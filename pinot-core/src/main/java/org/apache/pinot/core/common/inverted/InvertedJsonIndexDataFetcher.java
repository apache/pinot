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
package org.apache.pinot.core.common.inverted;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.local.realtime.impl.json.MutableJsonIndexImpl;
import org.apache.pinot.segment.local.segment.index.readers.json.ImmutableJsonIndexReader;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public class InvertedJsonIndexDataFetcher implements InvertedDataFetcher {
  private MutableJsonIndexImpl _mutableJsonIndex;
  private ImmutableJsonIndexReader _immutableJsonIndexReader;
  private int _startDictId = -1;
  private final String _keyInIndex;

  public InvertedJsonIndexDataFetcher(DataSource dataSource, String jsonPath) {
    if (dataSource.getJsonIndex() instanceof MutableJsonIndexImpl) {
      _mutableJsonIndex = (MutableJsonIndexImpl) dataSource.getJsonIndex();
    } else if (dataSource.getJsonIndex() instanceof ImmutableJsonIndexReader) {
      _immutableJsonIndexReader = (ImmutableJsonIndexReader) dataSource.getJsonIndex();
    } else {
      throw new IllegalStateException("Unknown JsonIndex implementation found");
    }
    _keyInIndex = jsonPath.substring(1);
  }

  @Override
  public Object[] getValues() {
    if (_mutableJsonIndex != null) {
      return _mutableJsonIndex.getValues(_keyInIndex);
    }
    Pair<Object[], Integer> valuesAndStartDictId = _immutableJsonIndexReader.getValues(_keyInIndex);
    _startDictId = valuesAndStartDictId.getRight();
    return valuesAndStartDictId.getLeft();
  }

  @Override
  public ImmutableRoaringBitmap getDocIds(int dictId) {
    return _immutableJsonIndexReader.getDocIds(dictId + _startDictId);
  }

  @Override
  public ImmutableRoaringBitmap getDocIds(Object value) {
    return _mutableJsonIndex.getDocIds(_keyInIndex, (String) value);
  }

  @Override
  public boolean supportsDictId() {
    return _immutableJsonIndexReader != null;
  }
}
