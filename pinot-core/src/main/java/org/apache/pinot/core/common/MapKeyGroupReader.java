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
package org.apache.pinot.core.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.segment.index.map.MapKeyIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.utils.MapUtils.PreparedMapKey;


/// Reads several keys of one MAP column together, visiting each document once instead of once per key.
///
/// `SELECT attributes['a'], attributes['b'] FROM t` resolves each key to a column of its own, and the projection
/// reads columns one at a time. Each read walks the same documents and, for every one of them, fetches and traverses
/// the same serialized map - work repeated in full for every projected key. This holds the keys that share an
/// underlying forward index so they can be pulled out of each document in a single visit.
///
/// This reads; it does not remember. [DataBlockCache] decides when a block's values are still good, which is what it
/// already does for every other column.
///
/// One instance belongs to one [DataFetcher], so it inherits that object's lifetime and single-threaded access: a
/// `DataFetcher` serves one segment of one query on one thread, and closes its readers explicitly.
///
/// Only the STRING read path is grouped. That is what a projected map key resolves to for the string-valued MAP
/// columns this exists for, and it keeps the numeric and byte paths on their existing per-key readers.
public class MapKeyGroupReader implements AutoCloseable {
  private final ForwardIndexReader _forwardIndexReader;
  private final Map<String, String> _queryOptions;
  private final List<String> _columns = new ArrayList<>();
  private final List<PreparedMapKey> _keys = new ArrayList<>();
  /// Per key, the string [MapKeyIndexReader] substitutes when a document does not carry it. Applied here so that a
  /// grouped read is indistinguishable from the per-key read it replaces.
  private final List<String> _defaultValues = new ArrayList<>();

  private boolean _readerContextCreated;
  private ForwardIndexReaderContext _readerContext;
  private PreparedMapKey[] _keyArray;
  private String[] _scratch;

  MapKeyGroupReader(ForwardIndexReader forwardIndexReader, Map<String, String> queryOptions) {
    _forwardIndexReader = forwardIndexReader;
    _queryOptions = queryOptions;
  }

  void addKey(String column, MapKeyIndexReader reader) {
    _columns.add(column);
    _keys.add(reader.getMapKey());
    _defaultValues.add(reader.getDefaultNullValueString());
    // Keys register lazily as the projection resolves them, so a later one can land after this group has already
    // been read from. Rebuild the flattened arrays on the next read rather than leaving one short of a key.
    _keyArray = null;
  }

  /// The columns this group reads, in the order [#readStringValues] fills its output.
  List<String> getColumns() {
    return _columns;
  }

  /// True once grouping can save work. A lone key would gain nothing - one visit per document for one key is what
  /// its own reader already does.
  boolean isGrouped() {
    return _columns.size() > 1;
  }

  /// Fills `outValues[i]` with the values of [#getColumns]`.get(i)` for the given documents.
  void readStringValues(int[] docIds, int length, String[][] outValues) {
    int numKeys = _keys.size();
    if (_keyArray == null) {
      _keyArray = _keys.toArray(new PreparedMapKey[0]);
      _scratch = new String[numKeys];
    }
    ForwardIndexReaderContext readerContext = getReaderContext();
    for (int i = 0; i < length; i++) {
      _forwardIndexReader.getMapEntryValuesAsString(docIds[i], readerContext, _keyArray, _scratch);
      for (int k = 0; k < numKeys; k++) {
        String value = _scratch[k];
        outValues[k][i] = value != null ? value : _defaultValues.get(k);
      }
    }
  }

  private ForwardIndexReaderContext getReaderContext() {
    if (!_readerContextCreated) {
      // Through the query-options overload, exactly as ColumnValueReader does: a reader may use them to adjust
      // per-query behaviour, and the tiered-storage MAP readers do.
      _readerContext = _forwardIndexReader.createContext(_queryOptions);
      _readerContextCreated = true;
    }
    return _readerContext;
  }

  @Override
  public void close() {
    if (_readerContext != null) {
      _readerContext.close();
    }
  }
}
