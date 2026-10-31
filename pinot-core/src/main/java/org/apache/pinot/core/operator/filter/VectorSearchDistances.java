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
package org.apache.pinot.core.operator.filter;

import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;


/// Hands the distances computed by a vector index search from [VectorSimilarityFilterOperator] to `VECTOR_DISTANCE`
/// on the same segment, column and query vector, so the projection does not recompute them from the forward index.
///
/// One instance lives in the [QueryContext] of each query, so the distances are released with the query and never
/// shared across queries. `VECTOR_DISTANCE` requests distances while the query is planned; the filter fills a request
/// only when one exists, so queries that do not project `VECTOR_DISTANCE` collect nothing.
public final class VectorSearchDistances {
  private final ConcurrentHashMap<Key, Request> _requests = new ConcurrentHashMap<>();

  /// Returns the instance of the given query.
  public static VectorSearchDistances of(QueryContext queryContext) {
    return queryContext.getOrComputeSharedValue(VectorSearchDistances.class, VectorSearchDistances.class,
        k -> new VectorSearchDistances());
  }

  /// Requests the distances of the next search of `reader` for `vector`. Called while the query is planned.
  public Request request(VectorIndexReader reader, float[] vector) {
    return _requests.computeIfAbsent(new Key(reader, vector), k -> new Request());
  }

  /// Returns the request for the distances of a search of `reader` for `vector`, or `null` when none was made.
  @Nullable
  public Request getRequest(VectorIndexReader reader, float[] vector) {
    return _requests.get(new Key(reader, vector));
  }

  /// Distances of one search, keyed by doc id. The filter sets them before the projection reads them, both on the
  /// thread that processes the segment.
  public static final class Request {
    @Nullable
    private volatile Int2FloatMap _distances;

    public void setDistances(Int2FloatMap distances) {
      _distances = distances;
    }

    /// Returns the distances of the search, or `null` when the search has not run or the index returned none.
    @Nullable
    public Int2FloatMap getDistances() {
      return _distances;
    }
  }

  /// Identifies a search by the reader instance (one per segment and column) and the query vector.
  private static final class Key {
    private final VectorIndexReader _reader;
    private final float[] _vector;
    private final int _hashCode;

    Key(VectorIndexReader reader, float[] vector) {
      _reader = reader;
      _vector = vector;
      _hashCode = 31 * System.identityHashCode(reader) + Arrays.hashCode(vector);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Key)) {
        return false;
      }
      Key key = (Key) o;
      return _reader == key._reader && Arrays.equals(_vector, key._vector);
    }

    @Override
    public int hashCode() {
      return _hashCode;
    }
  }
}
