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
package org.apache.pinot.segment.spi.index.reader;

import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/// Reader for vector index.
public interface VectorIndexReader extends IndexReader {
  /// Returns the bitmap of top k closest vectors from the given vector.
  /// @param vector vector to search
  /// @param topK number of closest vectors to return
  /// @return bitmap of top k closest vectors
  ImmutableRoaringBitmap getDocIds(float[] vector, int topK);

  /// Returns the distance of each of the top k closest vectors from the given vector, computed by the index, keyed by
  /// doc id. Distances use the column's configured distance function and match the corresponding distance function
  /// (e.g. `cosineDistance`). Returns `null` when the index cannot provide distances.
  ///
  /// The returned map may also hold documents beyond the top k (e.g. from an earlier filtered search with the same
  /// vector); callers must compute the distance of a document missing from the map themselves.
  @Nullable
  default Int2FloatMap getDocDistances(float[] vector, int topK) {
    return null;
  }

  /// Returns a map of effective runtime state for debug and explain output.
  /// Implementations should report actual loaded values, not just requested config.
  ///
  /// @return map of debug key-value pairs, or empty map if not supported
  default Map<String, Object> getIndexDebugInfo() {
    return Map.of();
  }
}
