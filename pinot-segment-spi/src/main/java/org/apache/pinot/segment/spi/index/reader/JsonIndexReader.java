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

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Reader for json index.
 */
public interface JsonIndexReader extends IndexReader {

  MutableRoaringBitmap getMatchingDocIds(String filterString);

  /**
   * Returns the matching document ids for the given filter Context.
   */
  MutableRoaringBitmap getMatchingDocIds(Object filterCtx);

  /**
   * Returns the matching document ids for the given filter.
   * @param flatDocCountFilter predicate used to filter results based on number of matched flattened document
   */
  default MutableRoaringBitmap getMatchingDocIds(String documentFilter, String flatDocCountFilter) {
    return getMatchingDocIds(documentFilter);
  }

  /**
   * For an array of docIds and context specific to a JSON key, returns the corresponding sv value for each docId.
   * @param docIds array of docIds
   * @param length length of the array
   * @param matchingValueToDocs Map from each unique value for the jsonPathKey value to the flattened docId
   *                                     posting list
   * @param isFlattenedDocIds whether the docIds are flattened or unflattened
   * @return String[] where String[i] is the sv value for docIds[i]
   */
  String[] getValuesSV(int[] docIds, int length, Map<String, RoaringBitmap> matchingValueToDocs,
      boolean isFlattenedDocIds);

  /**
   * For an array of docIds and context specific to a JSON key, returns the corresponding mv array for each docId.
   * @param docIds array of docIds
   * @param length length of the array
   * @param matchingValueToFlattenedDocs Map from each unique value for the jsonPathKey value to the flattened docId
   *                                     posting list
   * @return String[][] where String[i] is the mv array for docIds[i]
   */
  String[][] getValuesMV(int[] docIds, int length, Map<String, RoaringBitmap> matchingValueToFlattenedDocs);

  /**
   * For a JSON key, returns a Map from each value to the flattened docId posting list. This map should be used to
   * avoid reading and converting the posting list of flattened docIds to real docIds
   */
  Map<String, RoaringBitmap> getMatchingFlattenedDocsMap(String key, @Nullable String filterJsonString);

  /**
   * Converts the flattened docIds to real docIds using the map returned by getMatchingFlattenedDocsMap
   */
  void convertFlattenedDocIdsToDocIds(Map<String, RoaringBitmap> flattenedDocIdsMap);

  /**
   * Returns only the distinct value strings for the given key that match the optional filter, without materializing
   * per-value posting list bitmaps or converting flattened doc IDs. This is significantly faster than
   * {@link #getMatchingFlattenedDocsMap} followed by {@link #convertFlattenedDocIdsToDocIds} when only the distinct
   * values are needed (e.g. fully-pushed-down DISTINCT queries).
   *
   * <p>The default implementation delegates to {@link #getMatchingFlattenedDocsMap} and returns its key set.
   * Implementations should override for better performance.
   */
  default Set<String> getMatchingDistinctValues(String key, @Nullable String filterJsonString) {
    return getMatchingFlattenedDocsMap(key, filterJsonString).keySet();
  }

  /**
   * Returns true if the given JSON path is indexed and can be used for index-based operations
   * (e.g. getMatchingFlattenedDocsMap, JsonIndexDistinctOperator).
   * OSS JSON index indexes all paths, so always returns true. Composite JSON index with
   * selective indexing returns true only for paths in invertedIndexConfigs.
   */
  default boolean isPathIndexed(String jsonPath) {
    return true;
  }

  /**
   * Returns a non-negative upper bound on the number of distinct values seen at the given JSON path across all
   * documents in the segment. Implementations are free to return the exact count when it is cheap to compute; a
   * looser upper bound is permitted (and required for some array-indexed paths, see below).
   *
   * <p>The OSS immutable implementation answers scalar / wildcard paths in O(log N) via the sorted dictionary
   * range. Array-indexed paths (for example {@code $.items[0].name}) fall back to the materializing default and
   * return the union of distinct values across all indices, which is a strict upper bound on the per-index count.
   * The default implementation materializes the full value set via {@link #getMatchingDistinctValues}; it is
   * correct but intentionally slow on high-cardinality paths.
   *
   * <p>Callers use this value as a cardinality estimate to decide whether a dictionary-scan execution plan is
   * cheaper than a row-by-row scan baseline. Because the contract is an upper bound, an estimate that is too high
   * is perf-conservative (it may pick the slower row-scan path when the dictionary scan would have won); an
   * estimate that is too low would be unsafe, so implementations must never under-report. A path that does not
   * appear in the index returns 0.
   */
  default long getDistinctValueCountForPath(String jsonPath) {
    return getMatchingDistinctValues(jsonPath, null).size();
  }
}
