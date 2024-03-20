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
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Reader for json index.
 */
public interface JsonIndexReader extends IndexReader {

  /**
   * Returns the matching document ids for the given filter.
   */
  MutableRoaringBitmap getMatchingDocIds(String filterString);

  /**
   * For an array of docIds and context specific to a JSON key, returns the corresponding values for each docId. The
   * context should be created from the getMatchingDocsMap method.
   *
   * @return String[] where String[i] is the value for docIds[i]
   */
  String[] getValuesForKeyAndDocs(int[] docIds, Map<String, RoaringBitmap> context);

  /**
   * For a JSON key, returns a Map from each value to the flattened docId posting list.
   * @param jsonPathKey the JSONPath key to extract
   * @param filterJsonString The filter to apply on the flattened docs
   * @return Map from each unique value for the jsonPathKey value to the flattened docId posting list
   */
  Map<String, RoaringBitmap> getValueToFlattenedDocIdsMap(String jsonPathKey,
      @Nullable String filterJsonString);

  /**
   * For an array of docIds and context specific to a JSON key, returns the corresponding mv array for each docId.
   * @param docIds array of docIds
   * @param length length of the array
   * @param matchingValueToFlattenedDocs Map from each unique value for the jsonPathKey value to the flattened docId
   *                                     posting list
   * @return String[][] where String[i] is the mv array for docIds[i]
   */
  String[][] getValuesForMv(int[] docIds, int length, Map<String, RoaringBitmap> matchingValueToFlattenedDocs);

  /**
   * For an array of docIds and context specific to a JSON key, returns the corresponding sv value for each docId.
   * @param docIds array of docIds
   * @param length length of the array
   * @param matchingValueToFlattenedDocs Map from each unique value for the jsonPathKey value to the flattened docId
   *                                     posting list
   * @return String[] where String[i] is the sv value for docIds[i]
   */
  String[] getValuesForSv(int[] docIds, int length, Map<String, RoaringBitmap> matchingValueToFlattenedDocs);

  /**
   * For a JSON key, returns a Map from each value to the docId posting list. This map should be  used to avoid reading
   * and converting the posting list of flattened docIds to real docIds
   */
  Map<String, RoaringBitmap> getMatchingDocsMap(String key);
}
