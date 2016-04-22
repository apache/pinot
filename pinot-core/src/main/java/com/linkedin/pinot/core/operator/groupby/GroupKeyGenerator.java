/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.groupby;

import com.linkedin.pinot.core.query.utils.Pair;
import java.util.Iterator;


/**
 * Interface for generating group-by keys.
 */
public interface GroupKeyGenerator {

  /**
   * Generate group-by key for a given docId.
   *
   * @param docId
   * @return
   */
  int generateKeyForDocId(int docId);

  /**
   * Generate group-by keys for a given docId set and return the mapping
   * in the passed in docIdToGroupKey array.
   *
   * @param docIdSet
   * @param startIndex
   * @param length
   * @param docIdToGroupKey
   * @return
   */
  void generateKeysForDocIdSet(int[] docIdSet, int startIndex, int length, int[] docIdToGroupKey);

  /**
   * Generate group-by keys for the given docId set, and return a map of docId to int[] of group keys.
   * This interface is for multi-valued columns, where a docId can have multiple group keys.
   *
   * @param docIdSet
   * @param startIndex
   * @param length
   * @return
   */
  void generateKeysForDocIdSet(int[] docIdSet, int startIndex, int length, int[][] docIdToGroupKeys);

  /**
   * Returns an array
   * @return
   */
  Iterator<Pair<Integer, String>> getUniqueGroupKeys();

  /**
   * Return the maximum number of unique group keys.
   * @return
   */
  int getNumGroupKeys();
}
