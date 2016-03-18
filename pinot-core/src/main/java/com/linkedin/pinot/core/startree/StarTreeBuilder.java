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
package com.linkedin.pinot.core.startree;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import com.google.common.collect.HashBiMap;
import com.linkedin.pinot.core.data.GenericRow;
import java.util.Set;


public interface StarTreeBuilder {
  /**
   * Initializes the builder, called before append.
   *
   * @param splitOrder
   *  The dimensions that should be used in successive splits down the tree.
   * @param maxLeafRecords
   *  The maximum number of records that can exist at a leaf, if there are still split dimensions available.
   * @param table
   *  The temporary table to store dimensional data.
   * @throws Exception
   */
  void init(StarTreeBuilderConfig config) throws Exception;

  /**
   * Adds a possibly non-unique dimension combination to the StarTree table.
   * @throws Exception
   */
  void append(GenericRow row) throws Exception;

  /**
   * Builds the StarTree, called after all calls to append (after build);
   * @throws Exception
   */
  void build() throws Exception;

  /**
   * Clean up any temporary files/directories, called at the end.
   */
  void cleanup();

  /**
   * Returns the root node of the tree (after build).
   */
  StarTree getTree();

  /**
   *
   * @return
   * @throws Exception
   */
  Iterator<GenericRow> iterator(int startDocId, int endDocId) throws Exception;

  /**
   * Returns the total number of non-aggregate dimension combinations.
   */
  int getTotalRawDocumentCount();

  /**
   * Returns the total number of aggregate dimension combinations.
   */
  int getTotalAggregateDocumentCount();

  /**
   * Returns the maximum number of leaf records from init.
   */
  int getMaxLeafRecords();

  /**
   * Returns the split order
   */
  List<String> getDimensionsSplitOrder();

  Map<String, HashBiMap<Object, Integer>> getDictionaryMap();

  HashBiMap<String, Integer> getDimensionNameToIndexMap();

  Set<String> getSkipMaterializationForDimensions();
}
