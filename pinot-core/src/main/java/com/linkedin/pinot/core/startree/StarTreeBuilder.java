/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.google.common.collect.HashBiMap;
import com.linkedin.pinot.core.data.GenericRow;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


public interface StarTreeBuilder {
  /**
   * Initialize the builder, called before append.
   *
   * @param config star tree builder config.
   * @throws Exception
   */
  void init(StarTreeBuilderConfig config) throws Exception;

  /**
   * Append a new record row to the star tree builder.
   *
   * @param row record row to be appended.
   * @throws Exception
   */
  void append(GenericRow row) throws Exception;

  /**
   * Build the star tree, called after all record rows appended to the builder.
   *
   * @throws Exception
   */
  void build() throws Exception;

  /**
   * Clean up any temporary files/directories, called at the end.
   */
  void cleanup();

  /**
   * Get the generated star tree.
   *
   * @return generated star tree.
   */
  StarTree getTree();

  /**
   * Get an iterator for all non-aggregate records and aggregate records.
   *
   * @param startDocId start document id.
   * @param endDocId end document id.
   * @return records iterator.
   * @throws Exception
   */
  Iterator<GenericRow> iterator(int startDocId, int endDocId) throws Exception;

  /**
   * Get the total number of non-aggregate records.
   *
   * @return number of non-aggregate records.
   */
  int getTotalRawDocumentCount();

  /**
   * Get the total number of aggregate records.
   *
   * @return number of aggregate records.
   */
  int getTotalAggregateDocumentCount();

  /**
   * Get the maximum number of records in leaf node.
   *
   * @return maximum number of records in leaf node.
   */
  int getMaxLeafRecords();

  /**
   * Get the dimensions split order.
   *
   * @return dimensions split order.
   */
  List<String> getDimensionsSplitOrder();

  /**
   * Get the skip materialization dimensions set associated with the star tree.
   *
   * @return skip materialization dimensions set.
   */
  Set<String> getSkipMaterializationForDimensions();

  /**
   * Get the column to dictionary map associated with the star tree.
   *
   * @return column to dictionary map.
   */
  Map<String, HashBiMap<Object, Integer>> getDictionaryMap();
}
