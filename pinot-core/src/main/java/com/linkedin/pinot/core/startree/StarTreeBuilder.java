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

import java.util.List;
import java.util.Map;
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
   */
  void init(List<Integer> splitOrder, int maxLeafRecords, StarTreeTable table);

  /**
   * Adds a possibly non-unique dimension combination to the StarTree table.
   */
  void append(StarTreeTableRow row);

  /**
   * Builds the StarTree, called after all calls to append (after build);
   */
  void build();

  /**
   * Returns the root node of the tree (after build).
   */
  StarTreeIndexNode getTree();

  /**
   * Returns the leaf record table (after build).
   */
  StarTreeTable getTable();

  /**
   * Returns the record ID range in the StarTree table for a node (after build).
   */
  StarTreeTableRange getDocumentIdRange(int nodeId);

  /**
   * Returns the adjusted document ID range.
   *
   * <p>
   *   If a node contains aggregates in its path, then the return value signifies the range of documents
   *   within a contiguous table of only aggregates.
   * </p>
   *
   * <p>
   *   If a node does not contain aggregates in its path, then the return value signifies the range of
   *   documents within a contiguous table of only raw values.
   * </p>
   */
  StarTreeTableRange getAggregateAdjustedDocumentIdRange(int nodeId);

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
   * Returns the split order from init.
   */
  List<Integer> getSplitOrder();
}
