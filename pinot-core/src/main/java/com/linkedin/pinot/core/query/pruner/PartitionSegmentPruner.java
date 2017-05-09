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
package com.linkedin.pinot.core.query.pruner;

import com.linkedin.pinot.common.query.ServerQueryRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.core.data.partition.PartitionFunction;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.math.IntRange;


/**
 * Implementation of {@link SegmentPruner} that uses partition information to perform pruning.
 */
public class PartitionSegmentPruner extends AbstractSegmentPruner {
  @Override
  public void init(Configuration config) {

  }

  @Override
  public boolean prune(IndexSegment segment, ServerQueryRequest queryRequest) {
    FilterQueryTree filterQueryTree = queryRequest.getFilterQueryTree();
    return prune(segment, filterQueryTree);
  }

  /**
   * Version of prune that directly takes filter query tree.
   *
   * @param segment Segment to prune
   * @param filterQueryTree Filter query tree
   * @return True if segment can be pruned, false otherwise.
   */
  public boolean prune(IndexSegment segment, FilterQueryTree filterQueryTree) {
    if (filterQueryTree == null) {
      return false;
    }

    // For realtime segment, this map can be null.
    Map<String, ColumnMetadata> columnMetadataMap =
        ((SegmentMetadataImpl) segment.getSegmentMetadata()).getColumnMetadataMap();

    return (columnMetadataMap != null) && pruneSegment(filterQueryTree, columnMetadataMap);
  }

  /**
   * Helper method that prunes the segment as follows:
   * <ul>
   *   <li> For non-leaf nodes, calls base pruneNonLeaf method in the super class. </li>
   *   <li> For leaf nodes, segment is pruned if equality predicate value on column does fall within the
   *        partition range of the segment. For all other cases, segment is not pruned. </li>
   * </ul>
   * @param filterQueryTree Filter query tree
   * @param columnMetadataMap Column metadata map
   * @return True if segment can be pruned, false otherwise
   */
  @Override
  public boolean pruneSegment(FilterQueryTree filterQueryTree, Map<String, ColumnMetadata> columnMetadataMap) {
    List<FilterQueryTree> children = filterQueryTree.getChildren();

    // Non-leaf node
    if (children != null && !children.isEmpty()) {
      return pruneNonLeaf(filterQueryTree, columnMetadataMap);
    }

    // TODO: Enhance partition based pruning for RANGE operator.
    if (filterQueryTree.getOperator() != FilterOperator.EQUALITY) {
      return false;
    }

    // Leaf node
    String column = filterQueryTree.getColumn();
    ColumnMetadata metadata = columnMetadataMap.get(column);
    if (metadata == null) {
      return false;
    }

    List<IntRange> partitionRanges = metadata.getPartitionRanges();
    if (partitionRanges == null || partitionRanges.isEmpty()) {
      return false;
    }

    Comparable value = getValue(filterQueryTree.getValue().get(0), metadata.getDataType());
    PartitionFunction partitionFunction = metadata.getPartitionFunction();
    int partition = partitionFunction.getPartition(value);

    for (IntRange partitionRange : partitionRanges) {
      if (partitionRange.containsInteger(partition)) {
        return false;
      }
    }
    return true;
  }
}
