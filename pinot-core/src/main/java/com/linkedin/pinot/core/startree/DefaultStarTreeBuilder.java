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

import com.google.common.base.Objects;

import java.util.*;

public class DefaultStarTreeBuilder implements StarTreeBuilder {
  private List<Integer> splitOrder;
  private int maxLeafRecords;
  private StarTreeTable starTreeTable;
  private StarTreeIndexNode starTree;
  private Map<Integer, StarTreeTableRange> documentIdRanges;
  private Map<Integer, StarTreeTableRange> adjustedDocumentIdRanges;
  private boolean buildComplete;
  private int totalRawDocumentCount;
  private int totalAggDocumentCount;
  private StarTreeDocumentIdMap documentIdMap;

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("splitOrder", splitOrder)
        .add("maxLeafRecords", maxLeafRecords)
        .add("starTreeTable", starTreeTable)
        .toString();
  }

  @Override
  public void init(List<Integer> splitOrder,
                   int maxLeafRecords,
                   StarTreeTable starTreeTable,
                   StarTreeDocumentIdMap documentIdMap) {
    this.splitOrder = splitOrder;
    this.maxLeafRecords = maxLeafRecords;
    this.starTreeTable = starTreeTable;
    this.starTree = new StarTreeIndexNode();
    this.documentIdRanges = new HashMap<Integer, StarTreeTableRange>();
    this.adjustedDocumentIdRanges = new HashMap<Integer, StarTreeTableRange>();
    this.documentIdMap = documentIdMap;

    // Root node is everything (i.e. raw segment)
    this.starTree.setDimensionName(StarTreeIndexNode.all());
    this.starTree.setDimensionValue(StarTreeIndexNode.all());
    this.starTree.setLevel(0);
  }

  @Override
  public int getMaxLeafRecords() {
    return maxLeafRecords;
  }

  @Override
  public List<Integer> getSplitOrder() {
    return splitOrder;
  }

  @Override
  public void append(StarTreeTableRow row) {
    starTreeTable.append(row);
  }

  @Override
  public void build() {
    constructStarTree(starTree, starTreeTable);
    numberNodes();
    computeDocumentIdRanges();
    buildComplete = true;
  }

  @Override
  public StarTreeIndexNode getTree() {
    if (!buildComplete) {
      throw new IllegalStateException("Must call build first");
    }
    return starTree;
  }

  @Override
  public StarTreeTable getTable() {
    if (!buildComplete) {
      throw new IllegalStateException("Must call build first");
    }
    return starTreeTable;
  }

  @Override
  public StarTreeTableRange getDocumentIdRange(int nodeId) {
    if (!buildComplete) {
      throw new IllegalStateException("Must call build first");
    }
    return documentIdRanges.get(nodeId);
  }

  @Override
  public StarTreeTableRange getAggregateAdjustedDocumentIdRange(int nodeId) {
    if (!buildComplete) {
      throw new IllegalStateException("Must call build first");
    }
    return adjustedDocumentIdRanges.get(nodeId);
  }

  @Override
  public int getTotalRawDocumentCount() {
    if (!buildComplete) {
      throw new IllegalStateException("Must call build first");
    }
    return totalRawDocumentCount;
  }

  @Override
  public int getTotalAggregateDocumentCount() {
    if (!buildComplete) {
      throw new IllegalStateException("Must call build first");
    }
    return totalAggDocumentCount;
  }

  @Override
  public Integer getNextDocumentId(List<Integer> dimensions) {
    if (!buildComplete) {
      throw new IllegalStateException("Must call build first");
    }
    return documentIdMap.getNextDocumentId(dimensions);
  }

  /**
   * Recursively constructs the StarTree, splitting nodes and adding leaf records.
   *
   * @param node
   *  The sub-tree to potentially split.
   * @param table
   *  The projection of the StarTree table which corresponds to the sub tree.
   * @return
   *  The number of records that were added at this level
   */
  private int constructStarTree(StarTreeIndexNode node, StarTreeTable table) {
    if (node.getLevel() >= splitOrder.size() || table.size() <= maxLeafRecords) {
      // Either can no longer split, or max record constraint has been met
      return 0;
    }

    // The next dimension on which to split
    Integer splitDimensionId = splitOrder.get(node.getLevel());

    // Compute the remaining unique combinations after removing split dimension
    int aggregateCombinations = 0;
    Iterator<StarTreeTableRow> uniqueItr = table.getUniqueCombinations(Collections.singletonList(splitDimensionId));
    while (uniqueItr.hasNext()) {
      // And append them to the sub table
      // n.b. This appends to the end of the sub table, which may be actually inserting to a parent table
      StarTreeTableRow row = uniqueItr.next();
      table.append(row);
      aggregateCombinations++;
    }

    // Sort the sub-table based on the current tree prefix
    // n.b. If a view of a larger table, it will be partially sorted,
    // so the first couple of dimension prefix comparisons will be no-ops.
    List<Integer> pathDimensions = node.getPathDimensions();
    pathDimensions.add(splitDimensionId);
    table.sort(pathDimensions);

    // Make this node a parent
    node.setChildDimensionName(splitDimensionId);
    node.setChildren(new HashMap<Integer, StarTreeIndexNode>());

    // Compute the GROUP BY stats, including for ALL (i.e. "*") dimension value
    StarTreeTableGroupByStats groupByStats = table.groupBy(splitDimensionId);

    int subTreeAggregateCombinations = 0;
    for (Integer valueId : groupByStats.getValues()) {
      // Create child
      StarTreeIndexNode child = new StarTreeIndexNode();
      child.setDimensionName(splitDimensionId);
      child.setDimensionValue(valueId);
      child.setParent(node);
      child.setLevel(node.getLevel() + 1);
      // n.b. We will number the nodes later using BFS after fully split

      // Add child to parent
      node.getChildren().put(valueId, child);

      // Create table projection
      // n.b. Since the sub table is sorted, we can use minRecordId and raw count to determine the range for
      // containing the dimensions whose value is valueId for splitDimensionId, and create a sub table.
      Integer minRecordId = groupByStats.getMinRecordId(valueId) + subTreeAggregateCombinations;
      Integer rawRecordCount = groupByStats.getRawCount(valueId);
      StarTreeTable subTable = table.view(minRecordId, rawRecordCount);

      // Create sub-tree
      subTreeAggregateCombinations += constructStarTree(child, subTable);
    }

    return aggregateCombinations + subTreeAggregateCombinations;
  }

  /**
   * Numbers the StarTree nodes using BFS of the tree.
   */
  private void numberNodes() {
    int nodeId = 0;
    Queue<StarTreeIndexNode> queue = new LinkedList<StarTreeIndexNode>();
    queue.add(starTree);
    while (!queue.isEmpty()) {
      StarTreeIndexNode current = queue.remove();
      current.setNodeId(nodeId++);
      if (!current.isLeaf()) {
        for (StarTreeIndexNode child : current.getChildren().values()) {
          queue.add(child);
        }
      }
    }
  }

  /**
   * Computes the ranges to which each nodeId maps in the StarTree table.
   *
   * <p>
   *   This assumes that the table is sorted according to tree path dimension prefix.
   * </p>
   */
  private void computeDocumentIdRanges() {
    Map<Integer, Integer> currentPrefix = null;
    StarTreeIndexNode currentNode = null;
    int currentDocumentId = 0;
    int matchingPrefixCount = 0;
    int matchingStartDocumentId = 0;
    int matchingAdjustedStartDocumentId = 0;
    int currentRawDocumentId = 0;
    int currentAggDocumentId = 0;

    Iterator<StarTreeTableRow> tableItr = starTreeTable.getAllCombinations();

    while (tableItr.hasNext()) {
      StarTreeTableRow currentCombination = tableItr.next();

      // Initialize current node
      if (currentNode == null) {
        currentNode = starTree.getMatchingNode(currentCombination.getDimensions());
        currentPrefix = currentNode.getPathValues();
        if (currentPrefix.containsValue(StarTreeIndexNode.all())) {
          matchingAdjustedStartDocumentId = currentAggDocumentId;
        } else {
          matchingAdjustedStartDocumentId = currentRawDocumentId;
        }
      }

      if (StarTreeIndexNode.matchesPrefix(currentPrefix, currentCombination.getDimensions())) {
        // As long as current document matches the prefix, keep incrementing count
        matchingPrefixCount++;
      } else {
        // We are at the next node's range, so store the current range and reset state to
        // be consistent with the new node's range
        StarTreeTableRange range = new StarTreeTableRange(matchingStartDocumentId, matchingPrefixCount);
        documentIdRanges.put(currentNode.getNodeId(), range);
        StarTreeTableRange adjustedRange = new StarTreeTableRange(matchingAdjustedStartDocumentId, matchingPrefixCount);
        adjustedDocumentIdRanges.put(currentNode.getNodeId(), adjustedRange);

        // Reset the node
        currentNode = starTree.getMatchingNode(currentCombination.getDimensions());
        if (currentNode == null) {
          throw new IllegalStateException("No node matches combination " + currentCombination);
        }
        currentPrefix = currentNode.getPathValues();

        // Reset the matching document
        matchingPrefixCount = 1;
        matchingStartDocumentId = currentDocumentId;
        if (currentPrefix.containsValue(StarTreeIndexNode.all())) {
          matchingAdjustedStartDocumentId = currentAggDocumentId;
        } else {
          matchingAdjustedStartDocumentId = currentRawDocumentId;
        }
      }

      // Record the document ID
      if (currentPrefix.values().contains(StarTreeIndexNode.all())) {
        documentIdMap.recordDocumentId(currentCombination.getDimensions(), currentAggDocumentId);
      } else {
        documentIdMap.recordDocumentId(currentCombination.getDimensions(), currentRawDocumentId);
      }

      // Move on to next document, also within agg / raw
      currentDocumentId++;
      if (currentPrefix.containsValue(StarTreeIndexNode.all())) {
        currentAggDocumentId++;
      } else {
        currentRawDocumentId++;
      }
    }

    // The left overs
    if (currentNode != null) {
      StarTreeTableRange range = new StarTreeTableRange(matchingStartDocumentId, matchingPrefixCount);
      documentIdRanges.put(currentNode.getNodeId(), range);
      StarTreeTableRange adjustedRange = new StarTreeTableRange(matchingAdjustedStartDocumentId, matchingPrefixCount);
      adjustedDocumentIdRanges.put(currentNode.getNodeId(), adjustedRange);
    }

    totalAggDocumentCount = currentAggDocumentId;
    totalRawDocumentCount = currentRawDocumentId;
  }
}
