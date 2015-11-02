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
package com.linkedin.pinot.core.plan;

import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.*;
import com.linkedin.pinot.core.common.predicate.*;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.filter.*;
import com.linkedin.pinot.core.segment.index.readers.*;
import com.linkedin.pinot.core.startree.StarTreeIndexNode;

import java.util.*;

public class StarTreeFilterPlanNode extends BaseFilterPlanNode {
  private List<StarTreeIndexNode> matchingNodes;
  private List<StarTreeOperator> starTreeOperators;

  public StarTreeFilterPlanNode(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    super(indexSegment, brokerRequest);
  }

  @Override
  protected Operator constructPhysicalOperator(FilterQueryTree filterQueryTree) {
    if (matchingNodes == null) {
      matchingNodes = new ArrayList<>();
      starTreeOperators = new ArrayList<>();
      List<String> dimensionNames = indexSegment.getSegmentMetadata().getSchema().getDimensionNames();

      // Get conjunction values
      List<FilterQueryTree> conjunctions = null;
      if (filterQueryTree != null) {
        if (filterQueryTree.getChildren() == null) {
          conjunctions = ImmutableList.of(filterQueryTree);
        } else {
          conjunctions = filterQueryTree.getChildren();
        }
      }

      // Convert the query to tree path values
      Map<String, String> pathValues = new HashMap<>();
      if (conjunctions != null) {
        for (FilterQueryTree clause : conjunctions) {
          String column = clause.getColumn();
          String value = clause.getValue().get(0);
          pathValues.put(column, value);
        }
      }

      Queue<StarTreeIndexNode> searchQueue = new LinkedList<>();
      searchQueue.add(indexSegment.getStarTreeRoot());

      while (!searchQueue.isEmpty()) {
        StarTreeIndexNode current = searchQueue.remove();

        // Find the leaf that corresponds to the query
        while (current != null && !current.isLeaf()) {
          String nextDimension = dimensionNames.get(current.getChildDimensionName());
          String nextValue = pathValues.get(nextDimension);
          DataSource nextDataSource = indexSegment.getDataSource(nextDimension);

          int nextValueId;
          if (nextValue == null) {
            nextValueId = StarTreeIndexNode.all();
          } else {
            nextValueId = nextDataSource.getDictionary().indexOf(nextValue);
          }

          // If this is a group by, we split the query down several paths, but not star node
          if (brokerRequest.isSetGroupBy() && brokerRequest.getGroupBy().getColumns().contains(nextDimension)) {
            for (Map.Entry<Integer, StarTreeIndexNode> entry : current.getChildren().entrySet()) {
              if (entry.getKey() != StarTreeIndexNode.all()) {
                searchQueue.add(entry.getValue());
              }
            }
            current = null; // triggers exit out of both loops
          } else {
            // Otherwise, we continue down this path
            current = current.getChildren().get(nextValueId);
          }
        }

        if (current == null) {
          continue;
        }

        // We store to avoid traversing tree again, as the leaf node that matches does not change
        matchingNodes.add(current);

        // Find the remaining path values
        Map<String, String> pathValuesCopy = new HashMap<>(pathValues);
        for (Map.Entry<Integer, Integer> entry : current.getPathValues().entrySet()) {
          String dimensionName = dimensionNames.get(entry.getKey());
          pathValuesCopy.remove(dimensionName);
        }

        // Create ScanBasedFilterOperator with EqPredicate for each remaining path values
        List<Operator> scanOperators = new ArrayList<>();
        for (Map.Entry<String, String> entry : pathValuesCopy.entrySet()) {
          Predicate predicate = new EqPredicate(entry.getKey(), Collections.singletonList(entry.getValue()));
          ScanBasedFilterOperator operator = new ScanBasedFilterOperator(indexSegment.getDataSource(entry.getKey()));
          operator.setPredicate(predicate);
          scanOperators.add(operator);
        }

        // Join those scan operators as AND
        AndOperator andOperator = null;
        if (!scanOperators.isEmpty()) {
          andOperator = new AndOperator(scanOperators);
        }

        // Scan that sub-segment
        Integer startDocumentId = current.getStartDocumentId();
        Integer endDocumentId = startDocumentId + current.getDocumentCount() - 1 /* inclusive end */;
        StarTreeOperator starTreeOperator = new StarTreeOperator(startDocumentId, endDocumentId, andOperator);
        starTreeOperators.add(starTreeOperator);
      }
    }

    if (starTreeOperators.isEmpty()) {
      return new StarTreeOperator(Constants.EOF, Constants.EOF, null);
    }

    return new CompositeStarTreeOperator(starTreeOperators);
  }
}
