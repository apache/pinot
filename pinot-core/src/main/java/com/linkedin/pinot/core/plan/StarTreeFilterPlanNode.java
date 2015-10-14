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
import com.linkedin.pinot.core.startree.StarTreeIndexNode;

import java.util.*;

public class StarTreeFilterPlanNode extends BaseFilterPlanNode {
  private StarTreeIndexNode matchingNode;
  private StarTreeOperator starTreeOperator;

  public StarTreeFilterPlanNode(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    super(indexSegment, brokerRequest);
  }

  @Override
  protected Operator constructPhysicalOperator(FilterQueryTree filterQueryTree) {
    if (matchingNode == null) {
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

      // Find the leaf that corresponds to the query
      StarTreeIndexNode current = indexSegment.getStarTreeRoot();
      while (current != null && !current.isLeaf()) {
        String nextDimension = dimensionNames.get(current.getChildDimensionName());
        String nextValue = pathValues.get(nextDimension);
        DataSource dataSource = indexSegment.getDataSource(nextDimension);

        int nextValueId;
        if (nextValue == null) {
          nextValueId = StarTreeIndexNode.all();
        } else {
          nextValueId = dataSource.getDictionary().indexOf(nextValue);
        }

        current = current.getChildren().get(nextValueId);
      }

      if (current == null) {
        return new StarTreeOperator(Constants.EOF, Constants.EOF, null);
      }

      // We store to avoid traversing tree again, as the leaf node that matches does not change
      matchingNode = current;

      // Find the remaining path values
      for (Map.Entry<Integer, Integer> entry : matchingNode.getPathValues().entrySet()) {
        String dimensionName = dimensionNames.get(entry.getKey());
        pathValues.remove(dimensionName);
      }

      // Create ScanBasedFilterOperator with EqPredicate for each remaining path values
      List<Operator> scanOperators = new ArrayList<>();
      for (Map.Entry<String, String> entry : pathValues.entrySet()) {
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
      Integer startDocumentId = matchingNode.getStartDocumentId();
      Integer endDocumentId = startDocumentId + matchingNode.getDocumentCount() - 1 /* inclusive end */;
      starTreeOperator = new StarTreeOperator(startDocumentId, endDocumentId, andOperator);
    }

    return starTreeOperator;
  }
}
