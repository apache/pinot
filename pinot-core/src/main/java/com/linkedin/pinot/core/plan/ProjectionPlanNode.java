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
package com.linkedin.pinot.core.plan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BReusableFilteredDocIdSetOperator;
import com.linkedin.pinot.core.operator.MProjectionOperator;


/**
 * ProjectionPlanNode takes care of a map from column name to its corresponding
 * data source.
 */
public class ProjectionPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProjectionPlanNode.class);

  private final Map<String, ColumnarDataSourcePlanNode> _dataSourcePlanNodeMap =
      new HashMap<String, ColumnarDataSourcePlanNode>();
  private final DocIdSetPlanNode _docIdSetPlanNode;
  private MProjectionOperator _projectionOperator = null;

  public ProjectionPlanNode(IndexSegment indexSegment, String[] columns, DocIdSetPlanNode docIdSetPlanNode) {
    _docIdSetPlanNode = docIdSetPlanNode;
    for (String column : columns) {
      _dataSourcePlanNodeMap.put(column, new ColumnarDataSourcePlanNode(indexSegment, column, docIdSetPlanNode));
    }
  }

  public ProjectionPlanNode(IndexSegment indexSegment, DocIdSetPlanNode docIdSetPlanNode) {
    _docIdSetPlanNode = docIdSetPlanNode;
    for (String column : indexSegment.getColumnNames()) {
      _dataSourcePlanNodeMap.put(column, new ColumnarDataSourcePlanNode(indexSegment, column, docIdSetPlanNode));
    }

  }

  @Override
  public Operator run() {
    long start = System.currentTimeMillis();

    if (_projectionOperator == null) {
      Map<String, DataSource> dataSourceMap = new HashMap<String, DataSource>();
      BReusableFilteredDocIdSetOperator docIdSetOperator = (BReusableFilteredDocIdSetOperator) _docIdSetPlanNode.run();
      long endTime1 = System.currentTimeMillis();
      for (String column : _dataSourcePlanNodeMap.keySet()) {
        dataSourceMap.put(column, (DataSource) _dataSourcePlanNodeMap.get(column).run());
      }
      _projectionOperator = new MProjectionOperator(dataSourceMap, docIdSetOperator);
    }
    long end = System.currentTimeMillis();
    LOGGER.debug("Time take in ProjectionPlanNode: " + (end - start));
    return _projectionOperator;
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Operator: MProjectionOperator");
    LOGGER.debug(prefix + "Argument 0: DocIdSet - ");
    _docIdSetPlanNode.showTree(prefix + "    ");
    int i = 0;
    for (String column : _dataSourcePlanNodeMap.keySet()) {
      LOGGER.debug(prefix + "Argument " + (i + 1) + ": DataSourceOperator");
      _dataSourcePlanNodeMap.get(column).showTree(prefix + "    ");
      i++;
    }
  }

  public ColumnarDataSourcePlanNode getDataSourcePlanNode(String column) {
    return _dataSourcePlanNodeMap.get(column);
  }
}
