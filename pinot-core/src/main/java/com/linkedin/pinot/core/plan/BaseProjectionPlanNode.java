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

import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BReusableFilteredDocIdSetOperator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class BaseProjectionPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseProjectionPlanNode.class);
  protected final IndexSegment indexSegment;
  protected final String[] columns;
  protected final BaseDocIdSetPlanNode docIdSetPlanNode;
  protected final Map<String, BaseColumnarDataSourcePlanNode> dataSourcePlanNodeMap;

  protected MProjectionOperator projectionOperator = null;

  public BaseProjectionPlanNode(IndexSegment indexSegment, String[] columns, BaseDocIdSetPlanNode docIdSetPlanNode) {
    this.indexSegment = indexSegment;
    this.columns = columns;
    this.docIdSetPlanNode = docIdSetPlanNode;
    this.dataSourcePlanNodeMap = new HashMap<>();
    for (String column : columns) {
      dataSourcePlanNodeMap.put(column, getColumnarDataSourcePlanNode(column));
    }
  }

  protected abstract BaseColumnarDataSourcePlanNode getColumnarDataSourcePlanNode(String column);

  @Override
  public Operator run() {
    long start = System.currentTimeMillis();
    if (projectionOperator == null) {
      Map<String, DataSource> dataSourceMap = new HashMap<String, DataSource>();
      BReusableFilteredDocIdSetOperator docIdSetOperator = (BReusableFilteredDocIdSetOperator) docIdSetPlanNode.run();
      long endTime1 = System.currentTimeMillis();
      for (String column : dataSourcePlanNodeMap.keySet()) {
        dataSourceMap.put(column, (DataSource) dataSourcePlanNodeMap.get(column).run());
      }
      projectionOperator = new MProjectionOperator(dataSourceMap, docIdSetOperator);
    }
    long end = System.currentTimeMillis();
    LOGGER.debug("Time take in ProjectionPlanNode: " + (end - start));
    return projectionOperator;
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Operator: MProjectionOperator");
    LOGGER.debug(prefix + "Argument 0: DocIdSet - ");
    docIdSetPlanNode.showTree(prefix + "    ");
    int i = 0;
    for (String column : dataSourcePlanNodeMap.keySet()) {
      LOGGER.debug(prefix + "Argument " + (i + 1) + ": DataSourceOperator");
      dataSourcePlanNodeMap.get(column).showTree(prefix + "    ");
      i++;
    }
  }
}
