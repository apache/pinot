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

import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataSourcePlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataSourcePlanNode.class);

  private final IndexSegment _indexSegment;
  private final String _columnName;

  public DataSourcePlanNode(IndexSegment indexSegment, String columnName) {
    _indexSegment = indexSegment;
    _columnName = columnName;
  }

  @Override
  public DataSource run() {
    return _indexSegment.getDataSource(_columnName);
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Columnar Reader Data Source:");
    LOGGER.debug(prefix + "Operator: ColumnarReaderDataSource");
    LOGGER.debug(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    LOGGER.debug(prefix + "Argument 1: Column Name - " + _columnName);
  }
}
