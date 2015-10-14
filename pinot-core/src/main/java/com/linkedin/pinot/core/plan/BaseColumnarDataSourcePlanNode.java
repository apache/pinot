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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseColumnarDataSourcePlanNode implements PlanNode {
  protected static final Logger LOGGER = LoggerFactory.getLogger("QueryPlanLog");
  protected final IndexSegment indexSegment;
  protected final String columnName;
  protected final DataSource dataSource;

  public BaseColumnarDataSourcePlanNode(IndexSegment indexSegment, String columnName) {
    this.indexSegment = indexSegment;
    this.columnName = columnName;
    this.dataSource = getDataSource();
  }

  protected abstract DataSource getDataSource();

  @Override
  public Operator run() {
    return dataSource;
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Columnar Reader Data Source:");
    LOGGER.debug(prefix + "Operator: ColumnarReaderDataSource");
    LOGGER.debug(prefix + "Argument 0: IndexSegment - " + indexSegment.getSegmentName());
    LOGGER.debug(prefix + "Argument 1: Column Name - " + columnName);
  }
}
