/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.startree.v2;

import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.startree.StarTree;


/**
 * The {@code StarTreeV2} class is a wrapper on top of star-tree, its metadata, and the data sources associated
 * with it.
 */
public interface StarTreeV2 {

  /**
   * Returns the {@link StarTree} data structure.
   */
  StarTree getStarTree();

  /**
   * Returns the metadata of the star-tree.
   */
  StarTreeV2Metadata getMetadata();

  /**
   * Returns the data source for the given column name, where the column name could be dimension name or from
   * {@link AggregationFunctionColumnPair#toColumnName()}.
   */
  DataSource getDataSource(String columnName);
}
