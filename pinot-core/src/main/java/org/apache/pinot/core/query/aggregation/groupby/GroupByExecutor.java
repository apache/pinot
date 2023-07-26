/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.aggregation.groupby;

import java.util.Collection;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.TableResizer;
import org.apache.pinot.core.operator.blocks.ValueBlock;


/**
 * Interface class for executing the actual group-by operation.
 */
public interface GroupByExecutor {

  /**
   * Performs the group-by aggregation on the given value block.
   */
  void process(ValueBlock valueBlock);

  /**
   * Returns the result of group-by aggregation.
   * <p>Should be called after all transform blocks has been processed.
   *
   * @return Result of aggregation
   */
  AggregationGroupByResult getResult();

  /**
   * Returns the number of generated results
   *
   * @return Number of results
   */
  int getNumGroups();

  /**
   * Trim the GroupBy result up to the threshold max(configurable_threshold * 5, minTrimSize)
   * TODO: benchmark the performance of PQ vs. topK
   * <p>Should be called after all transform blocks has been processed.
   *
   */
  Collection<IntermediateRecord> trimGroupByResult(int trimSize, TableResizer tableResizer);

  GroupKeyGenerator getGroupKeyGenerator();

  GroupByResultHolder[] getGroupByResultHolders();
}
