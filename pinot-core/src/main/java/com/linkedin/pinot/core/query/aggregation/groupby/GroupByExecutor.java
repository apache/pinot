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
package com.linkedin.pinot.core.query.aggregation.groupby;

import com.linkedin.pinot.core.operator.blocks.TransformBlock;


/**
 * Interface class for executing the actual group-by operation.
 */
public interface GroupByExecutor {

  /**
   * Initializations that need to be performed before process can be called.
   */
  void init();

  /**
   * Performs the actual group-by aggregation on the given transform block.
   *
   * @param transformBlock Block to process
   */
  void process(TransformBlock transformBlock);

  /**
   * Post processing (if any) to be done after all docIdSets have been processed, and
   * before getResult can be called.
   */
  void finish();

  /**
   * Returns the result of group-by aggregation, ensures that 'finish' has been
   * called before calling getResult().
   *
   * @return Result of aggregation group-by.
   */
  AggregationGroupByResult getResult();
}
