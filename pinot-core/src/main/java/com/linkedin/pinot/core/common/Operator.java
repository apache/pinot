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
package com.linkedin.pinot.core.common;

import com.linkedin.pinot.core.operator.ExecutionStatistics;


public interface Operator<T extends Block> {

  /**
   * Get the next {@link Block}.
   * <p>For filter operator and operators above projection phase (aggregation, selection, combine etc.), method should
   * only be called once, and will return a non-null block.
   * <p>For operators in projection phase (docIdSet, projection, transformExpression), method can be called multiple
   * times, and will return non-empty block or null if no more documents available</p>
   */
  T nextBlock();

  ExecutionStatistics getExecutionStatistics();
}
