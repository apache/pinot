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
package com.linkedin.pinot.core.common;

import com.linkedin.pinot.core.operator.ExecutionStatistics;


public interface Operator<T extends Block> {
  /*
   * allows the operator to set up/initialize processing
   */
  boolean open();

  /**
   * Get the next non empty block, if there are additional predicates the
   * operator is responsible to apply the predicate and return the block that
   * has atleast one doc that satisfies the predicate
   *
   * @return
   */
  T nextBlock();

  boolean close();

  ExecutionStatistics getExecutionStatistics();
}
