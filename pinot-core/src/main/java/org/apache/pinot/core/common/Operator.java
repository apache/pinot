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
package org.apache.pinot.core.common;

import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.query.exception.EarlyTerminationException;


public interface Operator<T extends Block> {

  /**
   * Get the next {@link Block}.
   * <p>For filter operator and operators above projection phase (aggregation, selection, combine etc.), method should
   * only be called once, and will return a non-null block.
   * <p>For operators in projection phase (docIdSet, projection, transformExpression), method can be called multiple
   * times, and will return non-empty block or null if no more documents available
   *
   * @throws EarlyTerminationException if the operator is early-terminated (interrupted) before processing the next
   *         block of data. Operator can early terminated when the query times out, or is already satisfied.
   */
  T nextBlock();

  ExecutionStatistics getExecutionStatistics();
}
