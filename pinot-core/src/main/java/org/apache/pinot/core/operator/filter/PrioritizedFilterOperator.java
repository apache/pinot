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
package org.apache.pinot.core.operator.filter;

import java.util.OptionalInt;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.Operator;


/**
 * Operators that implements this interface can define how to be sorted in an AND, as defined in
 * {@link FilterOperatorUtils.Implementation#}.
 */
public interface PrioritizedFilterOperator<T extends Block> extends Operator<T> {

  int HIGH_PRIORITY = 0;
  int MEDIUM_PRIORITY = 100;
  int LOW_PRIORITY = 200;
  int AND_PRIORITY = 300;
  int OR_PRIORITY = 400;
  int SCAN_PRIORITY = 500;
  int EXPRESSION_PRIORITY = 1000;

  /**
   * Priority is a number that is used to compare different filters. Some predicates, like AND, sort their sub
   * predicates in order to first apply the ones that should be more efficient.
   *
   * For example, {@link SortedIndexBasedFilterOperator} is assigned to {@link #HIGH_PRIORITY},
   * {@link BitmapBasedFilterOperator} is assigned to {@link #MEDIUM_PRIORITY} and {@link H3IndexFilterOperator} to
   * {@link #LOW_PRIORITY}
   *
   * @return the priority of this filter operator or an empty optional if no special priority should be used.
   */
  OptionalInt getPriority();
}
