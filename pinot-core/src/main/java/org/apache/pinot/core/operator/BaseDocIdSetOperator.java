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
package org.apache.pinot.core.operator;

import javax.annotation.Nullable;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;


/// The base class for operators that produce [DocIdSetBlock].
///
/// These operators are the intermediate between
/// [filter operators][org.apache.pinot.core.operator.filter.BaseFilterOperator] and
/// [projection operators][org.apache.pinot.core.operator.ProjectionOperator].
///
/// Filter operators return a [org.apache.pinot.core.operator.blocks.FilterBlock], whose method
/// [getBlockDocIdSet()][org.apache.pinot.core.operator.blocks.FilterBlock#getBlockDocIdSet()] is used
/// to build different types of [BaseDocIdSetOperator]s (e.g. [DocIdSetOperator]). Contrary to filter operators,
/// whose [nextBlock()][org.apache.pinot.core.operator.filter.BaseFilterOperator#nextBlock()] method returns always
/// the same block (which contains all the matched document ids for the segment),
/// **DocIdSetOperator[.nextBlock()][BaseDocIdSetOperator#nextBlock()] split the segment in multiple blocks and
/// therefore must be called multiple times until it returns `null`**.
///
/// The blocks returned by [BaseDocIdSetOperator] can and usually are returned in a given order. Most of the time, the
/// order is ascending, which means that the blocks and the rows inside them are sorted by their document ids in
/// ascending order, but may be changed depending on the query. For example the optimizer may decide to configure
/// operators to emit rows in descending order to optimize the performance of top-N queries on segments sorted in
/// ascending order.
public abstract class BaseDocIdSetOperator extends BaseOperator<DocIdSetBlock>
    implements DidOrderedOperator<DocIdSetBlock> {

  /// Returns a [BaseDocIdSetOperator] that is compatible with the requested order or fails with
  /// [IllegalArgumentException] if the order is not supported for this operator.
  ///
  /// It may return `this` if the order is already the requested one.
  ///
  /// @return a [BaseDocIdSetOperator] that is ascending if `ascending` is true or descending otherwise. Remember that
  ///         an operator may be both ascending and descending if it is empty.
  /// @throws UnsupportedOperationException if the order is not supported for this operator.
  public abstract BaseDocIdSetOperator withOrder(DidOrder order) throws UnsupportedOperationException;

  /// Returns the next block of document ids that match the filter (if any).
  ///
  /// Each time this method is called, it returns the next block of document ids, or `null` if there is no more
  /// blocks.
  @Override
  @Nullable
  protected abstract DocIdSetBlock getNextBlock();
}
