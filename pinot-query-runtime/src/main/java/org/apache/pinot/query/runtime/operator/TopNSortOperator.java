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
package org.apache.pinot.query.runtime.operator;

import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// A [SortOperator] that keeps only the `fetch + offset` smallest rows, in a bounded max-heap.
///
/// Chosen when the result is bounded, either by the query's own `fetch` or by the broker response limit. Peak memory
/// is that bound rather than the size of the input, which is what makes this the preferred implementation whenever it
/// is applicable.
///
/// The heap is a *max*-heap on the collation (the comparator is inverted), so its head is the largest row retained so
/// far and is the one evicted when a smaller row arrives.
public class TopNSortOperator extends SortOperator {
  private static final String EXPLAIN_NAME = "SORT_TOP_N";
  private static final Logger LOGGER = LoggerFactory.getLogger(TopNSortOperator.class);

  private final PriorityQueue<Object[]> _priorityQueue;

  TopNSortOperator(OpChainExecutionContext context, MultiStageOperator input, DataSchema dataSchema, int offset,
      int numRowsToKeep, int maxRowsPerBlock, List<RelFieldCollation> collations, int defaultHolderCapacity) {
    super(context, input, dataSchema, offset, numRowsToKeep, maxRowsPerBlock, true);
    // Use the opposite direction as specified by the collation directions since we need the PriorityQueue to decide
    // which elements to keep and which to remove based on the limits.
    _priorityQueue = new PriorityQueue<>(Math.min(defaultHolderCapacity, numRowsToKeep),
        new SortUtils.SortComparator(collations, true));
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected MseBlock produceNextBlock() {
    MseBlock block = _input.nextBlock();
    while (block.isData()) {
      for (Object[] row : ((MseBlock.Data) block).asRowHeap().getRows()) {
        SelectionOperatorUtils.addToPriorityQueue(row, _priorityQueue, _numRowsToKeep);
      }
      checkTerminationAndSampleUsage();
      block = _input.nextBlock();
    }
    _eosBlock = (MseBlock.Eos) block;
    if (_eosBlock.isError()) {
      return _eosBlock;
    }
    int resultSize = _priorityQueue.size() - _offset;
    if (resultSize <= 0) {
      return _eosBlock;
    }
    // The heap yields the largest row first, so fill the result backwards. The rows left in the heap afterwards are
    // the `offset` smallest ones, which are exactly the ones OFFSET has to drop.
    Object[][] rows = new Object[resultSize][];
    for (int i = resultSize - 1; i >= 0; i--) {
      rows[i] = _priorityQueue.poll();
    }
    return emit(Arrays.asList(rows));
  }
}
