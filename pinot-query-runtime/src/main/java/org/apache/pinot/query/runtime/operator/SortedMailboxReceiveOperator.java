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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


/**
 * This {@code SortedMailboxReceiveOperator} receives data from a {@link ReceivingMailbox} and serve it out from the
 * {@link MultiStageOperator#getNextBlock()}()} API in a sorted manner.
 *
 *  TODO: Once sorting on the {@code MailboxSendOperator} is available, modify this to use a k-way merge instead of
 *        resorting via the PriorityQueue.
 */
public class SortedMailboxReceiveOperator extends BaseMailboxReceiveOperator {
  private static final String EXPLAIN_NAME = "SORTED_MAILBOX_RECEIVE";

  private final DataSchema _dataSchema;
  private final List<RexExpression> _collationKeys;
  private final List<Direction> _collationDirections;
  private final List<NullDirection> _collationNullDirections;
  private final boolean _isSortOnSender;
  private final List<Object[]> _rows = new ArrayList<>();

  private TransferableBlock _eosBlock;

  public SortedMailboxReceiveOperator(OpChainExecutionContext context, RelDistribution.Type exchangeType,
      DataSchema dataSchema, List<RexExpression> collationKeys, List<Direction> collationDirections,
      List<NullDirection> collationNullDirections, boolean isSortOnSender, int senderStageId) {
    super(context, exchangeType, senderStageId);
    Preconditions.checkState(!CollectionUtils.isEmpty(collationKeys), "Collation keys must be set");
    _dataSchema = dataSchema;
    _collationKeys = collationKeys;
    _collationDirections = collationDirections;
    _collationNullDirections = collationNullDirections;
    _isSortOnSender = isSortOnSender;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    if (_eosBlock != null) {
      return _eosBlock;
    }
    // Collect all the rows from the mailbox and sort them
    while (true) {
      TransferableBlock block = _multiConsumer.readBlockBlocking();
      if (block.isDataBlock()) {
        _rows.addAll(block.getContainer());
      } else if (block.isErrorBlock()) {
        return block;
      } else {
        assert block.isSuccessfulEndOfStreamBlock();
        if (!_rows.isEmpty()) {
          _eosBlock = block;
          // TODO: This might not be efficient because we are sorting all the received rows. We should use a k-way merge
          //       when sender side is sorted.
          _rows.sort(
              new SortUtils.SortComparator(_collationKeys, _collationDirections, _collationNullDirections, _dataSchema,
                  false));
          return new TransferableBlock(_rows, _dataSchema, DataBlock.Type.ROW);
        } else {
          return block;
        }
      }
    }
  }

  @Override
  public void close() {
    super.close();
    _rows.clear();
  }

  @Override
  public void cancel(Throwable t) {
    super.cancel(t);
    _rows.clear();
  }
}
