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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.plannode.MailboxMergeReceiveNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.utils.AsyncStream;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// K-way merges streams whose ordering is guaranteed by the logical plan.
///
/// The broker only emits this operator's plan node in a homogeneous cluster. Ordering is established upstream;
/// mailbox senders preserve it without transport metadata or runtime structural checks.
///
/// Reads whichever mailbox is ready to avoid cross-receiver backpressure cycles. Multi-sender output blocks contain
/// at most 10,000 rows; a single sender preserves its input blocks. Read-ahead retained while another sender is starved
/// can approach the legacy full receiver sort's memory footprint.
///
/// Driven by a single consumer thread; not thread-safe.
public class SortedMailboxMergeReceiveOperator extends BaseMailboxReceiveOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SortedMailboxMergeReceiveOperator.class);

  private static final String EXPLAIN_NAME = "SORTED_MAILBOX_MERGE_RECEIVE";
  private static final String MERGE_SCOPE = "SortedMailboxMergeReceiveOperator";
  private final DataSchema _dataSchema;
  private final Comparator<Object[]> _comparator;
  private final PriorityQueue<SenderCursor> _readyCursors;
  private final boolean _singleSortedSender;
  private int _rowsToSkip;
  private long _rowsToEmit;
  /// Senders that have not finished but do not currently have a row ready. Nothing can be emitted while this is
  /// non-empty because any one of these senders may hold the next row.
  private final Set<SenderCursor> _starvedCursors = Collections.newSetFromMap(new IdentityHashMap<>());
  private final Map<AsyncStream<ReceivingMailbox.MseBlockWithStats>, SenderCursor> _cursorsByStream =
      new IdentityHashMap<>();

  @Nullable
  private MseBlock _eosBlock;

  public SortedMailboxMergeReceiveOperator(OpChainExecutionContext context, MailboxMergeReceiveNode node) {
    super(context, node);
    Preconditions.checkState(!CollectionUtils.isEmpty(node.getCollations()), "Field collations must be set");
    _dataSchema = node.getDataSchema();
    _rowsToSkip = Math.max(node.getOffset(), 0);
    _rowsToEmit = node.getFetch() < 0 ? Long.MAX_VALUE : node.getFetch();
    _comparator = new SortUtils.SortComparator(List.copyOf(node.getCollations()), false);
    List<AsyncStream<ReceivingMailbox.MseBlockWithStats>> streams = _multiConsumer.getLiveStreamsSnapshot();
    _readyCursors = new PriorityQueue<>(Math.max(streams.size(), 1),
        (left, right) -> _comparator.compare(left.peek(), right.peek()));
    _singleSortedSender = streams.size() == 1;
    if (!_singleSortedSender) {
      for (AsyncStream<ReceivingMailbox.MseBlockWithStats> stream : streams) {
        SenderCursor cursor = new SenderCursor();
        _cursorsByStream.put(stream, cursor);
        _starvedCursors.add(cursor);
      }
    }
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
  protected MseBlock getNextBlock() {
    if (_eosBlock != null) {
      return _eosBlock;
    }
    if (_isEarlyTerminated) {
      return readUntilEos();
    }
    if (_rowsToEmit == 0) {
      earlyTerminate();
      return readUntilEos();
    }
    while (true) {
      MseBlock block = _singleSortedSender ? readSingleSortedSender() : mergeNextBlock();
      if (block.isEos()) {
        return block;
      }
      if (_rowsToSkip == 0 && _rowsToEmit == Long.MAX_VALUE) {
        return block;
      }
      List<Object[]> rows = ((MseBlock.Data) block).asRowHeap().getRows();
      int from = Math.min(_rowsToSkip, rows.size());
      _rowsToSkip -= from;
      int count = (int) Math.min(rows.size() - from, _rowsToEmit);
      _rowsToEmit -= count;
      if (_rowsToEmit == 0) {
        earlyTerminate();
      }
      if (count > 0) {
        return from == 0 && count == rows.size() ? block
            : new RowHeapDataBlock(rows.subList(from, from + count), _dataSchema);
      }
    }
  }

  /// Passes through one sorted sender without copying its rows through the merge heap.
  private MseBlock readSingleSortedSender() {
    while (true) {
      MseBlock block = _multiConsumer.readMseBlockBlocking();
      if (block.isEos()) {
        return terminate(block);
      }
      MseBlock.Data dataBlock = (MseBlock.Data) block;
      checkActiveTerminationAndSampleUsage();

      if (dataBlock.getNumRows() > 0) {
        return dataBlock;
      }
    }
  }

  /// Merges the sorted senders, emitting at most [SortOperator#DEFAULT_MAX_ROWS_PER_BLOCK] rows per call.
  private MseBlock mergeNextBlock() {
    List<Object[]> rows = new ArrayList<>();
    while (rows.size() < SortOperator.DEFAULT_MAX_ROWS_PER_BLOCK) {
      if (!_starvedCursors.isEmpty()) {
        MseBlock.Eos error;
        if (rows.isEmpty()) {
          error = readOneBlock();
        } else {
          // Rows already removed from the heap are a globally ordered prefix. Consume any immediately available
          // cursor progress so blocks can still be coalesced, but return that safe prefix instead of waiting merely
          // to fill the output block.
          MseBlock block = _multiConsumer.pollMseBlockOrStreamCompletion();
          if (block == null && _multiConsumer.getFinishedStreamsLastRead().isEmpty()) {
            break;
          }
          error = processReadBlock(block);
        }
        if (error != null) {
          return terminate(error);
        }

        continue;
      }
      if (_readyCursors.isEmpty()) {
        break;
      }
      SenderCursor cursor = _readyCursors.remove();
      rows.add(cursor.next());
      if (cursor.hasRow()) {
        _readyCursors.add(cursor);
      } else if (!cursor._finished) {
        _starvedCursors.add(cursor);
      }
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(rows.size(), MERGE_SCOPE,
          _context.getActiveDeadlineMs());
    }
    if (rows.isEmpty()) {
      return terminate(SuccessMseBlock.INSTANCE);
    }
    return new RowHeapDataBlock(rows, _dataSchema);
  }

  /// Reads one block from whichever sender is ready and updates only the cursor that produced it.
  ///
  /// @return the error that ended the read, or `null` when the read succeeded
  @Nullable
  private MseBlock.Eos readOneBlock() {
    return processReadBlock(_multiConsumer.readMseBlockOrStreamCompletionBlocking());
  }

  @Nullable
  private MseBlock.Eos processReadBlock(@Nullable MseBlock block) {
    if (block == null) {
      updateFinishedCursors();
      return null;
    }
    if (block.isEos()) {
      updateFinishedCursors();
      MseBlock.Eos eos = (MseBlock.Eos) block;
      if (eos.isError()) {
        return eos;
      }
      // Aggregate success is returned only after every sender has emitted EOS.
      _starvedCursors.clear();
      return null;
    }
    AsyncStream<ReceivingMailbox.MseBlockWithStats> stream = _multiConsumer.getLastReadStream();
    Preconditions.checkState(stream != null, "Read a data block from no mailbox on stage: %s", _context.getStageId());
    SenderCursor cursor = _cursorsByStream.get(stream);
    Preconditions.checkState(cursor != null, "Read a data block from unknown mailbox: %s", stream.getId());
    List<Object[]> rows = ((MseBlock.Data) block).asRowHeap().getRows();
    checkActiveTerminationAndSampleUsage();

    cursor.offer(rows);
    updateFinishedCursors();
    if (cursor.hasRow() && _starvedCursors.remove(cursor)) {
      _readyCursors.add(cursor);
    }
    return null;
  }

  /// Removes only the starved cursors whose EOS was consumed by the last read.
  private void updateFinishedCursors() {
    for (AsyncStream<ReceivingMailbox.MseBlockWithStats> stream : _multiConsumer.getFinishedStreamsLastRead()) {
      SenderCursor cursor = _cursorsByStream.get(stream);
      if (cursor != null) {
        cursor._finished = true;
        if (!cursor.hasRow()) {
          _starvedCursors.remove(cursor);
          _cursorsByStream.remove(stream);
        }
      }
    }
  }

  private void checkActiveTerminationAndSampleUsage() {
    QueryThreadContext.checkTerminationAndSampleUsage(MERGE_SCOPE, _context.getActiveDeadlineMs());
  }

  /// Drops data that raced with early termination until aggregate EOS or a sender error arrives.
  private MseBlock readUntilEos() {
    while (true) {
      MseBlock block = _multiConsumer.readMseBlockBlocking();
      if (block.isEos()) {
        return terminate(block);
      }
    }
  }

  private MseBlock terminate(MseBlock eosBlock) {
    onEos();
    _eosBlock = eosBlock;
    releaseBuffers();
    return eosBlock;
  }

  @Override
  protected void earlyTerminate() {
    super.earlyTerminate();
    releaseBuffers();
  }

  @Override
  protected void releaseBuffers() {
    releaseCursors();
  }

  @Override
  protected boolean hasBufferedState() {
    return !_cursorsByStream.isEmpty() || !_readyCursors.isEmpty() || !_starvedCursors.isEmpty();
  }

  private void releaseCursors() {
    _readyCursors.clear();
    _starvedCursors.clear();
    _cursorsByStream.clear();
  }

  @VisibleForTesting
  long getRetainedCursorRowCount() {
    long retainedRowCount = 0;
    for (SenderCursor cursor : _cursorsByStream.values()) {
      retainedRowCount += cursor.getRetainedRowCount();
    }
    return retainedRowCount;
  }

  private static class SenderCursor {
    private boolean _finished;
    private final Deque<List<Object[]>> _pending = new ArrayDeque<>();
    private List<Object[]> _rows = List.of();
    private int _index;

    void offer(List<Object[]> rows) {
      if (!rows.isEmpty()) {
        _pending.add(rows);
      }
    }

    boolean hasRow() {
      while (_index == _rows.size()) {
        _rows = List.of();
        _index = 0;
        List<Object[]> next = _pending.poll();
        if (next == null) {
          return false;
        }
        _rows = next;
      }
      return true;
    }

    Object[] peek() {
      return _rows.get(_index);
    }

    Object[] next() {
      return _rows.get(_index++);
    }



    int getRetainedRowCount() {
      int retainedRowCount = _rows.size();
      for (List<Object[]> pendingRows : _pending) {
        retainedRowCount += pendingRows.size();
      }
      return retainedRowCount;
    }
  }
}
