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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.utils.AsyncStream;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Receives streams that the plan declares sorted on the sender and merges them by the exchange collation.
///
/// An explicit sender [SortOperator] establishes the row ordering; [MailboxSendOperator] only transports that
/// ordering. The transport marker confirms rollout compatibility and is not itself a sorting mechanism.
///
/// The plan declaration alone is not trusted during a rolling upgrade. Every data block must carry the transport's
/// sender-sort confirmation. Before this operator emits its first row it obtains a head row, or EOS, from every live
/// sender. If any sender's first data is unconfirmed, all tentatively buffered rows are folded into a full receiver
/// sort. Once output starts, losing the confirmation is a protocol violation because already emitted rows cannot be
/// recovered into that fallback.
///
/// The merge reads whichever mailbox is ready instead of blocking on one sender. This prevents a sender that is
/// backpressured by another receiver from creating a cross-receiver wait cycle. Ready rows are emitted in blocks of
/// at most 10,000 while cursor state carries the ordering frontier across calls.
///
/// This operator is driven by a single consumer thread and is not thread-safe.
public class SortedMailboxMergeReceiveOperator extends BaseMailboxReceiveOperator implements SortedMultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SortedMailboxMergeReceiveOperator.class);

  private static final String EXPLAIN_NAME = "SORTED_MAILBOX_MERGE_RECEIVE";
  private static final String MERGE_SCOPE = "SortedMailboxMergeReceiveOperator";
  private static final int MAX_ROWS_PER_MERGED_BLOCK = 10_000;

  private final DataSchema _dataSchema;
  private final List<RelFieldCollation> _collations;
  private final Comparator<Object[]> _comparator;
  private final PriorityQueue<SenderCursor> _readyCursors;
  private final boolean _singleSortedSender;
  /// Senders that have not finished but do not currently have a row ready. Nothing can be emitted while this is
  /// non-empty because any one of these senders may hold the next row.
  private final Set<SenderCursor> _starvedCursors = Collections.newSetFromMap(new IdentityHashMap<>());
  private final Map<AsyncStream<ReceivingMailbox.MseBlockWithStats>, SenderCursor> _cursorsByStream =
      new IdentityHashMap<>();
  private boolean _mergeInitialized;
  private boolean _mergeOutputStarted;
  private boolean _fallbackToSort;

  private final List<Object[]> _rows = new ArrayList<>();

  @Nullable
  private MseBlock _eosBlock;

  public SortedMailboxMergeReceiveOperator(OpChainExecutionContext context, MailboxReceiveNode node) {
    super(context, node);
    Preconditions.checkState(node.isSort(), "Receiver-side sorting must be enabled");
    Preconditions.checkState(node.isSortedOnSender(), "Sender-side sorting must be enabled");
    Preconditions.checkState(!CollectionUtils.isEmpty(node.getCollations()), "Field collations must be set");
    _dataSchema = node.getDataSchema();
    _collations = List.copyOf(node.getCollations());
    _comparator = new SortUtils.SortComparator(_collations, false);
    _readyCursors = new PriorityQueue<>(Math.max(_asyncStreams.size(), 1),
        (cursor1, cursor2) -> _comparator.compare(cursor1.peek(), cursor2.peek()));
    _singleSortedSender = _asyncStreams.size() == 1;
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
  public List<RelFieldCollation> getCollations() {
    return _collations;
  }

  @Override
  protected MseBlock getNextBlock() {
    if (_eosBlock != null) {
      return _eosBlock;
    }
    if (_isEarlyTerminated) {
      return readUntilEos();
    }
    return _singleSortedSender ? readSingleSortedSender() : mergeNextBlock();
  }

  /// Passes through one confirmed sorted sender without copying its rows through the merge heap.
  private MseBlock readSingleSortedSender() {
    while (true) {
      MseBlock block = _multiConsumer.readMseBlockBlocking();
      if (block.isEos()) {
        return terminate(block);
      }
      MseBlock.Data dataBlock = (MseBlock.Data) block;
      checkActiveTerminationAndSampleUsage();
      if (!_multiConsumer.isLastBlockSortedOnSender()) {
        fallbackToFullSort(dataBlock.asRowHeap().getRows());
        return sortAllRows();
      }
      if (dataBlock.getNumRows() > 0) {
        _mergeOutputStarted = true;
        return dataBlock;
      }
    }
  }

  /// Merges the sorted senders, emitting at most [#MAX_ROWS_PER_MERGED_BLOCK] rows per call.
  private MseBlock mergeNextBlock() {
    if (!_mergeInitialized) {
      _mergeInitialized = true;
      for (AsyncStream<ReceivingMailbox.MseBlockWithStats> stream : _asyncStreams) {
        SenderCursor cursor = new SenderCursor(stream);
        _cursorsByStream.put(stream, cursor);
        _starvedCursors.add(cursor);
      }
    }
    List<Object[]> rows = new ArrayList<>(MAX_ROWS_PER_MERGED_BLOCK);
    while (rows.size() < MAX_ROWS_PER_MERGED_BLOCK) {
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
        if (_fallbackToSort) {
          // These rows were already removed from cursors while building this not-yet-emitted block.
          _rows.addAll(rows);
          return sortAllRows();
        }
        continue;
      }
      if (_readyCursors.isEmpty()) {
        break;
      }
      SenderCursor cursor = _readyCursors.poll();
      rows.add(cursor.next());
      if (cursor.hasRow()) {
        _readyCursors.add(cursor);
      } else if (_multiConsumer.isStreamLive(cursor._stream)) {
        _starvedCursors.add(cursor);
      }
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(rows.size(), MERGE_SCOPE,
          _context.getActiveDeadlineMs());
    }
    if (rows.isEmpty()) {
      return terminate(SuccessMseBlock.INSTANCE);
    }
    _mergeOutputStarted = true;
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
    if (!_multiConsumer.isLastBlockSortedOnSender()) {
      fallbackToFullSort(rows);
      return null;
    }
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
      if (cursor != null && !cursor.hasRow()) {
        _starvedCursors.remove(cursor);
      }
    }
  }

  /// Switches to a full receiver sort when a legacy sender omits the transport confirmation.
  private void fallbackToFullSort(List<Object[]> unconfirmedRows) {
    Preconditions.checkState(!_mergeOutputStarted,
        "Sender stopped confirming sorted data after merge output started on stage: %s", _context.getStageId());
    for (SenderCursor cursor : _cursorsByStream.values()) {
      cursor.drainTo(_rows);
    }
    _rows.addAll(unconfirmedRows);
    releaseCursors();
    _fallbackToSort = true;
    checkActiveTerminationAndSampleUsage();
  }

  /// Buffers the remaining sender rows and sorts all rows retained for fallback.
  private MseBlock sortAllRows() {
    while (true) {
      MseBlock block = _multiConsumer.readMseBlockBlocking();
      if (block.isData()) {
        _rows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
        checkActiveTerminationAndSampleUsage();
        continue;
      }
      MseBlock.Eos eosBlock = (MseBlock.Eos) block;
      if (eosBlock.isError() || _rows.isEmpty()) {
        return terminate(eosBlock);
      }
      _rows.sort(SortUtils.withTerminationAndUsageSampling(_comparator, MERGE_SCOPE,
          _context.getActiveDeadlineMs()));
      checkActiveTerminationAndSampleUsage();
      terminate(eosBlock);
      return new RowHeapDataBlock(_rows, _dataSchema);
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
    releaseCursors();
    return eosBlock;
  }

  @Override
  protected void earlyTerminate() {
    super.earlyTerminate();
    if (_eosBlock == null) {
      _rows.clear();
      releaseCursors();
    }
  }

  @Override
  public void close() {
    super.close();
    _rows.clear();
    releaseCursors();
  }

  @Override
  public void cancel(Throwable t) {
    super.cancel(t);
    _rows.clear();
    releaseCursors();
  }

  private void releaseCursors() {
    _readyCursors.clear();
    _starvedCursors.clear();
    for (SenderCursor cursor : _cursorsByStream.values()) {
      cursor.clear();
    }
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
    final AsyncStream<ReceivingMailbox.MseBlockWithStats> _stream;
    private final Deque<List<Object[]>> _pending = new ArrayDeque<>();
    private List<Object[]> _rows = List.of();
    private int _index;

    SenderCursor(AsyncStream<ReceivingMailbox.MseBlockWithStats> stream) {
      _stream = stream;
    }

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

    void drainTo(List<Object[]> rows) {
      if (_index < _rows.size()) {
        rows.addAll(_rows.subList(_index, _rows.size()));
      }
      for (List<Object[]> pendingRows : _pending) {
        rows.addAll(pendingRows);
      }
    }

    int getRetainedRowCount() {
      int retainedRowCount = _rows.size();
      for (List<Object[]> pendingRows : _pending) {
        retainedRowCount += pendingRows.size();
      }
      return retainedRowCount;
    }

    void clear() {
      _pending.clear();
      _rows = List.of();
      _index = 0;
    }
  }
}
