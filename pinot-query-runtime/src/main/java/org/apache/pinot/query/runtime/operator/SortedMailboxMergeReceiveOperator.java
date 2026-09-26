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
import java.util.Set;
import javax.annotation.Nullable;
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
import org.apache.pinot.spi.exception.QueryErrorCode;
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
/// backpressured by another receiver from creating a cross-receiver wait cycle. Multi-sender merge and fallback rows
/// are emitted in blocks of at most 10,000 while cursor state carries the ordering frontier across calls. A confirmed
/// single sender is passed through with its original block boundaries. A fast sender can be read ahead while another
/// sender is starved, so retained input is workload-dependent and can approach the legacy full receiver sort in the
/// worst case.
///
/// This operator is driven by a single consumer thread and is not thread-safe.
public class SortedMailboxMergeReceiveOperator extends BaseMailboxReceiveOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SortedMailboxMergeReceiveOperator.class);

  private static final String EXPLAIN_NAME = "SORTED_MAILBOX_MERGE_RECEIVE";
  private static final String MERGE_SCOPE = "SortedMailboxMergeReceiveOperator";
  private final DataSchema _dataSchema;
  private final Comparator<Object[]> _comparator;
  private final SenderCursorHeap _readyCursors;
  private final boolean _singleSortedSender;
  /// Senders that have not finished but do not currently have a row ready. Nothing can be emitted while this is
  /// non-empty because any one of these senders may hold the next row.
  private final Set<SenderCursor> _starvedCursors = Collections.newSetFromMap(new IdentityHashMap<>());
  private final Map<AsyncStream<ReceivingMailbox.MseBlockWithStats>, SenderCursor> _cursorsByStream =
      new IdentityHashMap<>();
  private boolean _mergeOutputStarted;
  private boolean _fallbackToSort;
  private boolean _tryEqualHeadMerge;
  private int _fallbackOutputIndex = -1;

  /// Rows buffered only for the mixed-version fallback. The sorted list is handed downstream as-is, so cleanup must
  /// drop this reference rather than clear it.
  @Nullable
  private List<Object[]> _rows;

  @Nullable
  private MseBlock _eosBlock;

  public SortedMailboxMergeReceiveOperator(OpChainExecutionContext context, MailboxReceiveNode node) {
    super(context, node);
    Preconditions.checkState(node.isSort(), "Receiver-side sorting must be enabled");
    Preconditions.checkState(node.isSortedOnSender(), "Sender-side sorting must be enabled");
    Preconditions.checkState(!CollectionUtils.isEmpty(node.getCollations()), "Field collations must be set");
    _dataSchema = node.getDataSchema();
    _comparator = new SortUtils.SortComparator(List.copyOf(node.getCollations()), false);
    List<AsyncStream<ReceivingMailbox.MseBlockWithStats>> streams = _multiConsumer.getLiveStreamsSnapshot();
    _readyCursors = new SenderCursorHeap(streams.size(), _comparator);
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
    if (_fallbackOutputIndex >= 0 && _rows != null) {
      return emitFallbackBlock();
    }
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

  /// Merges the sorted senders, emitting at most [SortOperator#DEFAULT_MAX_ROWS_PER_BLOCK] rows per call.
  private MseBlock mergeNextBlock() {
    ArrayList<Object[]> rows = new ArrayList<>(0);
    while (rows.size() < SortOperator.DEFAULT_MAX_ROWS_PER_BLOCK) {
      if (!_starvedCursors.isEmpty()) {
        MseBlock.Eos error;
        boolean receivedMoreRows = false;
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
          receivedMoreRows = block != null && block.isData() && ((MseBlock.Data) block).getNumRows() > 0;
        }
        if (error != null) {
          return terminate(error);
        }
        if (_fallbackToSort) {
          // These rows were already removed from cursors while building this not-yet-emitted block.
          _rows.addAll(rows);
          return sortAllRows();
        }
        if (receivedMoreRows) {
          // A refill proves this is not a one-block result. Restore the established full-block capacity once so
          // fragmented input cannot trigger repeated growth while this output block is assembled.
          rows.ensureCapacity(SortOperator.DEFAULT_MAX_ROWS_PER_BLOCK);
        }
        continue;
      }
      if (_readyCursors.isEmpty()) {
        break;
      }
      if (rows.isEmpty()) {
        // Keep empty and tiny results cheap without sacrificing the one-allocation path for full output blocks.
        // Sum all currently buffered rows once; later output blocks retain the established full-block capacity.
        int initialCapacity = _mergeOutputStarted ? SortOperator.DEFAULT_MAX_ROWS_PER_BLOCK
            : _readyCursors.getCappedAvailableRowCount(SortOperator.DEFAULT_MAX_ROWS_PER_BLOCK);
        // ArrayList grows by 1.5x. Round near-full first blocks up now so a small refill cannot allocate an oversized
        // replacement in addition to the nearly full initial array.
        if (initialCapacity + (initialCapacity >> 1) >= SortOperator.DEFAULT_MAX_ROWS_PER_BLOCK) {
          initialCapacity = SortOperator.DEFAULT_MAX_ROWS_PER_BLOCK;
        }
        rows.ensureCapacity(initialCapacity);
      }
      if (_tryEqualHeadMerge && _readyCursors.size() > 1) {
        int remaining = SortOperator.DEFAULT_MAX_ROWS_PER_BLOCK - rows.size();
        if (remaining >= _readyCursors.size()) {
          if (_readyCursors.allHeadsEqual()) {
            int previousRowCount = rows.size();
            List<SenderCursor> exhausted = _readyCursors.advanceEqualHeads(rows);
            if (exhausted != null) {
              for (SenderCursor cursor : exhausted) {
                if (!cursor._finished) {
                  _starvedCursors.add(cursor);
                }
              }
            }
            if (!_starvedCursors.isEmpty()) {
              _tryEqualHeadMerge = false;
            }
            checkActiveTerminationAndSampleUsageAfterBatch(previousRowCount, rows.size());
            continue;
          } else {
            _tryEqualHeadMerge = false;
          }
        } else {
          _readyCursors.ensureOrdered();
        }
      }
      if (_readyCursors.size() == 4) {
        advanceFourCursors(rows);
        continue;
      }
      SenderCursor cursor = _readyCursors.peek();
      rows.add(cursor.next());
      if (cursor.hasRow()) {
        // The cursor's key can only move forward, so restoring the heap from the root takes one sift-down. A generic
        // PriorityQueue poll followed by add performs two independent heap repairs for every emitted row.
        _readyCursors.updateTop();
      } else if (!cursor._finished) {
        _readyCursors.removeTop();
        _starvedCursors.add(cursor);
        _tryEqualHeadMerge = false;
      } else {
        _readyCursors.removeTop();
        _tryEqualHeadMerge = true;
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
      if (_starvedCursors.isEmpty()) {
        _tryEqualHeadMerge = true;
      }
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
      _tryEqualHeadMerge = true;
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
    if (_starvedCursors.isEmpty()) {
      _tryEqualHeadMerge = true;
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

  /// Switches to a full receiver sort when a legacy sender omits the transport confirmation.
  private void fallbackToFullSort(List<Object[]> unconfirmedRows) {
    if (_mergeOutputStarted) {
      throw QueryErrorCode.INTERNAL.asException(
          "Sender ordering confirmation changed after merge output started on stage " + _context.getStageId()
              + "; retry after the rolling upgrade completes or disable windowSortOnSender");
    }
    _rows = new ArrayList<>();
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
    assert _rows != null : "Fallback rows must not be released while the operator is running";
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
      onEos();
      _eosBlock = eosBlock;
      _fallbackOutputIndex = 0;
      return emitFallbackBlock();
    }
  }

  private MseBlock emitFallbackBlock() {
    assert _rows != null : "Fallback rows must exist while fallback output is pending";
    int endIndex = Math.min(_fallbackOutputIndex + SortOperator.DEFAULT_MAX_ROWS_PER_BLOCK, _rows.size());
    List<Object[]> rows = new ArrayList<>(_rows.subList(_fallbackOutputIndex, endIndex));
    _fallbackOutputIndex = endIndex;
    if (endIndex == _rows.size()) {
      _rows = null;
    }
    return new RowHeapDataBlock(rows, _dataSchema);
  }

  private void checkActiveTerminationAndSampleUsage() {
    QueryThreadContext.checkTerminationAndSampleUsage(MERGE_SCOPE, _context.getActiveDeadlineMs());
  }

  private void checkActiveTerminationAndSampleUsageAfterBatch(int previousRowCount, int currentRowCount) {
    int mask = QueryThreadContext.CHECK_TERMINATION_AND_SAMPLE_USAGE_RECORD_MASK;
    if ((previousRowCount & ~mask) != (currentRowCount & ~mask)) {
      checkActiveTerminationAndSampleUsage();
    }
  }

  /// Merges the common four-sender case with two comparisons per row instead of a heap sift-down. Stop when one
  /// cursor runs dry because its next block may contain the global minimum.
  private void advanceFourCursors(List<Object[]> output) {
    SenderCursor cursor0 = _readyCursors.get(0);
    SenderCursor cursor1 = _readyCursors.get(1);
    SenderCursor cursor2 = _readyCursors.get(2);
    SenderCursor cursor3 = _readyCursors.get(3);
    _readyCursors.clear();

    Object[] head0 = cursor0.peek();
    Object[] head1 = cursor1.peek();
    Object[] head2 = cursor2.peek();
    Object[] head3 = cursor3.peek();
    int leftWinner = betterHead(head0, 0, head1, 1);
    int rightWinner = betterHead(head2, 2, head3, 3);
    int winner = betterWinner(leftWinner, head0, head1, rightWinner, head2, head3);
    boolean exhausted = false;
    while (output.size() < SortOperator.DEFAULT_MAX_ROWS_PER_BLOCK && !exhausted) {
      switch (winner) {
        case 0:
          output.add(head0);
          cursor0.advance();
          head0 = cursor0.hasRow() ? cursor0.peek() : null;
          exhausted = head0 == null;
          leftWinner = betterHead(head0, 0, head1, 1);
          break;
        case 1:
          output.add(head1);
          cursor1.advance();
          head1 = cursor1.hasRow() ? cursor1.peek() : null;
          exhausted = head1 == null;
          leftWinner = betterHead(head0, 0, head1, 1);
          break;
        case 2:
          output.add(head2);
          cursor2.advance();
          head2 = cursor2.hasRow() ? cursor2.peek() : null;
          exhausted = head2 == null;
          rightWinner = betterHead(head2, 2, head3, 3);
          break;
        case 3:
          output.add(head3);
          cursor3.advance();
          head3 = cursor3.hasRow() ? cursor3.peek() : null;
          exhausted = head3 == null;
          rightWinner = betterHead(head2, 2, head3, 3);
          break;
        default:
          throw new IllegalStateException("Four-cursor tournament has no winner");
      }
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(output.size(), MERGE_SCOPE,
          _context.getActiveDeadlineMs());
      if (!exhausted) {
        winner = betterWinner(leftWinner, head0, head1, rightWinner, head2, head3);
      }
    }

    int cursorState = restoreFourCursor(cursor0, head0) | restoreFourCursor(cursor1, head1)
        | restoreFourCursor(cursor2, head2) | restoreFourCursor(cursor3, head3);
    if ((cursorState & 1) != 0) {
      _tryEqualHeadMerge = false;
    } else if ((cursorState & 2) != 0) {
      _tryEqualHeadMerge = true;
    }
  }

  /// Returns bit 0 for a starved cursor and bit 1 for a finished cursor.
  private int restoreFourCursor(SenderCursor cursor, @Nullable Object[] head) {
    if (head != null) {
      _readyCursors.add(cursor);
      return 0;
    }
    if (!cursor._finished) {
      _starvedCursors.add(cursor);
      return 1;
    }
    return 2;
  }

  private int betterWinner(int leftWinner, Object[] head0, Object[] head1, int rightWinner, Object[] head2,
      Object[] head3) {
    Object[] leftHead = leftWinner == 0 ? head0 : head1;
    Object[] rightHead = rightWinner == 2 ? head2 : head3;
    return betterHead(leftHead, leftWinner, rightHead, rightWinner);
  }

  private int betterHead(@Nullable Object[] left, int leftCursor, @Nullable Object[] right, int rightCursor) {
    if (left == null) {
      return right == null ? -1 : rightCursor;
    }
    if (right == null) {
      return leftCursor;
    }
    return _comparator.compare(left, right) <= 0 ? leftCursor : rightCursor;
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
    _rows = null;
    releaseCursors();
  }

  @Override
  protected boolean hasBufferedState() {
    return _rows != null || !_cursorsByStream.isEmpty() || !_readyCursors.isEmpty() || !_starvedCursors.isEmpty();
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

    void advance() {
      _index++;
    }

    int getCappedAvailableRowCount(int limit) {
      int rowCount = _rows.size() - _index;
      if (rowCount >= limit) {
        return limit;
      }
      for (List<Object[]> pendingRows : _pending) {
        int remaining = limit - rowCount;
        if (pendingRows.size() >= remaining) {
          return limit;
        }
        rowCount += pendingRows.size();
      }
      return rowCount;
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
  }

  /// Fixed-capacity min-heap for the current row from each sender.
  ///
  /// A merge advances only the minimum cursor, and a sorted cursor's next key cannot move backwards. Updating the
  /// root in place therefore needs one sift-down instead of a generic heap's poll-plus-add pair.
  private static class SenderCursorHeap {
    private final SenderCursor[] _heap;
    private final Comparator<Object[]> _comparator;
    private int _size;
    private boolean _dirty;

    SenderCursorHeap(int capacity, Comparator<Object[]> comparator) {
      _heap = new SenderCursor[Math.max(capacity, 1)];
      _comparator = comparator;
    }

    void add(SenderCursor cursor) {
      Preconditions.checkState(_size < _heap.length, "Cannot add more cursors than senders");
      int index = _size++;
      while (index > 0) {
        int parentIndex = (index - 1) >>> 1;
        SenderCursor parent = _heap[parentIndex];
        if (_comparator.compare(cursor.peek(), parent.peek()) >= 0) {
          break;
        }
        _heap[index] = parent;
        index = parentIndex;
      }
      _heap[index] = cursor;
    }

    SenderCursor peek() {
      return _heap[0];
    }

    SenderCursor get(int index) {
      return _heap[index];
    }

    void updateTop() {
      siftDown(_heap[0], _size);
    }

    void removeTop() {
      int newSize = --_size;
      SenderCursor replacement = _heap[newSize];
      _heap[newSize] = null;
      if (newSize > 0) {
        siftDown(replacement, newSize);
      }
    }

    int size() {
      return _size;
    }

    int getCappedAvailableRowCount(int limit) {
      int rowCount = 0;
      for (int i = 0; i < _size; i++) {
        int remaining = limit - rowCount;
        int availableRowCount = _heap[i].getCappedAvailableRowCount(remaining);
        if (availableRowCount >= remaining) {
          return limit;
        }
        rowCount += availableRowCount;
      }
      return rowCount;
    }

    void ensureOrdered() {
      if (_dirty) {
        heapify();
      }
    }

    /// Returns whether every sender currently has the same complete collation key.
    ///
    /// Equal heads can be advanced together because tie order across senders is unspecified. If an earlier equal-head
    /// advance left the array unordered and the new heads diverged, this method restores the heap before returning.
    boolean allHeadsEqual() {
      Object[] first = _heap[0].peek();
      for (int i = 1; i < _size; i++) {
        if (_comparator.compare(first, _heap[i].peek()) != 0) {
          if (_dirty) {
            heapify();
          }
          return false;
        }
      }
      // Equal keys satisfy the heap invariant regardless of their array order.
      _dirty = false;
      return true;
    }

    /// Emits one comparator-equal row from every cursor without per-cursor heap repairs.
    @Nullable
    List<SenderCursor> advanceEqualHeads(List<Object[]> output) {
      List<SenderCursor> exhausted = null;
      int active = 0;
      int oldSize = _size;
      for (int i = 0; i < oldSize; i++) {
        SenderCursor cursor = _heap[i];
        output.add(cursor.next());
        if (cursor.hasRow()) {
          _heap[active++] = cursor;
        } else {
          if (exhausted == null) {
            exhausted = new ArrayList<>();
          }
          exhausted.add(cursor);
        }
      }
      for (int i = active; i < oldSize; i++) {
        _heap[i] = null;
      }
      _size = active;
      if (active < oldSize) {
        heapify();
      } else if (active > 1) {
        _dirty = true;
      }
      return exhausted;
    }

    private void heapify() {
      for (int i = (_size >>> 1) - 1; i >= 0; i--) {
        siftDown(i, _heap[i], _size);
      }
      _dirty = false;
    }

    private void siftDown(SenderCursor cursor, int size) {
      siftDown(0, cursor, size);
    }

    private void siftDown(int index, SenderCursor cursor, int size) {
      int half = size >>> 1;
      while (index < half) {
        int childIndex = (index << 1) + 1;
        SenderCursor child = _heap[childIndex];
        int rightIndex = childIndex + 1;
        if (rightIndex < size
            && _comparator.compare(_heap[rightIndex].peek(), child.peek()) < 0) {
          childIndex = rightIndex;
          child = _heap[rightIndex];
        }
        if (_comparator.compare(cursor.peek(), child.peek()) <= 0) {
          break;
        }
        _heap[index] = child;
        index = childIndex;
      }
      _heap[index] = cursor;
    }

    boolean isEmpty() {
      return _size == 0;
    }

    void clear() {
      for (int i = 0; i < _size; i++) {
        _heap[i] = null;
      }
      _size = 0;
      _dirty = false;
    }
  }
}
