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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.utils.BlockingMultiStreamConsumer.StreamHandle;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// This `SortedMailboxReceiveOperator` receives data from a [ReceivingMailbox] and serves it out from the
/// [#nextBlock()] API in a globally sorted manner.
///
/// It supports two strategies, selected at construction time:
///
///   - **Accumulate-then-sort** (default): every row from every mailbox is buffered, sorted once at EOS, and
///     returned in a single data block. This is the historical behavior and is used whenever the k-way merge is not
///     enabled.
///   - **Streaming k-way merge**: when the sender is known to emit each mailbox already sorted on the receiver's
///     collation, the rows are merged incrementally with a min-heap and emitted in bounded blocks (of at most
///     `blockSize` rows). Global order is preserved across block boundaries because the heap state carries over
///     between [#getNextBlock()] calls. Senders are deliberately allowed to backpressure — that is where the
///     memory advantage comes from — but no sender is left parked indefinitely: every [#refill] takes at most
///     one element from each sibling stream, and a stalled merge drains ready siblings fully into a per-stream
///     backlog. Under sustained key skew that backlog trades some of the memory advantage for liveness.
///
/// The k-way merge is enabled only when the `streamingSortedMailboxReceive` query option is
/// `true` **and** [MailboxReceiveNode#isSortedOnSender()] is true. All other combinations fall back to
/// the accumulate-then-sort path. Which path was taken is reported in the query response `stageStats` as the
/// `kWayMergeUsed` stat (see [BaseMailboxReceiveOperator.StatKey#K_WAY_MERGE_USED]).
///
/// Like the rest of the receive operators, this class is driven by a single consumer thread; it is not thread-safe.
public class SortedMailboxReceiveOperator extends BaseMailboxReceiveOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SortedMailboxReceiveOperator.class);

  private static final String EXPLAIN_NAME = "SORTED_MAILBOX_RECEIVE";

  /// Default upper bound on the number of rows emitted per block in the streaming k-way merge, used when the
  /// `streamingSortedMailboxReceiveBlockSize` query option is not set. Defined locally to avoid introducing a
  /// dependency on `pinot-core` (where `SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY` lives).
  private static final int DEFAULT_BLOCK_SIZE = 10_000;

  private final DataSchema _dataSchema;
  private final List<RelFieldCollation> _collations;
  private final List<Object[]> _rows = new ArrayList<>();

  // Streaming k-way merge state. The merge-only fields are meaningful only when _useKWayMerge is true.
  private final boolean _useKWayMerge;
  private final int _blockSize;
  private final Comparator<Object[]> _comparator;
  // Built lazily on the first merge call so priming (driving every handle to first-data/EOS/error) happens once.
  private PriorityQueue<Cursor> _heap;
  // Per-sender merge state (backlog + exhaustion), built once alongside the heap during priming.
  private List<StreamState> _streams;
  // Mutable view of _streams used only for the sibling drain scan in refill(): entries are removed once their stream
  // is exhausted so a stalled call doesn't keep re-scanning senders that can never produce more data.
  private List<StreamState> _activeStreams;
  private boolean _primed;
  // Last row handed out by the merge, used to verify the sender-sorted precondition (see checkNonDecreasing).
  @Nullable
  private Object[] _lastEmitted;

  private MseBlock _eosBlock;

  public SortedMailboxReceiveOperator(OpChainExecutionContext context, MailboxReceiveNode node) {
    super(context, node);
    Preconditions.checkState(!CollectionUtils.isEmpty(node.getCollations()), "Field collations must be set");
    _dataSchema = node.getDataSchema();
    _collations = node.getCollations();
    // reverse=false => the collation minimum sits at the min-heap head (honors per-field ASC/DESC + null direction).
    _comparator = new SortUtils.SortComparator(_collations, false);
    _useKWayMerge = QueryOptionsUtils.isStreamingSortedMailboxReceiveEnabled(context.getOpChainMetadata())
        && node.isSortedOnSender();
    // Recorded eagerly (rather than on the first merged block) so the stat reports the configured path even when the
    // query returns no rows. Surfaces in the query response stageStats as "kWayMergeUsed".
    _statMap.merge(StatKey.K_WAY_MERGE_USED, _useKWayMerge);
    Integer blockSize = QueryOptionsUtils.getStreamingSortedMailboxReceiveBlockSize(context.getOpChainMetadata());
    _blockSize = blockSize != null ? blockSize : DEFAULT_BLOCK_SIZE;
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
    if (_useKWayMerge) {
      return getNextMergedBlock();
    }
    // Collect all the rows from the mailbox and sort them
    while (true) {
      MseBlock block = _multiConsumer.readMseBlockBlocking();
      if (block.isData()) {
        _rows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
        continue;
      }
      MseBlock.Eos eosBlock = (MseBlock.Eos) block;
      onEos();
      _eosBlock = eosBlock;
      if (eosBlock.isError()) {
        return eosBlock;
      } else {
        if (!_rows.isEmpty()) {
          _rows.sort(_comparator);
          return new RowHeapDataBlock(_rows, _dataSchema);
        } else {
          return block;
        }
      }
    }
  }

  /// Streaming k-way merge over the per-sender [StreamHandle]s. Emits at most [#_blockSize] rows per call,
  /// keeping the heap state between calls so global order is preserved across blocks.
  private MseBlock getNextMergedBlock() {
    if (_isEarlyTerminated) {
      // Stop pulling new data; drive every handle to EOS (so receiving stats are folded in) and finish.
      return drainToEos();
    }
    if (!_primed) {
      List<StreamHandle<ReceivingMailbox.MseBlockWithStats>> handles = streamHandles();
      Comparator<Object[]> comparator = _comparator;
      _heap = new PriorityQueue<>(Math.max(1, handles.size()),
          (a, b) -> comparator.compare(a.head(), b.head()));
      _streams = new ArrayList<>(handles.size());
      for (StreamHandle<ReceivingMailbox.MseBlockWithStats> handle : handles) {
        _streams.add(new StreamState(handle));
      }
      // Built before priming: refill()'s sibling drain scan runs during priming too (a stream can be starved on its
      // very first poll), so _activeStreams must already reflect every stream.
      _activeStreams = new ArrayList<>(_streams);
      // Prime: drive every stream to its first data block / EOS / error before the first pop, so the heap holds a head
      // for every still-active mailbox and the min is the global min.
      for (StreamState state : _streams) {
        Cursor cursor = refill(state);
        if (_eosBlock != null) {
          // An error was found while priming; refill already cached it and folded stats.
          return _eosBlock;
        }
        if (cursor != null) {
          _heap.add(cursor);
        }
      }
      _primed = true;
    }

    // Initial capacity is capped at DEFAULT_BLOCK_SIZE so a very large configured _blockSize does not eagerly allocate
    // a huge backing array up front; for larger blocks the list grows amortized as rows are appended.
    List<Object[]> out = new ArrayList<>(Math.min(_blockSize, DEFAULT_BLOCK_SIZE));
    while (out.size() < _blockSize) {
      if (_heap.isEmpty()) {
        onEos();
        _eosBlock = SuccessMseBlock.INSTANCE;
        return out.isEmpty() ? _eosBlock : new RowHeapDataBlock(out, _dataSchema);
      }
      Cursor cursor = _heap.poll();
      Object[] row = cursor.head();
      checkNonDecreasing(row, cursor);
      out.add(row);
      _lastEmitted = row;
      if (cursor.advance()) {
        // Still has rows in the current block: reseat with the new head.
        _heap.add(cursor);
      } else {
        // Current block exhausted: refill THIS mailbox before the next pop to restore the heap invariant.
        Cursor refilled = refill(cursor._state);
        if (_eosBlock != null) {
          // An error was found while refilling; short-circuit immediately.
          return _eosBlock;
        }
        if (refilled != null) {
          _heap.add(refilled);
        }
        // else this mailbox reached EOS and is dropped from the merge.
      }
    }
    return new RowHeapDataBlock(out, _dataSchema);
  }

  /// Verifies the precondition the whole merge rests on: each mailbox stream is already sorted on the receiver's
  /// collation. Only that guarantee makes "min of the heads" the global min, and nothing downstream re-sorts —
  /// `SortOperator` skips its priority queue when its input is a `SortedMailboxReceiveOperator`. If a sender
  /// violates it (a plan shape the fragmenter gate should have rejected, a leaf that concatenates two independently
  /// sorted runs, a collation mismatch), the merge would otherwise emit misordered rows and, under LIMIT/OFFSET, simply
  /// return the wrong rows with no error anywhere. One comparison per emitted row converts that into a hard failure.
  private void checkNonDecreasing(Object[] row, Cursor cursor) {
    if (_lastEmitted != null && _comparator.compare(_lastEmitted, row) > 0) {
      throw new IllegalStateException(
          "Sorted mailbox receive got out-of-order rows from mailbox: " + cursor._state._handle.getId()
              + ". The sender was declared sorted-on-sender but is not sorted on the receiver's collation");
    }
  }

  /// Produces the next [Cursor] for `state` without head-of-line blocking. The merge can only emit once it
  /// has the next row from `state`, but it must never park on `state` alone while sibling mailboxes fill up:
  /// that lets senders backpressure and deadlocks the single-threaded pipeline. So while `state` has nothing ready
  /// this drains any *other* ready stream into its backlog (relieving that sender), and only parks (on the shared
  /// new-data signal) when no stream anywhere has data. Buffered rows keep per-stream order, so global sort order is
  /// preserved. Returns `null` when `state` reaches success EOS (dropped from the merge) or on error (which
  /// is cached in [#_eosBlock] after folding stats via [#onEos()]).
  @Nullable
  private Cursor refill(StreamState state) {
    // Bounded relief pass: take at most one element from every other active stream before serving this one. Without
    // it, siblings are polled only while the merge is stalled, so with disjoint key ranges a fast sender can sit
    // parked on a full mailbox (capacity ReceivingMailbox.DEFAULT_MAX_PENDING_BLOCKS) for the whole query, holding an
    // MSE worker thread. One poll per refill (i.e. per consumed block, not per row) keeps every sender advancing
    // without eagerly draining a fast sender's entire output into the backlog, which would give back the memory
    // advantage the merge exists for.
    relieveSiblingsOnce(state);
    if (_eosBlock != null) {
      return null;
    }
    while (true) {
      if (!state._backlog.isEmpty()) {
        return new Cursor(state, state._backlog.poll());
      }
      if (_eosBlock != null) {
        return null;
      }
      if (state._handle.isExhausted()) {
        // Success EOS already seen and backlog drained: drop this mailbox from the merge.
        return null;
      }
      // Try to advance THIS stream without blocking.
      if (pollOnce(state)) {
        if (state._handle.isExhausted()) {
          _activeStreams.remove(state);
        }
        // Buffered rows (loop serves the backlog), hit success EOS (loop returns null), or read an empty block (retry).
        continue;
      }
      // This stream has nothing ready. Drain any OTHER ready stream to relieve its sender's backpressure; that may in
      // turn unblock the sender feeding this stream. Streams that reach exhaustion are pruned from _activeStreams so
      // later calls (for any mailbox) don't keep re-scanning senders that can never produce more data.
      boolean progressed = false;
      Iterator<StreamState> it = _activeStreams.iterator();
      while (it.hasNext()) {
        StreamState other = it.next();
        if (other == state) {
          continue;
        }
        while (pollOnce(other)) {
          progressed = true;
          if (_eosBlock != null) {
            return null;
          }
        }
        if (other._handle.isExhausted()) {
          it.remove();
        }
      }
      if (progressed) {
        // Draining may have delivered data (or woken this stream's sender); retry before parking.
        continue;
      }
      // Nothing ready anywhere: park until any stream signals new data (or the deadline is hit).
      ReceivingMailbox.MseBlockWithStats timedOut = _multiConsumer.awaitDataOrTerminal();
      if (timedOut != null) {
        onEos();
        _eosBlock = timedOut.getBlock();
        return null;
      }
      // Woken: loop and retry.
    }
  }

  /// Polls every active stream other than `state` at most once, non-blocking, pruning any that become exhausted.
  /// See [#refill] for why this runs unconditionally rather than only when the merge stalls.
  private void relieveSiblingsOnce(StreamState state) {
    if (_eosBlock != null) {
      return;
    }
    Iterator<StreamState> it = _activeStreams.iterator();
    while (it.hasNext()) {
      StreamState other = it.next();
      if (other == state) {
        continue;
      }
      pollOnce(other);
      if (_eosBlock != null) {
        return;
      }
      if (other._handle.isExhausted()) {
        it.remove();
      }
    }
  }

  /// Polls one stream once (non-blocking). Buffers any non-empty data rows into the stream's backlog and caches an
  /// error into [#_eosBlock]. Exhaustion itself is tracked by the underlying [StreamHandle#isExhausted()], not
  /// duplicated here. Returns `true` if any element (data, success EOS, or error) was read, `false` if the
  /// stream had nothing ready.
  private boolean pollOnce(StreamState state) {
    if (state._handle.isExhausted() || _eosBlock != null) {
      return false;
    }
    ReceivingMailbox.MseBlockWithStats element = state._handle.poll();
    if (element == null) {
      return false;
    }
    MseBlock block = element.getBlock();
    if (block.isError()) {
      onEos();
      _eosBlock = block;
      return true;
    }
    if (block.isSuccess()) {
      return true;
    }
    List<Object[]> rows = ((MseBlock.Data) block).asRowHeap().getRows();
    if (!rows.isEmpty()) {
      state._backlog.add(rows);
    }
    // Empty data blocks carry no head; returning true lets refill loop and poll again.
    return true;
  }

  /// Drains every handle to its terminal element after early termination, folding receiving stats. Like [#refill], this
  /// must not head-of-line block on one handle while sibling mailboxes still have data buffered: doing so would let
  /// their senders backpressure and deadlock the pipeline, exactly as it would during normal merging. So this polls
  /// every not-yet-exhausted handle in round-robin passes (discarding data, since early termination means the result is
  /// no longer needed) and only parks when a full pass makes no progress on any handle. Returns the cached error block
  /// if any handle yields one, otherwise a success EOS.
  private MseBlock drainToEos() {
    List<StreamHandle<ReceivingMailbox.MseBlockWithStats>> handles = streamHandles();
    int numRemaining = 0;
    boolean[] exhausted = new boolean[handles.size()];
    for (int i = 0; i < handles.size(); i++) {
      if (handles.get(i).isExhausted()) {
        exhausted[i] = true;
      } else {
        numRemaining++;
      }
    }
    while (numRemaining > 0) {
      boolean progressed = false;
      for (int i = 0; i < handles.size(); i++) {
        if (exhausted[i]) {
          continue;
        }
        StreamHandle<ReceivingMailbox.MseBlockWithStats> handle = handles.get(i);
        ReceivingMailbox.MseBlockWithStats element = handle.poll();
        if (element == null) {
          continue;
        }
        progressed = true;
        MseBlock block = element.getBlock();
        if (block.isError()) {
          onEos();
          _eosBlock = block;
          return block;
        }
        if (handle.isExhausted()) {
          exhausted[i] = true;
          numRemaining--;
        }
        // Data blocks are discarded; a still-active handle is retried on a later pass.
      }
      if (!progressed) {
        // No handle had anything ready this pass: park until any stream signals new data (or the deadline is hit).
        ReceivingMailbox.MseBlockWithStats timedOut = _multiConsumer.awaitDataOrTerminal();
        if (timedOut != null) {
          onEos();
          _eosBlock = timedOut.getBlock();
          return _eosBlock;
        }
      }
    }
    onEos();
    _eosBlock = SuccessMseBlock.INSTANCE;
    return _eosBlock;
  }

  @Override
  public void close() {
    super.close();
    _rows.clear();
    clearMergeState();
  }

  @Override
  public void cancel(Throwable t) {
    super.cancel(t);
    _rows.clear();
    clearMergeState();
  }

  private void clearMergeState() {
    if (_heap != null) {
      _heap.clear();
    }
    if (_streams != null) {
      for (StreamState state : _streams) {
        state._backlog.clear();
      }
    }
    if (_activeStreams != null) {
      _activeStreams.clear();
    }
  }

  /// Per-sender merge state: the stream handle plus a backlog of data blocks buffered ahead of the merge's current
  /// position. Rows are staged here (in arrival order, which the sender guarantees is sorted) when the merge drains
  /// this mailbox while waiting on another stream. Exhaustion is tracked by [StreamHandle#isExhausted()] on the handle
  /// itself, not duplicated here.
  private static final class StreamState {
    final StreamHandle<ReceivingMailbox.MseBlockWithStats> _handle;
    final Deque<List<Object[]>> _backlog = new ArrayDeque<>();

    StreamState(StreamHandle<ReceivingMailbox.MseBlockWithStats> handle) {
      _handle = handle;
    }
  }

  /// A cursor over one mailbox's current data block. Holds the owning [StreamState] so the merge can refill this
  /// specific mailbox (from its backlog or the stream) when the block is exhausted. Created only for non-empty blocks,
  /// so [#head()] is valid until [#advance()] returns `false`.
  private static final class Cursor {
    final StreamState _state;
    final List<Object[]> _rows;
    int _idx;
    // Cached _rows.get(_idx). The heap comparator reads this on every comparison (~2*log2(k) per emitted row), so it
    // is kept as a field rather than re-resolved through the List interface each time.
    Object[] _head;

    Cursor(StreamState state, List<Object[]> rows) {
      _state = state;
      _rows = rows;
      _head = rows.get(0);
    }

    Object[] head() {
      return _head;
    }

    /// Advances past the current row. Returns `true` if a new head is available in this block.
    boolean advance() {
      _idx++;
      if (_idx < _rows.size()) {
        _head = _rows.get(_idx);
        return true;
      }
      _head = null;
      return false;
    }
  }
}
