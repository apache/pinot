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
package org.apache.pinot.query.runtime.operator.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.TerminationException;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// This class is a utility class that helps to consume multiple mailboxes in a blocking manner by a single thread.
///
/// The reader entry point is [#readBlockBlocking()] which will block until some of the mailboxes is ready to be
/// read. The method is blocking and will return the next block to be consumed. This method is designed to be called by
/// a single thread we call the consumer thread.
///
/// All other methods but the ones specifically specified can only be called by the consumer thread.
/// @param <E>
public abstract class BlockingMultiStreamConsumer<E> implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlockingMultiStreamConsumer.class);
  private final Object _id;
  protected final List<? extends AsyncStream<E>> _mailboxes;
  protected final ArrayBlockingQueue<Boolean> _newDataReady = new ArrayBlockingQueue<>(1);
  private final long _deadlineMs;
  /// An index that used to calculate where do we are going to start reading.
  /// The invariant is that we are always going to start reading from `_lastRead + 1`.
  /// Therefore [#_lastRead] must be in the range `[-1, mailbox.size() - 1]`
  protected int _lastRead;
  @Nullable
  private E _errorBlock = null;

  /// A consumer instance reads either in round-robin mode (via [#readBlockBlocking()]) or in per-stream mode
  /// (via [#streamHandles()] / [StreamHandle#readBlocking()]), never both. The mode is latched on first use
  /// and mixing the two throws [IllegalStateException]. Both modes share the same EOS/error/timeout bookkeeping
  /// (the abstract hooks below), so stats stay correct regardless of which mode is used.
  private enum Mode {
    UNSET, ROUND_ROBIN, PER_STREAM
  }

  private Mode _mode = Mode.UNSET;
  /// Lazily built per-stream handles, used only in [Mode#PER_STREAM]. Built once from [#_mailboxes] (which
  /// the mode guard keeps the round-robin path from mutating) and cached so [#streamHandles()] is idempotent.
  @Nullable
  private List<StreamHandle<E>> _handles = null;

  public BlockingMultiStreamConsumer(Object id, long deadlineMs, List<? extends AsyncStream<E>> asyncProducers) {
    _id = id;
    _deadlineMs = deadlineMs;
    AsyncStream.OnNewData onNewData = this::onData;
    _mailboxes = asyncProducers;
    _mailboxes.forEach(blockProducer -> blockProducer.addOnNewDataListener(onNewData));
    _lastRead = _mailboxes.size() - 1;
  }

  /// Returns whether the element is considered an error element or not.
  ///
  /// This method is called by the consumer thread.
  protected abstract boolean isError(E element);

  /// Returns whether the element is considered a successful end of stream element or not.
  ///
  /// This method is called by the consumer thread.
  protected abstract boolean isSuccess(E element);

  /// This method is called whenever a [`successful EOS`]\[#isSuccess(Object)\] is read from one of the mailboxes.
  ///
  /// It is guaranteed that the received element is an EOS as defined by [#isSuccess(Object)].
  /// No more messages are going to be read from that mailbox.
  ///
  /// This method is called by the consumer thread.
  protected abstract void onMailboxSuccess(E element);

  /// This method is called whenever a timeout is reached while reading an element.
  ///
  /// This method is called by the consumer thread.
  protected abstract E onTimeout();

  /// This method is called whenever an exception (other than timeout) is thrown while reading an element.
  ///
  /// This method is called by the consumer thread.
  protected abstract E onException(Exception e);

  /// This method is called whenever all mailboxes emitted EOS.
  ///
  /// This method is called by the consumer thread.
  protected abstract E onSuccess();

  /// This method is called when an error is found in any of the mailboxes.
  ///
  /// After this method is called no more messages are going to be read from the mailboxes.
  protected abstract void onError(E element);

  /// This method must be called when the consumer is not going to read anymore from the mailboxes.
  ///
  /// **This method can be called from any thread**.
  @Override
  public void close() {
    cancelRemainingMailboxes();
  }

  /// This method is called whenever the consumer is cancelled.
  ///
  /// **This method can be called from any thread**.
  public void cancel(Throwable t) {
    cancelRemainingMailboxes();
  }

  /// This method is called whenever the consumer is early terminated.
  ///
  /// This method is called by the consumer thread.
  public void earlyTerminate() {
    for (AsyncStream<E> mailbox : _mailboxes) {
      mailbox.earlyTerminate();
    }
  }

  /// This method is called whenever the consumer is early terminated.
  ///
  /// **This method can be called from any thread**.
  protected void cancelRemainingMailboxes() {
    for (AsyncStream<E> mailbox : _mailboxes) {
      mailbox.cancel();
    }
  }

  /// This method is called whenever the consumer is early terminated.
  ///
  /// **This method can be called by any thread**, although it is expected to be called by producer
  /// threads.
  protected void onData() {
    if (_newDataReady.offer(Boolean.TRUE)) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("New data notification delivered on " + _id + ". " + System.identityHashCode(_newDataReady));
      }
    } else if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("New data notification ignored on " + _id + ". " + System.identityHashCode(_newDataReady));
    }
  }

  /// Reads the next block for any ready mailbox or blocks until some of them is ready.
  ///
  /// The method implements a sequential read semantic. Meaning that:
  ///
  /// 1. EOS is only returned when all mailboxes already emitted EOS or there are no mailboxes
  /// 2. If an error is read from a mailbox, the error is returned
  /// 3. If data is read from a mailbox, that data block is returned
  /// 4. If no mailbox is ready, the calling thread is blocked
  ///
  /// Right now the implementation tries to be fair. If one call returned the block from mailbox `i`, then next
  /// call will look for mailbox `i+1`, `i+2`... in a circular manner.
  ///
  /// In order to unblock a thread blocked here, [#onData()] should be called.
  ///
  /// This method is called by the consumer thread.
  public E readBlockBlocking() {
    latchMode(Mode.ROUND_ROBIN);
    if (LOGGER.isTraceEnabled()) {
      String mailboxIds = _mailboxes.stream()
          .map(AsyncStream::getId)
          .map(Object::toString)
          .collect(Collectors.joining(","));
      LOGGER.trace("==[RECEIVE]== Enter getNextBlock from: " + _id + ". Mailboxes: " + mailboxIds);
    }
    // Standard optimistic execution. First we try to read without acquiring the lock.
    E block = readDroppingSuccessEos();
    if (block != null) {
      return block;
    }
    try {
      boolean timeout;
      while (true) { // we didn't find a mailbox ready to read, so we need to be pessimistic
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("==[RECEIVE]== Blocked on : " + _id + ". " + System.identityHashCode(_newDataReady));
        }
        long timeoutMs = _deadlineMs - System.currentTimeMillis();
        timeout = _newDataReady.poll(timeoutMs, TimeUnit.MILLISECONDS) == null;
        if (timeout) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.warn("==[RECEIVE]== Timeout on: " + _id);
          }
          _errorBlock = onTimeout();
          return _errorBlock;
        }
        LOGGER.debug("==[RECEIVE]== More data available. Trying to read again");
        block = readDroppingSuccessEos();
        if (block != null) {
          if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("==[RECEIVE]== Ready to emit on: " + _id);
          }
          return block;
        }
      }
    } catch (Exception e) {
      _errorBlock = onException(e);
      return _errorBlock;
    }
  }

  /// This is a utility method that tries to read from the different mailboxes in a circular manner.
  ///
  /// The method is a bit more complex than expected because ir order to simplify [#readBlockBlocking] we added
  /// some extra logic here. For example, this method checks for timeouts, adds some logs, releases mailboxes that
  /// emitted EOS and in case an error block is found, stores it.
  ///
  /// @return the new block to consume or null if none is found. EOS is only emitted when all mailboxes already emitted
  /// EOS.
  @Nullable
  private E readDroppingSuccessEos() {
    if (_errorBlock != null) {
      return _errorBlock;
    }
    if (System.currentTimeMillis() > _deadlineMs) {
      _errorBlock = onTimeout();
      return _errorBlock;
    }

    E block = readBlockOrNull();
    while (block != null && isSuccess(block)) {
      // we have read an EOS
      assert !_mailboxes.isEmpty() : "readBlockOrNull should return null when there are no mailboxes";
      AsyncStream<E> removed = _mailboxes.remove(_lastRead);
      // this is done in order to keep the invariant.
      _lastRead--;
      if (LOGGER.isDebugEnabled()) {
        String ids = _mailboxes.stream()
            .map(AsyncStream::getId)
            .map(Object::toString)
            .collect(Collectors.joining(","));
        LOGGER.debug("==[RECEIVE]== EOS received : " + _id + " in mailbox: " + removed.getId()
            + " (mailboxes alive: " + ids + ")");
      }
      onMailboxSuccess(block);

      block = readBlockOrNull();
    }
    if (_mailboxes.isEmpty()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("==[RECEIVE]== Finished : " + _id);
      }
      return onSuccess();
    }
    if (block != null) {
      if (LOGGER.isTraceEnabled()) {
        AsyncStream<E> mailbox = _mailboxes.get(_lastRead);
        LOGGER.trace("==[RECEIVE]== Returned block from : " + _id + " in mailbox: " + mailbox.getId());
      }
      if (isError(block)) {
        AsyncStream<E> mailbox = _mailboxes.get(_lastRead);
        LOGGER.info("==[RECEIVE]== Error block found from : " + _id + " in mailbox " + mailbox.getId());
        _errorBlock = block;
        onError(block);
      }
    }
    return block;
  }

  /// The utility method that actually does the circular reading trying to be fair.
  /// @return The first block that is found on any mailbox, including EOS.
  @Nullable
  private E readBlockOrNull() {
    // in case _lastRead is _mailboxes.size() - 1, we just skip this loop.
    for (int i = _lastRead + 1; i < _mailboxes.size(); i++) {
      AsyncStream<E> mailbox = _mailboxes.get(i);
      E block = mailbox.poll();
      if (block != null) {
        _lastRead = i;
        return block;
      }
    }
    for (int i = 0; i <= _lastRead; i++) {
      AsyncStream<E> mailbox = _mailboxes.get(i);
      E block = mailbox.poll();
      if (block != null) {
        _lastRead = i;
        return block;
      }
    }
    return null;
  }

  /// Parks the consumer thread until any stream signals new data or the deadline is reached. Used by the per-stream
  /// (k-way merge) read mode: after finding every stream momentarily empty, a caller waits here for progress instead of
  /// committing to a single stream (which would head-of-line block while sibling mailboxes fill up and their senders
  /// backpressure, deadlocking the pipeline). Returns `null` when woken by new data (the caller should re-poll the
  /// streams), or the terminal error element (already routed through [#onTimeout()]) when the deadline is
  /// exceeded.
  ///
  /// This method is called by the consumer thread.
  @Nullable
  public E awaitDataOrTerminal() {
    latchMode(Mode.PER_STREAM);
    if (_errorBlock != null) {
      return _errorBlock;
    }
    try {
      long timeoutMs = _deadlineMs - System.currentTimeMillis();
      if (timeoutMs <= 0 || _newDataReady.poll(timeoutMs, TimeUnit.MILLISECONDS) == null) {
        _errorBlock = onTimeout();
        return _errorBlock;
      }
      return null;
    } catch (Exception e) {
      _errorBlock = onException(e);
      return _errorBlock;
    }
  }

  /// Latches the read mode on first use and enforces that a single consumer instance is read in exactly one mode.
  ///
  /// @throws IllegalStateException if a different mode was already latched.
  private void latchMode(Mode mode) {
    if (_mode == Mode.UNSET) {
      _mode = mode;
    } else if (_mode != mode) {
      throw new IllegalStateException("BlockingMultiStreamConsumer mixes round-robin and per-stream reads");
    }
  }

  /// A narrow per-stream handle for the k-way-merge read mode. Returned by [#streamHandles()].
  ///
  /// Unlike [#readBlockBlocking()], which reads from all mailboxes in a fair round-robin and hides which mailbox a
  /// block came from, a handle reads from one specific stream so a caller (the k-way merge) can advance each sender
  /// independently. All terminal bookkeeping (success EOS, error, timeout, exception) still routes through the same
  /// hooks the round-robin path uses, so `calculateStats()` stays correct.
  ///
  /// All methods are called by the single consumer thread only.
  ///
  /// @param <T> the element type, matching the enclosing consumer.
  public interface StreamHandle<T> {
    /// The id of the underlying stream. Mostly used for logging.
    Object getId();

    /// Blocking read of the next element from this stream only. Returns a data element, a success-EOS element (after
    /// which [#isExhausted()] is true), or an error/timeout element (after which the whole consumer is in error
    /// and every handle returns that same error element on subsequent calls). Never returns null.
    T readBlocking();

    /// Non-blocking read of the next element from this stream only. Returns a data element, a success-EOS element
    /// (after which [#isExhausted()] is true), an error element (after which the whole consumer is in error), or
    /// `null` when nothing is ready yet. Unlike [#readBlocking()] this never parks the consumer thread, so
    /// the k-way merge can drain whichever siblings are ready while waiting for the specific stream it needs.
    @Nullable
    T poll();

    /// Returns true once this stream has emitted a success EOS, meaning no more data will come from it.
    boolean isExhausted();

    /// Sets the underlying stream to early-terminate state, asking for the metadata block.
    void earlyTerminate();
  }

  /// Returns one [StreamHandle] per mailbox in declaration order (an empty list when there are no mailboxes).
  ///
  /// The first call latches this consumer into per-stream mode; subsequent calls to [#readBlockBlocking()] throw.
  /// The returned list is built once and cached, so repeated calls return the same handles. All returned handles must
  /// be driven by the single consumer thread (they share this consumer's wakeup and error state with no extra
  /// synchronization).
  public List<StreamHandle<E>> streamHandles() {
    latchMode(Mode.PER_STREAM);
    if (_handles == null) {
      List<StreamHandle<E>> handles = new ArrayList<>(_mailboxes.size());
      for (AsyncStream<E> mailbox : _mailboxes) {
        handles.add(new Handle(mailbox));
      }
      _handles = Collections.unmodifiableList(handles);
    }
    return _handles;
  }

  /// Per-stream handle implementation. Reads from a single captured [AsyncStream] (not an index into the
  /// round-robin-mutated [#_mailboxes]), reusing the shared [#_newDataReady] wakeup and [#_deadlineMs]
  /// deadline. A wakeup meant for another stream simply causes a re-poll that returns null and loops, which is correct
  /// because [AsyncStream#poll()] reads from the per-mailbox queue, not from [#_newDataReady].
  private class Handle implements StreamHandle<E> {
    private final AsyncStream<E> _stream;
    private boolean _exhausted;
    /// The success-EOS element seen on this stream. Cached so that, once exhausted, we return it without polling an
    /// already-released mailbox again (and without re-merging its stats).
    @Nullable
    private E _eosElement;

    Handle(AsyncStream<E> stream) {
      _stream = stream;
    }

    @Override
    public Object getId() {
      return _stream.getId();
    }

    @Override
    public boolean isExhausted() {
      return _exhausted;
    }

    @Override
    public void earlyTerminate() {
      _stream.earlyTerminate();
    }

    @Override
    public E readBlocking() {
      if (_errorBlock != null) {
        // A global error (from this or any other handle) short-circuits every handle.
        return _errorBlock;
      }
      if (_exhausted) {
        // EOS already seen; do not poll an already-released mailbox again. Stats were merged once when first seen.
        assert _eosElement != null : "_eosElement must be set whenever _exhausted is true";
        return _eosElement;
      }
      // Mirror the round-robin path (readDroppingSuccessEos): a deadline already in the past times out before any read,
      // so both modes report a timeout rather than racing a last-moment block.
      if (System.currentTimeMillis() > _deadlineMs) {
        _errorBlock = onTimeout();
        return _errorBlock;
      }
      // Optimistic read without waiting.
      E block = pollThisStream();
      if (block != null) {
        return block;
      }
      try {
        while (true) {
          long timeoutMs = _deadlineMs - System.currentTimeMillis();
          if (_newDataReady.poll(timeoutMs, TimeUnit.MILLISECONDS) == null) {
            _errorBlock = onTimeout();
            return _errorBlock;
          }
          block = pollThisStream();
          if (block != null) {
            return block;
          }
        }
      } catch (Exception e) {
        _errorBlock = onException(e);
        return _errorBlock;
      }
    }

    @Nullable
    @Override
    public E poll() {
      if (_errorBlock != null) {
        // A global error (from this or any other handle) short-circuits every handle.
        return _errorBlock;
      }
      if (_exhausted) {
        // Success EOS already seen; do not poll an already-released mailbox again.
        return null;
      }
      return pollThisStream();
    }

    /// Polls this stream once, routing any terminal element through the shared hooks.
    ///
    /// @return the element read (data, success EOS, or error), or null if nothing is ready yet.
    @Nullable
    private E pollThisStream() {
      E block = _stream.poll();
      if (block == null) {
        return null;
      }
      if (isError(block)) {
        _errorBlock = block;
        onError(block);
        return block;
      }
      if (isSuccess(block)) {
        _exhausted = true;
        _eosElement = block;
        onMailboxSuccess(block);
        return block;
      }
      return block;
    }
  }

  /// A [BlockingMultiStreamConsumer] that reads [ReceivingMailbox.MseBlockWithStats]s.
  ///
  /// This class is also the entry point for
  /// [BaseMailboxReceiveOperator][org.apache.pinot.query.runtime.operator.BaseMailboxReceiveOperator]s to read the
  /// blocks from the mailboxes.
  /// Remember that in mailboxes blocks also contain stats from upstream (aka children) stages while
  /// [MultiStageOperators][org.apache.pinot.query.runtime.operator.MultiStageOperator] communicate using [MseBlock]s,
  /// which do not contain stats.
  /// This class receives the [ReceivingMailbox.MseBlockWithStats]s, extracts the [MseBlock] and accumulates the stats.
  /// This is why it is recommended to call [#readMseBlockBlocking()] instead of [#readBlockBlocking()] to get the next
  /// block and then call [#calculateStats()] to get the stats once the stream finishes.
  public static class OfMseBlock extends BlockingMultiStreamConsumer<ReceivingMailbox.MseBlockWithStats> {

    private final int _stageId;
    @Nullable
    private MultiStageQueryStats _stats;
    private final int _senderStageId;

    public OfMseBlock(OpChainExecutionContext context,
        List<? extends AsyncStream<ReceivingMailbox.MseBlockWithStats>> asyncProducers, int senderStageId) {
      super(context.getId(), context.getPassiveDeadlineMs(), asyncProducers);
      _stageId = context.getStageId();
      _stats = MultiStageQueryStats.emptyStats(context.getStageId());
      _senderStageId = senderStageId;
    }

    @Override
    protected boolean isError(ReceivingMailbox.MseBlockWithStats element) {
      return element.getBlock().isError();
    }

    @Override
    protected boolean isSuccess(ReceivingMailbox.MseBlockWithStats element) {
      return element.getBlock().isSuccess();
    }

    @Override
    protected void onMailboxSuccess(ReceivingMailbox.MseBlockWithStats element) {
      mergeStats(element);
    }

    @Override
    protected void onError(ReceivingMailbox.MseBlockWithStats element) {
      mergeStats(element);
    }

    private void mergeStats(ReceivingMailbox.MseBlockWithStats element) {
      try {
        MultiStageQueryStats stats = _stats;
        if (_stats != null) {
          stats.mergeUpstream(element.getSerializedStats(), true);
        }
      } catch (Exception e) {
        // If there is any error merging stats, continue without them
        LOGGER.warn("Error merging stats", e);
        _stats = null;
      }
    }

    @Override
    protected ReceivingMailbox.MseBlockWithStats onTimeout() {
      // Use the terminate exception when query is explicitly terminated.
      TerminationException terminateException = QueryThreadContext.getTerminateException();
      if (terminateException != null) {
        return onException(terminateException.getErrorCode(), terminateException.getMessage());
      }
      String errMsg = "Timed out on stage " + _stageId + " waiting for data from child stage " + _senderStageId;
      // We log this case as debug because:
      // - The opchain will already log a stackless message once the opchain fail
      // - The trace is not useful (the log message is good enough to find where we failed)
      // - We may fail for timeout reasons often and in case there is an execution error this log will be noisy and
      //   will make it more difficult to find the real error in the log.
      LOGGER.debug(errMsg);
      return onException(QueryErrorCode.EXECUTION_TIMEOUT, errMsg);
    }

    @Override
    protected ReceivingMailbox.MseBlockWithStats onException(Exception e) {
      // Use the terminate exception when query is explicitly terminated.
      TerminationException terminateException = QueryThreadContext.getTerminateException();
      if (terminateException != null) {
        return onException(terminateException.getErrorCode(), terminateException.getMessage());
      }
      String errMsg = "Found an error on stage " + _stageId + " while reading from a child stage " + _senderStageId;
      // We log this case as warn because contrary to the timeout case, it should be rare to finish an execution
      // with an exception and the stack trace may be useful to find the root cause.
      LOGGER.warn(errMsg, e);
      return onException(QueryErrorCode.INTERNAL, errMsg);
    }

    private ReceivingMailbox.MseBlockWithStats onException(QueryErrorCode code, String errMsg) {
      List<DataBuffer> serializedStats;
      try {
        if (_stats != null) {
          serializedStats = _stats.serialize();
        } else {
          serializedStats = List.of();
        }
      } catch (IOException ioEx) {
        LOGGER.warn("Could not serialize stats", ioEx);
        serializedStats = List.of();
      }
      ErrorMseBlock errorBlock = ErrorMseBlock.fromException(code.asException(errMsg));
      return new ReceivingMailbox.MseBlockWithStats(errorBlock, serializedStats);
    }

    @Override
    protected ReceivingMailbox.MseBlockWithStats onSuccess() {
      return new ReceivingMailbox.MseBlockWithStats(SuccessMseBlock.INSTANCE, List.of());
    }

    public MultiStageQueryStats calculateStats() {
      MultiStageQueryStats stats = _stats;
      if (_stats == null) { // possible in case of error
        stats = MultiStageQueryStats.emptyStats(_stageId);
      }
      return MultiStageQueryStats.copy(stats);
    }

    /// Reads the next block for any ready mailbox or blocks until some of them is ready.
    /// Operators should call this method instead of [#readBlockBlocking()] to get the next block, given stats are not
    /// useful for them while reading the blocks.
    public MseBlock readMseBlockBlocking() {
      return readBlockBlocking().getBlock();
    }
  }
}
