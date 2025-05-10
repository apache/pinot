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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is a utility class that helps to consume multiple mailboxes in a blocking manner by a single thread.
 *
 * The reader entry point is {@link #readBlockBlocking()} which will block until some of the mailboxes is ready to be
 * read. The method is blocking and will return the next block to be consumed. This method is designed to be called by
 * a single thread we call the consumer thread.
 *
 * All other methods but the ones specifically specified can only be called by the consumer thread.
 * @param <E>
 */
public abstract class BlockingMultiStreamConsumer<E> implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlockingMultiStreamConsumer.class);
  private final Object _id;
  protected final List<? extends AsyncStream<E>> _mailboxes;
  protected final ArrayBlockingQueue<Boolean> _newDataReady = new ArrayBlockingQueue<>(1);
  private final long _deadlineMs;
  /**
   * An index that used to calculate where do we are going to start reading.
   * The invariant is that we are always going to start reading from {@code _lastRead + 1}.
   * Therefore {@link #_lastRead} must be in the range {@code [-1, mailbox.size() - 1]}
   */
  protected int _lastRead;
  @Nullable
  private E _errorBlock = null;

  public BlockingMultiStreamConsumer(Object id, long deadlineMs, List<? extends AsyncStream<E>> asyncProducers) {
    _id = id;
    _deadlineMs = deadlineMs;
    AsyncStream.OnNewData onNewData = this::onData;
    _mailboxes = asyncProducers;
    _mailboxes.forEach(blockProducer -> blockProducer.addOnNewDataListener(onNewData));
    _lastRead = _mailboxes.size() - 1;
  }

  /**
   * Returns whether the element is considered an error element or not.
   *
   * This method is called by the consumer thread.
   */
  protected abstract boolean isError(E element);

  /**
   * Returns whether the element is considered a successful end of stream element or not.
   *
   * This method is called by the consumer thread.
   */
  protected abstract boolean isSuccess(E element);

  /**
   * This method is called whenever a {@link #isSuccess(Object) successful EOS} is read from one of the mailboxes.
   *
   * It is guaranteed that the received element is an EOS as defined by {@link #isSuccess(Object)}.
   * No more messages are going to be read from that mailbox.
   *
   * This method is called by the consumer thread.
   */
  protected abstract void onMailboxSuccess(E element);

  /**
   * This method is called whenever a timeout is reached while reading an element.
   *
   * This method is called by the consumer thread.
   */
  protected abstract E onTimeout();

  /**
   * This method is called whenever an exception (other than timeout) is thrown while reading an element.
   *
   * This method is called by the consumer thread.
   */
  protected abstract E onException(Exception e);

  /**
   * This method is called whenever all mailboxes emitted EOS.
   *
   * This method is called by the consumer thread.
   */
  protected abstract E onSuccess();

  /**
   * This method is called when an error is found in any of the mailboxes.
   *
   * After this method is called no more messages are going to be read from the mailboxes.
   */
  protected abstract void onError(E element);

  /**
   * This method must be called when the consumer is not going to read anymore from the mailboxes.
   *
   * <strong>This method can be called from any thread</strong>.
   */
  @Override
  public void close() {
    cancelRemainingMailboxes();
  }

  /**
   * This method is called whenever the consumer is cancelled.
   *
   * <strong>This method can be called from any thread</strong>.
   */
  public void cancel(Throwable t) {
    cancelRemainingMailboxes();
  }

  /**
   * This method is called whenever the consumer is early terminated.
   *
   * This method is called by the consumer thread.
   */
  public void earlyTerminate() {
    for (AsyncStream<E> mailbox : _mailboxes) {
      mailbox.earlyTerminate();
    }
  }

  /**
   * This method is called whenever the consumer is early terminated.
   *
   * <strong>This method can be called from any thread</strong>.
   */
  protected void cancelRemainingMailboxes() {
    for (AsyncStream<E> mailbox : _mailboxes) {
      mailbox.cancel();
    }
  }

  /**
   * This method is called whenever the consumer is early terminated.
   *
   * <strong>This method can be called by any thread</strong>, although it is expected to be called by producer threads.
   */
  protected void onData() {
    if (_newDataReady.offer(Boolean.TRUE)) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("New data notification delivered on " + _id + ". " + System.identityHashCode(_newDataReady));
      }
    } else if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("New data notification ignored on " + _id + ". " + System.identityHashCode(_newDataReady));
    }
  }

  /**
   * Reads the next block for any ready mailbox or blocks until some of them is ready.
   *
   * The method implements a sequential read semantic. Meaning that:
   * <ol>
   *   <li>EOS is only returned when all mailboxes already emitted EOS or there are no mailboxes</li>
   *   <li>If an error is read from a mailbox, the error is returned</li>
   *   <li>If data is read from a mailbox, that data block is returned</li>
   *   <li>If no mailbox is ready, the calling thread is blocked</li>
   * </ol>
   *
   * Right now the implementation tries to be fair. If one call returned the block from mailbox {@code i}, then next
   * call will look for mailbox {@code i+1}, {@code i+2}... in a circular manner.
   *
   * In order to unblock a thread blocked here, {@link #onData()} should be called.
   *
   * This method is called by the consumer thread.
   */
  public E readBlockBlocking() {
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
    } catch (InterruptedException ex) {
      return onException(ex);
    }
  }

  /**
   * This is a utility method that tries to read from the different mailboxes in a circular manner.
   *
   * The method is a bit more complex than expected because ir order to simplify {@link #readBlockBlocking} we added
   * some extra logic here. For example, this method checks for timeouts, adds some logs, releases mailboxes that
   * emitted EOS and in case an error block is found, stores it.
   *
   * @return the new block to consume or null if none is found. EOS is only emitted when all mailboxes already emitted
   * EOS.
   */
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

  /**
   * The utility method that actually does the circular reading trying to be fair.
   * @return The first block that is found on any mailbox, including EOS.
   */
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

    public OfMseBlock(OpChainExecutionContext context,
        List<? extends AsyncStream<ReceivingMailbox.MseBlockWithStats>> asyncProducers) {
      super(context.getId(), context.getDeadlineMs(), asyncProducers);
      _stageId = context.getStageId();
      _stats = MultiStageQueryStats.emptyStats(context.getStageId());
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
      // TODO: Add the sender stage id to the error message
      String errMsg = "Timed out on stage " + _stageId + " waiting for data sent by a child stage";
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
      // TODO: Add the sender stage id to the error message
      String errMsg = "Found an error on stage " + _stageId + " while reading from a child stage";
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
          serializedStats = Collections.emptyList();
        }
      } catch (IOException ioEx) {
        LOGGER.warn("Could not serialize stats", ioEx);
        serializedStats = Collections.emptyList();
      }
      ErrorMseBlock errorBlock = ErrorMseBlock.fromException(code.asException(errMsg));
      return new ReceivingMailbox.MseBlockWithStats(errorBlock, serializedStats);
    }

    @Override
    protected ReceivingMailbox.MseBlockWithStats onSuccess() {
      return new ReceivingMailbox.MseBlockWithStats(SuccessMseBlock.INSTANCE, Collections.emptyList());
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
