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

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  private E _errorBlock = null;

  public BlockingMultiStreamConsumer(Object id, long deadlineMs, List<? extends AsyncStream<E>> asyncProducers) {
    _id = id;
    _deadlineMs = deadlineMs;
    AsyncStream.OnNewData onNewData = this::onData;
    _mailboxes = asyncProducers;
    _mailboxes.forEach(blockProducer -> blockProducer.addOnNewDataListener(onNewData));
    _lastRead = _mailboxes.size() - 1;
  }

  protected abstract boolean isError(E element);

  protected abstract boolean isEos(E element);

  protected abstract E onTimeout();

  protected abstract E onException(Exception e);

  protected abstract E onEos();

  @Override
  public void close() {
    cancelRemainingMailboxes();
  }

  public void cancel(Throwable t) {
    cancelRemainingMailboxes();
  }

  public void earlyTerminate() {
    for (AsyncStream<E> mailbox : _mailboxes) {
      mailbox.earlyTerminate();
    }
  }

  protected void cancelRemainingMailboxes() {
    for (AsyncStream<E> mailbox : _mailboxes) {
      mailbox.cancel();
    }
  }

  public void onData() {
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
   * In order to unblock a thread blocked here, {@link #onData()} should be called.   *
   */
  public E readBlockBlocking() {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("==[RECEIVE]== Enter getNextBlock from: " + _id + " mailboxSize: " + _mailboxes.size());
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
   * This is a utility method that reads tries to read from the different mailboxes in a circular manner.
   *
   * The method is a bit more complex than expected because ir order to simplify {@link #readBlockBlocking} we added
   * some extra logic here. For example, this method checks for timeouts, add some logs, releases mailboxes that emitted
   * EOS and in case an error block is found, stores it.
   *
   * @return the new block to consume or null if none is found. EOS is only emitted when all mailboxes already emitted
   * EOS.
   */
  @Nullable
  private E readDroppingSuccessEos() {
    if (System.currentTimeMillis() > _deadlineMs) {
      _errorBlock = onTimeout();
      return _errorBlock;
    }

    E block = readBlockOrNull();
    while (block != null && isEos(block)) {
      // we have read an EOS
      assert !_mailboxes.isEmpty() : "readBlockOrNull should return null when there are no mailboxes";
      AsyncStream<E> removed = _mailboxes.remove(_lastRead);
      // this is done in order to keep the invariant.
      _lastRead--;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("==[RECEIVE]== EOS received : " + _id + " in mailbox: " + removed.getId()
            + " (" + _mailboxes.size() + " mailboxes alive)");
      }

      block = readBlockOrNull();
    }
    if (_mailboxes.isEmpty()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("==[RECEIVE]== Finished : " + _id);
      }
      return onEos();
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

  public static class OfTransferableBlock extends BlockingMultiStreamConsumer<TransferableBlock> {
    public OfTransferableBlock(Object id, long deadlineMs,
        List<? extends AsyncStream<TransferableBlock>> asyncProducers) {
      super(id, deadlineMs, asyncProducers);
    }

    @Override
    protected boolean isError(TransferableBlock element) {
      return element.isErrorBlock();
    }

    @Override
    protected boolean isEos(TransferableBlock element) {
      return element.isSuccessfulEndOfStreamBlock();
    }

    @Override
    protected TransferableBlock onTimeout() {
      return TransferableBlockUtils.getErrorTransferableBlock(QueryException.EXECUTION_TIMEOUT_ERROR);
    }

    @Override
    protected TransferableBlock onException(Exception e) {
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    }

    @Override
    protected TransferableBlock onEos() {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    }
  }
}
