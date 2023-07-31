package org.apache.pinot.query.runtime.operator.utils;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class BlockingMultiConsumer<E> implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlockingMultiConsumer.class);
  private final Object _id;
  protected final List<? extends AsyncStream<E>> _mailboxes;
  protected final ReentrantLock _lock = new ReentrantLock(false);
  protected final Condition _notEmpty = _lock.newCondition();
  private final long _deadlineMs;
  /**
   * An index that used to calculate where do we are going to start reading.
   * The invariant is that we are always going to start reading from {@code _lastRead + 1}.
   * Therefore {@link #_lastRead} must be in the range {@code [-1, mailbox.size() - 1]}
   */
  protected int _lastRead;
  private E _errorBlock = null;

  public BlockingMultiConsumer(Object id, long deadlineMs, List<? extends AsyncStream<E>> asyncProducers) {
    _id = id;
    _deadlineMs = deadlineMs;
    AsyncStream.OnNewData onNewData = this::onData;
    _mailboxes = asyncProducers;
    _mailboxes.forEach(blockProducer -> blockProducer.addOnNewDataListener(onNewData));
    _lastRead = _mailboxes.size() - 1;
  }

  public BlockingMultiConsumer(Object id, long deadlineMs, Executor executor,
      List<? extends BlockingStream<E>> blockingProducers) {
    this(id, deadlineMs,
        blockingProducers.stream().map(p -> new BlockingToAsyncStream<>(executor, p)).collect(Collectors.toList()));
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

  protected void cancelRemainingMailboxes() {
    for (AsyncStream<E> mailbox : _mailboxes) {
      mailbox.cancel();
    }
  }

  public void onData() {
    _lock.lock();
    try {
      _notEmpty.signal();
    } finally {
      _lock.unlock();
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
    if (block == null) { // we didn't find a mailbox ready to read, so we need to be pessimistic
      boolean timeout = false;
      _lock.lock();
      try {
        // let's try to read one more time now that we have the lock
        block = readDroppingSuccessEos();
        while (block == null && !timeout) { // still no data, we need to yield
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("==[RECEIVE]== Yield : " + _id);
          }
          timeout = !_notEmpty.await(_deadlineMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
          block = readDroppingSuccessEos();
        }
        if (timeout) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.warn("==[RECEIVE]== Timeout on: " + _id);
          }
          _errorBlock = onTimeout();
          return _errorBlock;
        }
      } catch (InterruptedException ex) { // we've got interrupted while waiting for the condition
        return onException(ex);
      } finally {
        _lock.unlock();
      }
    }
    return block;
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
    while (block != null && isEos(block) && !_mailboxes.isEmpty()) {
      // we have read a EOS
      AsyncStream<E> removed = _mailboxes.remove(_lastRead);
      // this is done in order to keep the invariant.
      _lastRead--;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("==[RECEIVE]== EOS received : " + _id + " in mailbox: " + removed.getId());
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

  public static class OfTransferableBlock extends BlockingMultiConsumer<TransferableBlock> {
    public OfTransferableBlock(Object id, long deadlineMs,
        List<? extends AsyncStream<TransferableBlock>> asyncProducers) {
      super(id, deadlineMs, asyncProducers);
    }

    public OfTransferableBlock(Object id, long deadlineMs, Executor executor,
        List<? extends BlockingStream<TransferableBlock>> blockingProducers) {
      super(id, deadlineMs, executor, blockingProducers);
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
