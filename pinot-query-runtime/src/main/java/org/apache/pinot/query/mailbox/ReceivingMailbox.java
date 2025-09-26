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
package org.apache.pinot.query.mailbox;

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.ThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SerializedDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Mailbox that's used to receive data. Ownership of the ReceivingMailbox is with the MailboxService, which is unlike
 * the {@link SendingMailbox} whose ownership lies with the send operator. This is because the ReceivingMailbox can be
 * initialized even before the corresponding OpChain is registered on the receiver, whereas the SendingMailbox is
 * initialized when the send operator is running.
 *
 * There is a single ReceivingMailbox for each {@link MailboxReceiveOperator}.
 * The offer methods will be called when new blocks are received from different sources. For example local workers will
 * directly call {@link #offer(MseBlock, List, long)} while each remote worker opens a GPRC channel where messages
 * are sent in raw format and {@link #offerRaw(List, long)} is called from them.
 */
@ThreadSafe
public class ReceivingMailbox {
  public static final int DEFAULT_MAX_PENDING_BLOCKS = 5;

  private static final Logger LOGGER = LoggerFactory.getLogger(ReceivingMailbox.class);

  private final String _id;
  // TODO: Make the queue size configurable
  // TODO: Revisit if this is the correct way to apply back pressure
  /// The queue where blocks are going to be stored.
  private final CancellableBlockingQueue _blocks = new CancellableBlockingQueue(DEFAULT_MAX_PENDING_BLOCKS);
  private long _lastArriveTime = System.currentTimeMillis();

  @Nullable
  private volatile Reader _reader;
  private final StatMap<StatKey> _stats = new StatMap<>(StatKey.class);

  public ReceivingMailbox(String id) {
    _id = id;
  }

  public void registeredReader(Reader reader) {
    if (_reader != null) {
      throw new IllegalArgumentException("Only one reader is supported");
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("==[MAILBOX]== Reader registered for mailbox: " + _id);
    }
    _reader = reader;
  }

  public String getId() {
    return _id;
  }

  /**
   * Offers a raw block into the mailbox within the timeout specified, returns whether the block is successfully added.
   * If the block is not added, an error block is added to the mailbox.
   * <p>
   * Contrary to {@link #offer(MseBlock, List, long)}, the block may be an error block.
   */
  public ReceivingMailboxStatus offerRaw(List<ByteBuffer> byteBuffers, long timeoutMs)
      throws IOException {
    updateWaitCpuTime();

    long startTimeMs = System.currentTimeMillis();
    int totalBytes = 0;
    for (ByteBuffer bb : byteBuffers) {
      totalBytes += bb.remaining();
    }
    DataBlock dataBlock = DataBlockUtils.deserialize(byteBuffers);
    _stats.merge(StatKey.DESERIALIZED_MESSAGES, 1);
    _stats.merge(StatKey.DESERIALIZED_BYTES, totalBytes);
    _stats.merge(StatKey.DESERIALIZATION_TIME_MS, System.currentTimeMillis() - startTimeMs);

    MseBlock block;
    if (dataBlock instanceof MetadataBlock) {
      Map<Integer, String> exceptions = dataBlock.getExceptions();
      if (exceptions.isEmpty()) {
        block = SuccessMseBlock.INSTANCE;
      } else {
        MetadataBlock metadataBlock = (MetadataBlock) dataBlock;
        Map<QueryErrorCode, String> exceptionsByQueryError = QueryErrorCode.fromKeyMap(exceptions);
        ErrorMseBlock errorBlock =
            new ErrorMseBlock(metadataBlock.getStageId(), metadataBlock.getWorkerId(), metadataBlock.getServerId(),
                exceptionsByQueryError);
        setErrorBlock(errorBlock, dataBlock.getStatsByStage());
        return ReceivingMailboxStatus.FIRST_ERROR;
      }
    } else {
      block = new SerializedDataBlock(dataBlock);
    }
    return offerPrivate(block, dataBlock.getStatsByStage(), timeoutMs);
  }

  public ReceivingMailboxStatus offer(MseBlock block, List<DataBuffer> serializedStats, long timeoutMs) {
    updateWaitCpuTime();
    _stats.merge(StatKey.IN_MEMORY_MESSAGES, 1);
    if (block instanceof ErrorMseBlock) {
      setErrorBlock((ErrorMseBlock) block, serializedStats);
      return ReceivingMailboxStatus.EARLY_TERMINATED;
    }
    return offerPrivate(block, serializedStats, timeoutMs);
  }

  /**
   * Offers a non-error block into the mailbox within the timeout specified, returns whether the block is successfully
   * added. If the block is not added, an error block is added to the mailbox.
   */
  private ReceivingMailboxStatus offerPrivate(MseBlock block, List<DataBuffer> stats, long timeoutMs) {
    if (timeoutMs <= 0) {
      LOGGER.debug("Mailbox: {} is already timed out", _id);
      setErrorBlock(
          ErrorMseBlock.fromException(new TimeoutException("Timed out while offering data to mailbox: " + _id)),
          stats);
      return ReceivingMailboxStatus.TIMEOUT;
    }
    try {
      long now = System.currentTimeMillis();
      ReceivingMailboxStatus status = _blocks.offer(block, stats, timeoutMs, TimeUnit.MILLISECONDS);
      switch (status) {
        case SUCCESS: {
          _stats.merge(StatKey.OFFER_CPU_TIME_MS, System.currentTimeMillis() - now);
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("==[MAILBOX]== Block " + block + " ready to read from mailbox: " + _id);
          }
          break;
        }
        case TIMEOUT: {
          LOGGER.debug("Failed to offer block into mailbox: {} within: {}ms", _id, timeoutMs);
          break;
        }
        case CANCELLED: {
          LOGGER.debug("Mailbox: {} is already cancelled, ignoring the late block", _id);
          break;
        }
        case ERROR: {
          LOGGER.debug("Mailbox: {} is already errored out, ignoring the late block", _id);
          break;
        }
        default:
          // Nothing todo
      }
      return status;
    } catch (InterruptedException e) {
      LOGGER.error("Interrupted while offering block into mailbox: {}", _id);
      setErrorBlock(ErrorMseBlock.fromException(e), stats);
      return ReceivingMailboxStatus.ERROR;
    }
  }

  private synchronized void updateWaitCpuTime() {
    long now = System.currentTimeMillis();
    _stats.merge(StatKey.WAIT_CPU_TIME_MS, now - _lastArriveTime);
    _lastArriveTime = now;
  }

  /**
   * Sets an error block into the mailbox. No more blocks are accepted after calling this method.
   */
  public void setErrorBlock(ErrorMseBlock errorBlock, List<DataBuffer> serializedStats) {
    _blocks.setError(errorBlock, serializedStats);
  }

  /**
   * Returns the first block from the mailbox, or {@code null} if there is no block received yet. Error block is
   * returned if exists.
   */
  @Nullable
  public MseBlockWithStats poll() {
    Preconditions.checkState(_reader != null, "A reader must be registered");
    return _blocks.poll();
  }

  /**
   * Early terminate the mailbox, called when upstream doesn't expect any more data block.
   */
  public void earlyTerminate() {
    _blocks.earlyTerminate();
  }

  /**
   * Cancels the mailbox. No more blocks are accepted after calling this method. Should only be called by the receive
   * operator to clean up the remaining blocks.
   */
  public void cancel() {
    LOGGER.debug("Cancelling mailbox: {}", _id);
    _blocks.setError(ErrorMseBlock.fromException(null), List.of());
  }

  public int getNumPendingBlocks() {
    return _blocks.exactSize();
  }

  public StatMap<StatKey> getStatMap() {
    return _stats;
  }

  public interface Reader {
    void blockReadyToRead();
  }

  public enum ReceivingMailboxStatus {
    SUCCESS, FIRST_ERROR, ERROR, TIMEOUT, CANCELLED, EARLY_TERMINATED
  }

  public enum StatKey implements StatMap.Key {
    DESERIALIZED_MESSAGES(StatMap.Type.INT),
    DESERIALIZED_BYTES(StatMap.Type.LONG),
    DESERIALIZATION_TIME_MS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    IN_MEMORY_MESSAGES(StatMap.Type.INT),
    OFFER_CPU_TIME_MS(StatMap.Type.LONG),
    WAIT_CPU_TIME_MS(StatMap.Type.LONG),
    ALLOCATED_MEMORY_BYTES(StatMap.Type.LONG),
    GC_TIME_MS(StatMap.Type.LONG);

    private final StatMap.Type _type;

    StatKey(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }

  public static class MseBlockWithStats {
    private final MseBlock _block;
    private final List<DataBuffer> _serializedStats;

    public MseBlockWithStats(MseBlock block, List<DataBuffer> serializedStats) {
      _block = block;
      _serializedStats = serializedStats;
    }

    public MseBlock getBlock() {
      return _block;
    }

    public List<DataBuffer> getSerializedStats() {
      return _serializedStats;
    }
  }

  /// This is a bounded blocking queue implementation similar to ArrayBlockingQueue, but also supports the ability to
  /// cancel the queue and add an error block to it.
  ///
  /// All methods of this class are thread-safe and may block, although only [#offer] should block for a long time.
  @ThreadSafe
  private class CancellableBlockingQueue {
    /// The items in the queue.
    ///
    /// This is a circular array where [#_putIndex] is the index to add the next item and [#_takeIndex] is the index to
    /// take the next item from.
    ///
    /// Like in normal blocking queues, elements are added when upstream threads call [#offer] and removed when the
    /// downstream thread calls [#poll]. Unlike normal blocking queues, elements can also be removed when there is an
    /// error notified with [#setError]. In that case, all elements in the queue are removed and the error block is
    /// stored as [#_errorBlock].
    ///
    /// It is important to notice that when [#earlyTerminate()] is called items on the buffer are not removed and will
    /// be returned by [#poll()] until the buffer is empty. After that, [#poll()] will return `null`.
    private final MseBlockWithStats[] _items;
    private int _takeIndex;
    private int _putIndex;
    private int _count;
    private final ReentrantLock _lock = new ReentrantLock();
    private final Condition _notFull = _lock.newCondition();
    /// This block is set when a upstream finds an error and is used by [#poll()] to return the error block to the
    /// downstream.
    ///
    /// Once is set, no more blocks are accepted and [#_items] is cleared.
    ///
    /// Although this is similar to [#_writerFinalStatus], the latter can be also set by the downstream (to indicate
    /// early termination), in which case no error block is set.
    /// Therefore [#_errorBlock] can be null even when [#_writerFinalStatus] is set, while the opposite is not true.
    private MseBlockWithStats _errorBlock;
    /// This is set when the downstream calls earlyTerminate() to indicate that no more data blocks will be sent or when
    /// a upstream finds an error.
    ///
    /// It is only read by upstreams to know whether they should accept more blocks or not.
    ///
    /// Once this is set, no more blocks are accepted.
    ///
    /// Although this is similar to [#_errorBlock], the latter is only set by a upstream (to indicate an error).
    /// Therefore [#_errorBlock] can be null even when [#_writerFinalStatus] is set, while the opposite is not true.
    private ReceivingMailboxStatus _writerFinalStatus;

    public CancellableBlockingQueue(int capacity) {
      _items = new MseBlockWithStats[capacity];
    }

    /// Offers a block into the queue within the timeout specified, returning the error code if the block is not added
    /// or null if the block is added successfully.
    public ReceivingMailboxStatus offer(MseBlock block, List<DataBuffer> stats, long timeout, TimeUnit timeUnit)
        throws InterruptedException {
      if (_writerFinalStatus != null) {
        LOGGER.debug("Mailbox: {} is already cancelled or errored out, ignoring the late block", _id);
        return _writerFinalStatus;
      }

      MseBlockWithStats blockWithStats = new MseBlockWithStats(block, stats);

      long nanos = timeUnit.toNanos(timeout);
      final ReentrantLock lock = _lock;
      lock.lockInterruptibly();
      try {
        MseBlockWithStats[] items = _items;
        while (_writerFinalStatus == null && _count == items.length) {
          if (nanos <= 0L) {
            ErrorMseBlock timeoutBlock = ErrorMseBlock.fromError(QueryErrorCode.EXECUTION_TIMEOUT,
                "Timed out while waiting for receive operator to consume data from mailbox: " + _id);
            setError(timeoutBlock, stats);
            _writerFinalStatus = ReceivingMailboxStatus.TIMEOUT;
            return ReceivingMailboxStatus.TIMEOUT;
          }
          nanos = _notFull.awaitNanos(nanos);
        }
        if (_writerFinalStatus != null) {
          LOGGER.debug("Mailbox: {} is already cancelled or errored out, ignoring the late block", _id);
          return _writerFinalStatus;
        }
        items[_putIndex] = blockWithStats;
        if (++_putIndex == items.length) {
          _putIndex = 0;
        }
        _count++;
      } finally {
        lock.unlock();
      }
      notifyReader();
      return ReceivingMailboxStatus.SUCCESS;
    }

    /// Notifies the downstream that there is data to read. This method doesn't acquire any locks and must be called
    /// after releasing the lock to avoid deadlocks in case the downstream calls back into this class on another thread.
    private void notifyReader() {
      Reader reader = _reader;
      if (reader != null) {
        LOGGER.debug("Notifying reader");
        reader.blockReadyToRead();
      } else {
        LOGGER.debug("No reader to notify");
      }
    }

    /// Sets an error block into the queue. No more blocks are accepted after calling this method and any call to
    /// [#poll] will return the error block.
    ///
    /// This method may call briefly block while acquiring the lock, but it doesn't actually require waiting for the
    /// queue to have space.
    public void setError(ErrorMseBlock errorBlock, List<DataBuffer> serializedStats) {
      if (_writerFinalStatus == null) {
        return;
      }
      ReentrantLock lock = _lock;
      lock.lock();
      try {
        if (_writerFinalStatus != null) {
          return;
        }
        _errorBlock = new MseBlockWithStats(errorBlock, serializedStats);
        _writerFinalStatus = ReceivingMailboxStatus.ERROR;
        // To help the GC
        Arrays.fill(_items, null);
        _notFull.signalAll();
      } finally {
        lock.unlock();
      }
      notifyReader();
    }

    /// Returns the first block from the queue, or `null` if there is no block in the queue. The returned block will be
    /// an error block if the queue has been cancelled or has encountered an error.
    ///
    /// Notice that after calling [#earlyTerminate()], this method will always return `null`, given the queue is empty
    /// and no more blocks will be added.
    ///
    /// This method may block briefly while acquiring the lock, but it doesn't actually require waiting for data in the
    /// queue, as it returns `null` if the queue is empty.
    @Nullable
    public MseBlockWithStats poll() {
      if (_errorBlock != null) {
        return _errorBlock;
      }
      ReentrantLock lock = _lock;
      lock.lock();
      try {
        if (_errorBlock != null) {
          return _errorBlock;
        }
        if (_count == 0) {
          return null;
        }
        MseBlockWithStats[] items = _items;
        MseBlockWithStats block = items[_takeIndex];
        items[_takeIndex] = null;
        if (++_takeIndex == items.length) {
          _takeIndex = 0;
        }
        _count--;
        _notFull.signal();

        return block;
      } finally {
        lock.unlock();
      }
    }

    public int exactSize() {
      ReentrantLock lock = _lock;
      lock.lock();
      try {
        return _count;
      } finally {
        lock.unlock();
      }
    }

    /// Called by the downstream to indicate that no more data blocks will be read.
    ///
    /// This means no more calls to [#poll()] are expected and also all pending or future calls to [#offer()] will
    /// return [ReceivingMailboxStatus#EARLY_TERMINATED].
    public void earlyTerminate() {
      if (_writerFinalStatus != null || _errorBlock != null) {
        return;
      }
      ReentrantLock lock = _lock;
      lock.lock();
      try {
        if (_writerFinalStatus != null || _errorBlock != null) {
          return;
        }
        _writerFinalStatus = ReceivingMailboxStatus.EARLY_TERMINATED;
        // To help the GC
        Arrays.fill(_items, null);
        _notFull.signalAll();
      } finally {
        lock.unlock();
      }
    }
  }
}
