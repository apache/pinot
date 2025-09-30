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
import javax.annotation.concurrent.GuardedBy;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SerializedDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.utils.BlockingMultiStreamConsumer;
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
 * There is a single ReceivingMailbox for each pair of (sender, receiver) opchains. This means that each receive
 * operator will have multiple ReceivingMailbox instances, one for each sender. They are coordinated by a
 * {@link BlockingMultiStreamConsumer}.
 *
 * A ReceivingMailbox can have at most one reader and one writer at any given time. This means that different threads
 * writing to the same mailbox must be externally synchronized.
 *
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
  // TODO: Apply backpressure at the sender side when the queue is full.
  /// The queue where blocks are going to be stored.
  private final CancellableBlockingQueue _blocks;
  private long _lastArriveTime = System.currentTimeMillis();

  @Nullable
  private volatile Reader _reader;
  private final StatMap<StatKey> _stats = new StatMap<>(StatKey.class);

  public ReceivingMailbox(String id, int maxPendingBlocks) {
    _id = id;
    _blocks = new CancellableBlockingQueue(maxPendingBlocks);
  }

  public ReceivingMailbox(String id) {
    this(id, DEFAULT_MAX_PENDING_BLOCKS);
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
   * <p>
   * Contrary to {@link #offer(MseBlock, List, long)}, the block may be an error block.
   */
  public ReceivingMailboxStatus offerRaw(List<ByteBuffer> byteBuffers, long timeoutMs)
      throws IOException, InterruptedException, TimeoutException {
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
        block = new ErrorMseBlock(metadataBlock.getStageId(), metadataBlock.getWorkerId(), metadataBlock.getServerId(),
                exceptionsByQueryError);
      }
    } else {
      block = new SerializedDataBlock(dataBlock);
    }
    return offerPrivate(block, dataBlock.getStatsByStage(), timeoutMs);
  }

  /**
   * Offers a non-error block into the mailbox within the timeout specified, returns whether the block is successfully
   * added.
   */
  public ReceivingMailboxStatus offer(MseBlock block, List<DataBuffer> serializedStats, long timeoutMs)
      throws InterruptedException, TimeoutException {
    updateWaitCpuTime();
    _stats.merge(StatKey.IN_MEMORY_MESSAGES, 1);
    return offerPrivate(block, serializedStats, timeoutMs);
  }

  /**
   * Offers a non-error block into the mailbox within the timeout specified, returns whether the block is successfully
   * added.
   */
  private ReceivingMailboxStatus offerPrivate(MseBlock block, List<DataBuffer> stats, long timeoutMs)
      throws InterruptedException, TimeoutException {
    long start = System.currentTimeMillis();
    try {
      ReceivingMailboxStatus result;
      if (block.isEos()) {
        result = _blocks.offerEos((MseBlock.Eos) block, stats);
      } else {
        result = _blocks.offerData((MseBlock.Data) block, timeoutMs, TimeUnit.MILLISECONDS);
      }

      switch (result) {
        case SUCCESS:
        case LAST_BLOCK:
          notifyReader();
          _stats.merge(StatKey.OFFER_CPU_TIME_MS, System.currentTimeMillis() - start);
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("==[MAILBOX]== Block " + block + " ready to read from mailbox: " + _id);
          }
          break;
        case WAITING_EOS:
        case ALREADY_TERMINATED:
        default:
          // Nothing to do
      }
      return result;
    } catch (TimeoutException e) {
      _stats.merge(StatKey.OFFER_CPU_TIME_MS, System.currentTimeMillis() - start);
      throw e;
    } catch (InterruptedException e) {
      String errorMessage = "Interrupted on mailbox " + _id + " while offering blocks";
      setErrorBlock(ErrorMseBlock.fromError(QueryErrorCode.INTERNAL, errorMessage), stats);
      throw e;
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
    _blocks.offerEos(errorBlock, serializedStats);
  }

  /**
   * Returns the first block from the mailbox, or {@code null} if there is no block received yet.
   */
  @Nullable
  public MseBlockWithStats poll() {
    Preconditions.checkState(_reader != null, "A reader must be registered");
    return _blocks.poll();
  }

  /**
   * Early terminate the mailbox, called when upstream doesn't expect any more <em>data</em> block.
   */
  public void earlyTerminate() {
    _blocks.earlyTerminate();
  }

  /**
   * Cancels the mailbox. No more blocks are accepted after calling this method and {@link #poll()} will always return
   * an error block.
   */
  public void cancel() {
    LOGGER.debug("Cancelling mailbox: {}", _id);
    _blocks.offerEos(ErrorMseBlock.fromException(null), List.of());
  }

  public int getNumPendingBlocks() {
    return _blocks.exactSize();
  }

  /// Notifies the downstream that there is data to read.
  private void notifyReader() {
    Reader reader = _reader;
    if (reader != null) {
      LOGGER.debug("Notifying reader");
      reader.blockReadyToRead();
    } else {
      LOGGER.debug("No reader to notify");
    }
  }

  public StatMap<StatKey> getStatMap() {
    return _stats;
  }

  public interface Reader {
    void blockReadyToRead();
  }

  public enum ReceivingMailboxStatus {
    /// The block was successfully added to the mailbox.
    ///
    /// More blocks can be sent.
    SUCCESS,
    /// The block is rejected because downstream has early terminated and now is only waiting for EOS in order to
    /// get the stats.
    WAITING_EOS,
    /// The received message is the last block the mailbox will ever read.
    ///
    /// This happens for example when an EOS block is added to the mailbox.
    ///
    /// No more blocks can be sent.
    LAST_BLOCK,
    /// The mailbox has been closed for write. Only EOS blocks are expected.
    ///
    /// No more blocks can be sent.
    ALREADY_TERMINATED
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

  /// This is a special bounded blocking queue implementation similar to ArrayBlockingQueue, but:
  /// - Only accepts a single reader (aka downstream).
  /// - Only accepts a multiple concurrent writers (aka upstream)
  /// - Can be [closed for write][#closeForWrite(MseBlock.Eos, List)].
  /// - Can be [#earlyTerminate()]d.
  ///
  /// All methods of this class are thread-safe and may block, although only [#offer] should block for a long time.
  @ThreadSafe
  private class CancellableBlockingQueue {
    /// This is set when the queue is in [State#FULL_CLOSED] or [State#UPSTREAM_FINISHED].
    @Nullable
    @GuardedBy("_lock")
    private MseBlockWithStats _eos;
    /// The current state of the queue.
    ///
    /// All changes to this field must be done by calling [#changeState(State, String)].
    @GuardedBy("_lock")
    private State _state = State.FULL_OPEN;
    /// The items in the queue.
    ///
    /// This is a circular array where [#_putIndex] is the index to add the next item and [#_takeIndex] is the index to
    /// take the next item from.
    ///
    /// Like in normal blocking queues, elements are added when upstream threads call [#offer] and removed when the
    /// downstream thread calls [#poll]. Unlike normal blocking queues, elements will be [removed][#drainDataBlocks()]
    /// when transitioning to [State#WAITING_EOS] or [State#FULL_CLOSED].
    @GuardedBy("_lock")
    private final MseBlock.Data[] _dataBlocks;
    @GuardedBy("_lock")
    private int _takeIndex;
    @GuardedBy("_lock")
    private int _putIndex;
    @GuardedBy("_lock")
    private int _count;
    /// Threads waiting to add more data to the queue.
    ///
    /// This is used to prevent the following situation:
    /// 1. The queue is full.
    /// 2. Thread A tries to add data. Thread A will be blocked waiting for space in the queue.
    /// 3. Thread B adds an EOS block, which will transition the queue to [State#UPSTREAM_FINISHED].
    /// 4. Thread C reads data from the queue in a loop, the scheduler doesn't give time to Thread A.
    /// 5. Thread C consumes all data from the queue and then reads the EOS block.
    /// 6. Finally Thread A is unblocked and adds data to the queue, even though the queue is already closed for write
    ///
    /// As a result the block from A will be lost. Instead, we use this counter to return null in [#poll] when the
    /// queue is empty but there are still threads trying to add data to the queue.
    @GuardedBy("_lock")
    private int _pendingData;
    private final ReentrantLock _fullLock = new ReentrantLock();
    private final Condition _notFull = _fullLock.newCondition();

    public CancellableBlockingQueue(int capacity) {
      _dataBlocks = new MseBlock.Data[capacity];
    }

    /// Offers a successful or erroneous EOS block into the queue, returning the status of the operation.
    ///
    /// This method never blocks for long, as it doesn't need to wait for space in the queue.
    public ReceivingMailboxStatus offerEos(MseBlock.Eos block, List<DataBuffer> stats) {
      ReentrantLock lock = _fullLock;
      lock.lock();
      try {
        switch (_state) {
          case FULL_CLOSED:
          case UPSTREAM_FINISHED:
            // The queue is closed for write. Always reject the block.
            LOGGER.debug("Mailbox: {} is already closed for write, ignoring the late EOS block", _id);
            return ReceivingMailboxStatus.ALREADY_TERMINATED;
          case WAITING_EOS:
            // We got the EOS block we expected. Close the queue for both read and write.
            changeState(State.FULL_CLOSED, "received EOS block");
            _eos = new MseBlockWithStats(block, stats);
            return ReceivingMailboxStatus.LAST_BLOCK;
          case FULL_OPEN:
            changeState(State.UPSTREAM_FINISHED, "received EOS block");
            _eos = new MseBlockWithStats(block, stats);
            if (block.isError()) {
              drainDataBlocks();
              _notFull.signal();
            }
            return ReceivingMailboxStatus.LAST_BLOCK;
          default:
            throw new IllegalStateException("Unexpected state: " + _state);
        }
      } finally {
        lock.unlock();
      }
    }

    /// Offers a data block into the queue within the timeout specified, returning the status of the operation.
    public ReceivingMailboxStatus offerData(MseBlock.Data block, long timeout, TimeUnit timeUnit)
        throws InterruptedException, TimeoutException {
      ReentrantLock lock = _fullLock;
      lock.lockInterruptibly();
      try {
        while (true) {
          switch (_state) {
            case FULL_CLOSED:
            case UPSTREAM_FINISHED:
              // The queue is closed for write. Always reject the block.
              LOGGER.debug("Mailbox: {} is already closed for write, ignoring the late data block", _id);
              return ReceivingMailboxStatus.ALREADY_TERMINATED;
            case WAITING_EOS:
              // The downstream is not interested in reading more data.
              LOGGER.debug("Mailbox: {} is not interesting in late data block", _id);
              return ReceivingMailboxStatus.WAITING_EOS;
            case FULL_OPEN:
              if (offerDataToBuffer(block, timeout, timeUnit)) {
                return ReceivingMailboxStatus.SUCCESS;
              }
              // otherwise iterate again
              break;
            default:
              throw new IllegalStateException("Unexpected state: " + _state);
          }
        }
      } finally {
        lock.unlock();
      }
    }

    /// Offers a data block into the queue within the timeout specified, returning true if the block was added
    /// successfully.
    ///
    /// This method can only be called while the queue is in the FULL_OPEN state and the lock is held.
    ///
    /// This method can time out, in which case we automatically transition to the [State#FULL_CLOSED] state.
    /// But instead of returning false, we throw a [TimeoutException]. This is because the caller may want to
    /// distinguish between a timeout and other reasons for not being able to add the block to the queue in order to
    /// report different error messages.
    ///
    /// @return true if the block was added successfully, false if the state changed while waiting.
    /// @throws InterruptedException if the thread is interrupted while waiting for space in the queue.
    /// @throws TimeoutException if the timeout specified elapsed before space was available in the queue.
    @GuardedBy("_lock")
    private synchronized boolean offerDataToBuffer(MseBlock.Data block, long timeout, TimeUnit timeUnit)
        throws InterruptedException, TimeoutException {

      assert _state == State.FULL_OPEN;

      long nanos = timeUnit.toNanos(timeout);
      MseBlock.Data[] items = _dataBlocks;
      _pendingData++;
      try {
        while (_count == items.length && nanos > 0L) {
          nanos = _notFull.awaitNanos(nanos);

          switch (_state) {
            case FULL_OPEN: // we are in the same state, continue waiting for space
              break;
            case FULL_CLOSED:
            case WAITING_EOS:
              // The queue is closed and the reader is not interested in reading more data.
              return false;
            case UPSTREAM_FINISHED:
              // Another thread offered the EOS while we were waiting for space.
              assert _eos != null;
              if (_eos._block.isSuccess()) { // If closed with EOS, the reader is still interested in reading our block
                continue;
              }
              // if closed with an error, the reader is not interested in reading our block
              return false;
            default:
              throw new IllegalStateException("Unexpected state: " + _state);
          }
        }
        if (nanos <= 0L) { // timed out
          ErrorMseBlock timeoutBlock = ErrorMseBlock.fromError(QueryErrorCode.EXECUTION_TIMEOUT,
              "Timed out while waiting for receive operator to consume data from mailbox: " + _id);
          changeState(State.FULL_CLOSED, "timed out while waiting to offer data block");
          drainDataBlocks();
          _eos = new MseBlockWithStats(timeoutBlock, List.of());
          throw new TimeoutException();
        }
        items[_putIndex] = block;
        if (++_putIndex == items.length) {
          _putIndex = 0;
        }
        _count++;
        return true;
      } finally {
        _pendingData--;
      }
    }

    /// Returns the first block from the queue, or `null` if there is no block in the queue. The returned block will be
    /// an error block if the queue has been cancelled or has encountered an error.
    ///
    /// This method may block briefly while acquiring the lock, but it doesn't actually require waiting for data in the
    /// queue.
    @Nullable
    public MseBlockWithStats poll() {
      ReentrantLock lock = _fullLock;
      if (!lock.tryLock()) {
        return null;
      }
      try {
        switch (_state) {
          case FULL_CLOSED:
            // The queue is closed for both read and write. Always return the error block.
            assert _eos != null;
            return _eos;
          case WAITING_EOS:
            // The downstream is not interested in reading more data but is waiting for an EOS block to get the stats.
            // Polls returns null and only EOS blocks are accepted by offer.
            assert _eos == null;
            return null;
          case UPSTREAM_FINISHED:
            // The upstream has indicated that no more data will be sent. Poll returns pending blocks and then the EOS
            // block.
            if (_count == 0) {
              if (_pendingData > 0) {
                // There are still threads trying to add data to the queue. We should wait for them to finish.
                return null;
              } else {
                changeState(State.FULL_CLOSED, "read all data blocks");
                return _eos;
              }
            }
            break;
          case FULL_OPEN:
            if (_count == 0) {
              assert _eos == null;
              return null;
            }
            break;
          default:
            throw new IllegalStateException("Unexpected state: " + _state);
        }
        assert _count > 0 : "if we reach here, there must be data in the queue";
        MseBlock.Data[] items = _dataBlocks;
        MseBlock.Data block = items[_takeIndex];
        items[_takeIndex] = null;
        if (++_takeIndex == items.length) {
          _takeIndex = 0;
        }
        _count--;
        _notFull.signal();

        return new MseBlockWithStats(block, List.of());
      } finally {
        lock.unlock();
      }
    }

    @GuardedBy("_lock")
    private void changeState(State newState, String desc) {
      LOGGER.debug("Mailbox: {} {}, transitioning from {} to {}", _id, desc, _state, newState);
      _state = newState;
    }

    @GuardedBy("_lock")
    private void drainDataBlocks() {
      Arrays.fill(_dataBlocks, null);
      _count = 0;
    }

    public int exactSize() {
      ReentrantLock lock = _fullLock;
      lock.lock();
      try {
        return _count;
      } finally {
        lock.unlock();
      }
    }

    /// Called by the downstream to indicate that no more data blocks will be read.
    public void earlyTerminate() {
      ReentrantLock lock = _fullLock;
      lock.lock();
      try {
        switch (_state) {
          case FULL_CLOSED:
          case WAITING_EOS:
            LOGGER.debug("Mailbox: {} is already closed for read", _id);
            return;
          case UPSTREAM_FINISHED:
            drainDataBlocks();
            changeState(State.FULL_CLOSED, "early terminated");
            break;
          case FULL_OPEN:
            drainDataBlocks();
            changeState(State.WAITING_EOS, "early terminated");
            break;
          default:
            throw new IllegalStateException("Unexpected state: " + _state);
        }
      } finally {
        lock.unlock();
      }
    }
  }

  /// The state of the queue.
  ///
  /// ```
  /// +-------------------+   offerEos    +-------------------+
  /// |    FULL_OPEN      | ----------->  |  UPSTREAM_FINISHED|
  /// +-------------------+               +-------------------+
  ///       |                                 |
  ///       | earlyTerminate                  | poll -- when all pending data is read
  ///       v                                 v
  /// +-------------------+   offerEos   +-------------------+
  /// |   WAITING_EOS     | -----------> |   FULL_CLOSED     |
  /// +-------------------+              +-------------------+
  /// ```
  private enum State {
    /// The queue is open for both read and write.
    FULL_OPEN,
    /// The downstream is not interested in reading more data but is waiting for an EOS block to get the stats.
    ///
    /// Polls return null and only EOS blocks are accepted by offer.
    WAITING_EOS,
    /// The upstream has indicated that no more data will be sent.
    ///
    /// Offer fails while poll returns pending blocks and then the EOS block.
    UPSTREAM_FINISHED,
    /// The queue is closed for both read and write.
    ///
    /// Offers are rejected and polls return the EOS block, which is always not null.
    FULL_CLOSED
  }
}
