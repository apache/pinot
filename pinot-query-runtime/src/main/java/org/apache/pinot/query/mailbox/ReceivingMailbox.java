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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SerializedDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.SimpleQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Mailbox that's used to receive data. Ownership of the ReceivingMailbox is with the MailboxService, which is unlike
 * the {@link SendingMailbox} whose ownership lies with the send operator. This is because the ReceivingMailbox can be
 * initialized even before the corresponding OpChain is registered on the receiver, whereas the SendingMailbox is
 * initialized when the send operator is running.
 *
 * There is a single ReceivingMailbox for each {@link org.apache.pinot.query.runtime.operator.MailboxReceiveOperator}.
 * The offer methods will be called when new blocks are received from different sources. For example local workers will
 * directly call {@link #offer(MseBlock, List, long)} while each remote worker opens a GPRC channel where messages
 * are sent in raw format and {@link #offerRaw(ByteBuffer, long)} is called from them.
 */
@ThreadSafe
public class ReceivingMailbox {
  public static final int DEFAULT_MAX_PENDING_BLOCKS = 5;

  private static final Logger LOGGER = LoggerFactory.getLogger(ReceivingMailbox.class);
  private static final MseBlockWithStats CANCELLED_ERROR_BLOCK = new MseBlockWithStats(
      ErrorMseBlock.fromException(new RuntimeException("Cancelled by receiver")), Collections.emptyList());

  private final String _id;
  // TODO: Make the queue size configurable
  // TODO: Revisit if this is the correct way to apply back pressure
  /// The queue where blocks are going to be stored.
  private final BlockingQueue<MseBlockWithStats> _blocks = new ArrayBlockingQueue<>(DEFAULT_MAX_PENDING_BLOCKS);
  private final AtomicReference<MseBlockWithStats> _errorBlock = new AtomicReference<>();
  private volatile boolean _isEarlyTerminated = false;
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
  public ReceivingMailboxStatus offerRaw(ByteBuffer byteBuffer, long timeoutMs)
      throws IOException {
    MseBlock block;
    updateWaitCpuTime();
    _stats.merge(StatKey.DESERIALIZED_BYTES, byteBuffer.remaining());
    _stats.merge(StatKey.DESERIALIZED_MESSAGES, 1);

    long now = System.currentTimeMillis();
    DataBlock dataBlock = DataBlockUtils.readFrom(byteBuffer);
    _stats.merge(StatKey.DESERIALIZATION_TIME_MS, System.currentTimeMillis() - now);

    if (dataBlock instanceof MetadataBlock) {
      Map<Integer, String> exceptions = dataBlock.getExceptions();
      if (exceptions.isEmpty()) {
        block = SuccessMseBlock.INSTANCE;
      } else {
        Map<QueryErrorCode, String> exceptionsByQueryError = QueryErrorCode.fromKeyMap(exceptions);
        setErrorBlock(new ErrorMseBlock(exceptionsByQueryError), dataBlock.getStatsByStage());
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
    MseBlockWithStats errorBlock = _errorBlock.get();
    if (errorBlock != null) {
      LOGGER.debug("Mailbox: {} is already cancelled or errored out, ignoring the late block", _id);
      return errorBlock == CANCELLED_ERROR_BLOCK ? ReceivingMailboxStatus.CANCELLED : ReceivingMailboxStatus.ERROR;
    }
    if (timeoutMs <= 0) {
      LOGGER.debug("Mailbox: {} is already timed out", _id);
      setErrorBlock(
          ErrorMseBlock.fromException(new SimpleQueryException("Timed out while offering data to mailbox: " + _id)),
          stats);
      return ReceivingMailboxStatus.TIMEOUT;
    }
    try {
      long now = System.currentTimeMillis();
      MseBlockWithStats blockWithStats = new MseBlockWithStats(block, stats);
      boolean accepted = _blocks.offer(blockWithStats, timeoutMs, TimeUnit.MILLISECONDS);
      _stats.merge(StatKey.OFFER_CPU_TIME_MS, System.currentTimeMillis() - now);
      if (accepted) {
        errorBlock = _errorBlock.get();
        if (errorBlock == null) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("==[MAILBOX]== Block " + block + " ready to read from mailbox: " + _id);
          }
          notifyReader();
          return _isEarlyTerminated ? ReceivingMailboxStatus.EARLY_TERMINATED : ReceivingMailboxStatus.SUCCESS;
        } else {
          LOGGER.debug("Mailbox: {} is already cancelled or errored out, ignoring the late block", _id);
          _blocks.clear();
          return errorBlock == CANCELLED_ERROR_BLOCK ? ReceivingMailboxStatus.CANCELLED : ReceivingMailboxStatus.ERROR;
        }
      } else {
        LOGGER.debug("Failed to offer block into mailbox: {} within: {}ms", _id, timeoutMs);
        SimpleQueryException exception = new SimpleQueryException(
            "Timed out while waiting for receive operator to consume data from mailbox: " + _id);
        setErrorBlock(ErrorMseBlock.fromException(exception), stats);
        return ReceivingMailboxStatus.TIMEOUT;
      }
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
    if (_errorBlock.compareAndSet(null, new MseBlockWithStats(errorBlock, serializedStats))) {
      _blocks.clear();
      notifyReader();
    }
  }

  /**
   * Returns the first block from the mailbox, or {@code null} if there is no block received yet. Error block is
   * returned if exists.
   */
  @Nullable
  public MseBlockWithStats poll() {
    Preconditions.checkState(_reader != null, "A reader must be registered");
    MseBlockWithStats errorBlock = _errorBlock.get();
    return errorBlock != null ? errorBlock : _blocks.poll();
  }

  /**
   * Early terminate the mailbox, called when upstream doesn't expect any more data block.
   */
  public void earlyTerminate() {
    _isEarlyTerminated = true;
  }

  /**
   * Cancels the mailbox. No more blocks are accepted after calling this method. Should only be called by the receive
   * operator to clean up the remaining blocks.
   */
  public void cancel() {
    LOGGER.debug("Cancelling mailbox: {}", _id);
    if (_errorBlock.compareAndSet(null, CANCELLED_ERROR_BLOCK)) {
      _blocks.clear();
    }
  }

  public int getNumPendingBlocks() {
    return _blocks.size();
  }

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
    WAIT_CPU_TIME_MS(StatMap.Type.LONG);

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
}
