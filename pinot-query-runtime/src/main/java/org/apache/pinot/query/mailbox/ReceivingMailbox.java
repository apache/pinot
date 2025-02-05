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
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.QException;
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
 * directly call {@link #offer(TransferableBlock, long)} while each remote worker opens a GPRC channel where messages
 * are sent in raw format and {@link #offerRaw(ByteBuffer, long)} is called from them.
 */
@ThreadSafe
public class ReceivingMailbox {
  public static final int DEFAULT_MAX_PENDING_BLOCKS = 5;

  private static final Logger LOGGER = LoggerFactory.getLogger(ReceivingMailbox.class);
  private static final TransferableBlock CANCELLED_ERROR_BLOCK =
      TransferableBlockUtils.getErrorTransferableBlock(QException.INTERNAL_ERROR_CODE, "Cancelled by receiver");

  private final String _id;
  // TODO: Make the queue size configurable
  // TODO: Revisit if this is the correct way to apply back pressure
  private final BlockingQueue<TransferableBlock> _blocks = new ArrayBlockingQueue<>(DEFAULT_MAX_PENDING_BLOCKS);
  private final AtomicReference<TransferableBlock> _errorBlock = new AtomicReference<>();
  private volatile boolean _isEarlyTerminated = false;
  private long _lastArriveTime = System.currentTimeMillis();

  @Nullable
  private volatile Reader _reader;
  private final StatMap<StatKey> _stats = new StatMap<>(StatKey.class);
  private volatile OpChainExecutionContext _context;

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
   * Contrary to {@link #offer(TransferableBlock, long)}, the block may be an
   * {@link TransferableBlock#isErrorBlock() error block}.
   */
  public ReceivingMailboxStatus offerRaw(ByteBuffer byteBuffer, long timeoutMs)
      throws IOException {
    TransferableBlock block;
    long now = System.currentTimeMillis();
    _stats.merge(StatKey.WAIT_CPU_TIME_MS, now - _lastArriveTime);
    _lastArriveTime = now;
    _stats.merge(StatKey.DESERIALIZED_BYTES, byteBuffer.remaining());
    _stats.merge(StatKey.DESERIALIZED_MESSAGES, 1);

    now = System.currentTimeMillis();
    DataBlock dataBlock = DataBlockUtils.readFrom(byteBuffer);
    _stats.merge(StatKey.DESERIALIZATION_TIME_MS, System.currentTimeMillis() - now);

    if (dataBlock instanceof MetadataBlock) {
      Map<Integer, String> exceptions = dataBlock.getExceptions();
      if (exceptions.isEmpty()) {
        block = TransferableBlockUtils.wrap(dataBlock);
      } else {
        setErrorBlock(TransferableBlockUtils.getErrorTransferableBlock(exceptions));
        return ReceivingMailboxStatus.FIRST_ERROR;
      }
    } else {
      block = TransferableBlockUtils.wrap(dataBlock);
    }
    return offerPrivate(block, timeoutMs);
  }

  public ReceivingMailboxStatus offer(TransferableBlock block, long timeoutMs) {
    long now = System.currentTimeMillis();
    _stats.merge(StatKey.WAIT_CPU_TIME_MS, now - _lastArriveTime);
    _lastArriveTime = now;
    _stats.merge(StatKey.IN_MEMORY_MESSAGES, 1);
    return offerPrivate(block, timeoutMs);
  }

  /**
   * Offers a non-error block into the mailbox within the timeout specified, returns whether the block is successfully
   * added. If the block is not added, an error block is added to the mailbox.
   */
  private ReceivingMailboxStatus offerPrivate(TransferableBlock block, long timeoutMs) {
    TransferableBlock errorBlock = _errorBlock.get();
    if (errorBlock != null) {
      LOGGER.debug("Mailbox: {} is already cancelled or errored out, ignoring the late block", _id);
      return errorBlock == CANCELLED_ERROR_BLOCK ? ReceivingMailboxStatus.CANCELLED
          : ReceivingMailboxStatus.ERROR;
    }
    if (timeoutMs <= 0) {
      LOGGER.debug("Mailbox: {} is already timed out", _id);
      setErrorBlock(TransferableBlockUtils.getErrorTransferableBlock(
          new TimeoutException("Timed out while offering data to mailbox: " + _id)));
      return ReceivingMailboxStatus.TIMEOUT;
    }
    try {
      long now = System.currentTimeMillis();
      boolean accepted = _blocks.offer(block, timeoutMs, TimeUnit.MILLISECONDS);
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
          return errorBlock == CANCELLED_ERROR_BLOCK ? ReceivingMailboxStatus.CANCELLED
              : ReceivingMailboxStatus.ERROR;
        }
      } else {
        LOGGER.debug("Failed to offer block into mailbox: {} within: {}ms", _id, timeoutMs);
        setErrorBlock(QException.EXECUTION_TIMEOUT_ERROR_CODE, "Timed out offering new data into mailbox: " + _id);
        return ReceivingMailboxStatus.TIMEOUT;
      }
    } catch (InterruptedException e) {
      LOGGER.error("Interrupted while offering block into mailbox: {}", _id);
      setErrorBlock(QException.INTERNAL_ERROR_CODE, "Interrupted while offering block into mailbox " + _id);
      return ReceivingMailboxStatus.ERROR;
    }
  }

  /**
   * Sets an error block into the mailbox. No more blocks are accepted after calling this method.
   */
  public void setErrorBlock(TransferableBlock errorBlock) {
    if (_errorBlock.compareAndSet(null, errorBlock)) {
      _blocks.clear();
      notifyReader();
    }
  }

  public void setErrorBlock(int errorCode, String errorMessage) {
    setErrorBlock(TransferableBlockUtils.getErrorTransferableBlock(errorCode, errorMessage));
  }

  /**
   * Returns the first block from the mailbox, or {@code null} if there is no block received yet. Error block is
   * returned if exists.
   */
  @Nullable
  public TransferableBlock poll() {
    Preconditions.checkState(_reader != null, "A reader must be registered");
    TransferableBlock errorBlock = _errorBlock.get();
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

  public void setOpChainContext(OpChainExecutionContext context) {
    _context = context;
  }

  @Nullable
  public OpChainExecutionContext getContext() {
    return _context;
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
}
