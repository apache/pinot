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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Mailbox that's used to receive data. Ownership of the ReceivingMailbox is with the MailboxService, which is unlike
 * the {@link SendingMailbox} whose ownership lies with the send operator. This is because the ReceivingMailbox can be
 * initialized even before the corresponding OpChain is registered on the receiver, whereas the SendingMailbox is
 * initialized when the send operator is running.
 */
public class ReceivingMailbox {
  public static final int DEFAULT_MAX_PENDING_BLOCKS = 5;

  private static final Logger LOGGER = LoggerFactory.getLogger(ReceivingMailbox.class);
  private static final TransferableBlock CANCELLED_ERROR_BLOCK =
      TransferableBlockUtils.getErrorTransferableBlock(new RuntimeException("Cancelled by receiver"));

  private final String _id;
  // TODO: Make the queue size configurable
  // TODO: Revisit if this is the correct way to apply back pressure
  private final BlockingQueue<TransferableBlock> _blocks = new ArrayBlockingQueue<>(DEFAULT_MAX_PENDING_BLOCKS);
  private final AtomicReference<TransferableBlock> _errorBlock = new AtomicReference<>();
  private volatile boolean _isEarlyTerminated = false;

  @Nullable
  private volatile Reader _reader;

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
   * Offers a non-error block into the mailbox within the timeout specified, returns whether the block is successfully
   * added. If the block is not added, an error block is added to the mailbox.
   */
  public ReceivingMailboxStatus offer(TransferableBlock block, long timeoutMs) {
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
      if (_blocks.offer(block, timeoutMs, TimeUnit.MILLISECONDS)) {
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
        setErrorBlock(TransferableBlockUtils.getErrorTransferableBlock(
            new TimeoutException("Timed out while waiting for receive operator to consume data from mailbox: " + _id)));
        return ReceivingMailboxStatus.TIMEOUT;
      }
    } catch (InterruptedException e) {
      LOGGER.error("Interrupted while offering block into mailbox: {}", _id);
      setErrorBlock(TransferableBlockUtils.getErrorTransferableBlock(e));
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

  public interface Reader {
    void blockReadyToRead();
  }

  public enum ReceivingMailboxStatus {
    SUCCESS, ERROR, TIMEOUT, CANCELLED, EARLY_TERMINATED
  }
}
