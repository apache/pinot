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
package org.apache.pinot.query.runtime.operator.exchange;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.ExceptionUtils;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class contains the shared logic across all different exchange types for
 * exchanging data across different servers.
 *
 * {@link BlockExchange} is used by {@link org.apache.pinot.query.runtime.operator.MailboxSendOperator} to
 * exchange data between underlying {@link org.apache.pinot.query.mailbox.MailboxService} and the query stage execution
 * engine running the actual {@link org.apache.pinot.query.runtime.operator.OpChain}.
 */
public abstract class BlockExchange {
  public static final int DEFAULT_MAX_PENDING_BLOCKS = 5;
  private static final Logger LOGGER = LoggerFactory.getLogger(BlockExchange.class);
  // TODO: Deduct this value via grpc config maximum byte size; and make it configurable with override.
  // TODO: Max block size is a soft limit. only counts fixedSize datatable byte buffer
  private static final int MAX_MAILBOX_CONTENT_SIZE_BYTES = 4 * 1024 * 1024;

  private final OpChainId _opChainId;
  private final List<SendingMailbox> _sendingMailboxes;
  private final BlockSplitter _splitter;
  private final Consumer<OpChainId> _callback;
  private final long _deadlineMs;

  private final BlockingQueue<TransferableBlock> _queue = new ArrayBlockingQueue<>(DEFAULT_MAX_PENDING_BLOCKS);
  private final AtomicReference<TransferableBlock> _errorBlock = new AtomicReference<>();

  public static BlockExchange getExchange(OpChainId opChainId, List<SendingMailbox> sendingMailboxes,
      RelDistribution.Type exchangeType, KeySelector<Object[], Object[]> selector, BlockSplitter splitter,
      Consumer<OpChainId> callback, long deadlineMs) {
    switch (exchangeType) {
      case SINGLETON:
        return new SingletonExchange(opChainId, sendingMailboxes, splitter, callback, deadlineMs);
      case HASH_DISTRIBUTED:
        return new HashExchange(opChainId, sendingMailboxes, selector, splitter, callback, deadlineMs);
      case RANDOM_DISTRIBUTED:
        return new RandomExchange(opChainId, sendingMailboxes, splitter, callback, deadlineMs);
      case BROADCAST_DISTRIBUTED:
        return new BroadcastExchange(opChainId, sendingMailboxes, splitter, callback, deadlineMs);
      case ROUND_ROBIN_DISTRIBUTED:
      case RANGE_DISTRIBUTED:
      case ANY:
      default:
        throw new UnsupportedOperationException("Unsupported mailbox exchange type: " + exchangeType);
    }
  }

  protected BlockExchange(OpChainId opChainId, List<SendingMailbox> sendingMailboxes, BlockSplitter splitter,
      Consumer<OpChainId> callback, long deadlineMs) {
    _opChainId = opChainId;
    _sendingMailboxes = sendingMailboxes;
    _splitter = splitter;
    _callback = callback;
    _deadlineMs = deadlineMs;
  }

  public boolean offerBlock(TransferableBlock block, long timeoutMs)
      throws Exception {
    return _queue.offer(block, timeoutMs, TimeUnit.MILLISECONDS);
  }

  public int getRemainingCapacity() {
    return _queue.remainingCapacity();
  }

  public TransferableBlock send() {
    try {
      TransferableBlock block;
      long timeoutMs = _deadlineMs - System.currentTimeMillis();
      if (_errorBlock.get() != null) {
        LOGGER.debug("Exchange: {} is already cancelled or errored out internally, ignore the late block", _opChainId);
        return _errorBlock.get();
      }
      block = _queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
      if (block == null) {
        block = TransferableBlockUtils.getErrorTransferableBlock(
            new TimeoutException("Timed out on exchange for opChain: " + _opChainId));
      } else {
        // Notify that the block exchange can now accept more blocks.
        _callback.accept(_opChainId);
        if (block.isEndOfStreamBlock()) {
          for (SendingMailbox sendingMailbox : _sendingMailboxes) {
            sendBlock(sendingMailbox, block);
          }
        } else {
          route(_sendingMailboxes, block);
        }
      }
      return block;
    } catch (Exception e) {
      TransferableBlock errorBlock = TransferableBlockUtils.getErrorTransferableBlock(
          new RuntimeException("Exception while sending data via exchange for opChain: " + _opChainId + "\n"
              + ExceptionUtils.consolidateExceptionMessages(e)));
      setErrorBlock(errorBlock);
      return errorBlock;
    }
  }

  protected void sendBlock(SendingMailbox sendingMailbox, TransferableBlock block)
      throws Exception {
    if (block.isEndOfStreamBlock()) {
      sendingMailbox.send(block);
      sendingMailbox.complete();
      return;
    }

    DataBlock.Type type = block.getType();
    Iterator<TransferableBlock> splits = _splitter.split(block, type, MAX_MAILBOX_CONTENT_SIZE_BYTES);
    while (splits.hasNext()) {
      sendingMailbox.send(splits.next());
    }
  }

  private void setErrorBlock(TransferableBlock errorBlock) {
    if (_errorBlock.compareAndSet(null, errorBlock)) {
      try {
        for (SendingMailbox sendingMailbox : _sendingMailboxes) {
          sendBlock(sendingMailbox, errorBlock);
        }
      } catch (Exception e) {
        LOGGER.error("error while sending exception block via exchange for opChain: " + _opChainId, e);
      }
      _queue.clear();
    }
  }

  protected abstract void route(List<SendingMailbox> destinations, TransferableBlock block)
      throws Exception;

  // Called when the OpChain gracefully returns.
  // TODO: This is a no-op right now.
  public void close() {
  }

  public void cancel(Throwable t) {
    for (SendingMailbox sendingMailbox : _sendingMailboxes) {
      sendingMailbox.cancel(t);
    }
  }
}
