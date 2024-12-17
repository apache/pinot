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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An exchange that buffers the blocks in a queue before sending them to the destination.
 *
 * This is useful when one of the destinations is slow and we want to buffer the blocks for that destination without
 * affecting the other destinations.
 *
 * The current implementation buffers the blocks on heap, but it may be changed in future versions if needed.
 */
public class BufferedExchange extends BlockExchange {
  private static final Logger LOGGER = LoggerFactory.getLogger(BufferedExchange.class);

  private static final Throwable CORRECT_MARKER = new Exception("Fake exception marking correct completion");
  private final BlockingQueue<TransferableBlock>[] _queues;
  @Nullable
  private volatile Throwable _throwable = null;

  public BufferedExchange(List<SendingMailbox> sendingMailboxes, BlockSplitter splitter, long requestId, int stageId,
      int workerId) {
    super(sendingMailboxes, splitter);
    ThreadFactory threadFactory = new NamedThreadFactory(
        "BufferedExchange-req-" + requestId + "-s" + stageId + "-w" + workerId);
    _queues = new BlockingQueue[sendingMailboxes.size()];
    for (int i = 0; i < sendingMailboxes.size(); i++) {
      final SendingMailbox sendingMailbox = sendingMailboxes.get(i);
      final BlockingQueue<TransferableBlock> buffer = new ArrayBlockingQueue<>(1000);
      final int index = i;
      _queues[i] = buffer;
      Runnable runnable = () -> {
        while (_throwable == null) {
          try {
            LOGGER.trace("Reading from {}", index);
            TransferableBlock block = buffer.take();
            LOGGER.trace("Read from {}. Starting to send", index);
            sendingMailbox.send(block);
            LOGGER.trace("Send to {} finished", index);
          } catch (InterruptedException | IOException | TimeoutException e) {
            sendingMailbox.cancel(e);
            return;
          }
        }
        if (_throwable != CORRECT_MARKER) {
          LOGGER.trace("Exceptional post completion for {}", index);
          sendingMailbox.cancel(_throwable);
        } else {
          LOGGER.trace("Normal post completion for {}", index);
          while (!buffer.isEmpty()) {
            TransferableBlock poll = buffer.poll();
            LOGGER.trace("Post completion read from {}. Starting to send", index);
            try {
              sendingMailbox.send(poll);
              LOGGER.trace("Post completion send to {} finished", index);
            } catch (IOException | TimeoutException e) {
              LOGGER.info("Failed to send {} block after successful completion", poll.getType(), e);
              return;
            }
          }
        }
      };
      threadFactory.newThread(runnable).start();
    }
  }

  @Override
  protected void route(List<SendingMailbox> destinations, TransferableBlock block)
      throws IOException, TimeoutException {
    LOGGER.trace("Routing block: {} {} to {}", block, System.identityHashCode(block), destinations);
    for (int i = 0; i < _queues.length; i++) {
      if (!_queues[i].offer(block)) {
        cancel(new RuntimeException("Failed to offer block to queue " + i));
      }
    }
  }

  @Override
  public void close() {
    super.close();
    LOGGER.trace("Closing BufferedExchange");
    _throwable = CORRECT_MARKER;
  }

  @Override
  public void cancel(Throwable t) {
    LOGGER.trace("Cancelling BufferedExchange", t);
    _throwable = t;
  }
}
