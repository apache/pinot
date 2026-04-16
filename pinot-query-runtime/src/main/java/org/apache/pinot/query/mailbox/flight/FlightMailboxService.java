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
package org.apache.pinot.query.mailbox.flight;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.datablock.ArrowDataBlock;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.segment.spi.memory.ArrowBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An Arrow Flight-based mailbox service that coexists with the gRPC mailbox service.
 *
 * <p>Data blocks travel over Flight's {@code acceptPut} endpoint while EOS and error blocks continue to use gRPC.
 * This design allows a zero-downtime rollout: senders choose whether to use Flight or gRPC on a per-mailbox basis,
 * and both receivers run simultaneously on the same node.
 *
 * <p>The Flight server binds on a separate port from gRPC (configured at construction time).
 */
public class FlightMailboxService implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FlightMailboxService.class);
  private static final int DANGLING_MAILBOX_EXPIRY_SECONDS = 300;

  private final FlightServer _flightServer;
  private final FlightChannelManager _channelManager;
  private final ArrowBuffers _arrowBuffers;
  private final Cache<String, ReceivingMailbox> _receivingMailboxCache;

  public FlightMailboxService(String hostname, int flightPort, @Nullable TlsConfig tlsConfig,
      ArrowBuffers arrowBuffers) {
    _receivingMailboxCache = CacheBuilder.newBuilder()
        .expireAfterAccess(DANGLING_MAILBOX_EXPIRY_SECONDS, TimeUnit.SECONDS)
        .removalListener((RemovalListener<String, ReceivingMailbox>) notification -> {
          if (notification.wasEvicted()) {
            int pending = notification.getValue().getNumPendingBlocks();
            if (pending > 0) {
              LOGGER.warn("Evicting dangling Flight receiving mailbox: {} with {} pending blocks",
                  notification.getKey(), pending);
            }
          }
        })
        .build();

    Location location = tlsConfig != null
        ? Location.forGrpcTls(hostname, flightPort)
        : Location.forGrpcInsecure(hostname, flightPort);

    _arrowBuffers = arrowBuffers;
    FlightServer.Builder builder = FlightServer.builder(
        arrowBuffers.newQueryAllocator("flight-mailbox-server"), location, new MailboxFlightProducer());

    if (tlsConfig != null) {
      try {
        builder.useTls(new File(tlsConfig.getKeyStorePath()), new File(tlsConfig.getTrustStorePath()));
      } catch (IOException e) {
        throw new RuntimeException("Failed to configure Flight TLS", e);
      }
    }
    _flightServer = builder.build();
    _channelManager = new FlightChannelManager(tlsConfig, arrowBuffers);
  }

  /** Starts the Flight server. Must be called before any mailboxes are used. */
  public void start() {
    try {
      _flightServer.start();
      LOGGER.info("Arrow Flight mailbox server started on port {}", _flightServer.getPort());
    } catch (Exception e) {
      throw new RuntimeException("Failed to start Arrow Flight mailbox server", e);
    }
  }

  /** Returns (or creates) the receiving mailbox for the given id. */
  public ReceivingMailbox getReceivingMailbox(String mailboxId) {
    try {
      return _receivingMailboxCache.get(mailboxId, () -> new ReceivingMailbox(mailboxId));
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /** Removes the receiving mailbox from the cache when it is no longer needed. */
  public void releaseReceivingMailbox(ReceivingMailbox mailbox) {
    _receivingMailboxCache.invalidate(mailbox.getId());
  }

  public FlightChannelManager getChannelManager() {
    return _channelManager;
  }

  @Override
  public void close() {
    try {
      _flightServer.close();
    } catch (Exception e) {
      LOGGER.warn("Error stopping Arrow Flight server", e);
    }
    try {
      _channelManager.close();
    } catch (Exception e) {
      LOGGER.warn("Error closing Arrow Flight channel manager", e);
    }
  }

  // ----- Flight producer (server-side stream receiver) -----

  private class MailboxFlightProducer extends org.apache.arrow.flight.NoOpFlightProducer {
    @Override
    public Runnable acceptPut(CallContext context, org.apache.arrow.flight.FlightStream stream,
        StreamListener<PutResult> ack) {
      return () -> {
        String mailboxId = extractMailboxId(stream);
        if (mailboxId == null) {
          ack.onError(new IllegalArgumentException("Missing mailbox ID in Flight descriptor"));
          return;
        }
        try {
          ReceivingMailbox mailbox = _receivingMailboxCache.get(mailboxId, () -> new ReceivingMailbox(mailboxId));
          processStream(stream, mailbox);
          ack.onCompleted();
        } catch (Exception e) {
          LOGGER.error("Error in Flight acceptPut for mailbox: {}", mailboxId, e);
          ack.onError(e);
        }
      };
    }

    @Nullable
    private String extractMailboxId(org.apache.arrow.flight.FlightStream stream) {
      try {
        return stream.getDescriptor().getPath().get(0);
      } catch (Exception e) {
        LOGGER.warn("Could not extract mailbox ID from Flight descriptor", e);
        return null;
      }
    }

    private long extractDeadlineMs(org.apache.arrow.flight.FlightStream stream) {
      try {
        List<String> path = stream.getDescriptor().getPath();
        if (path.size() >= 2) {
          return Long.parseLong(path.get(1));
        }
      } catch (Exception e) {
        LOGGER.warn("Could not extract deadline from Flight descriptor, using no deadline", e);
      }
      return Long.MAX_VALUE;
    }

    private void processStream(org.apache.arrow.flight.FlightStream stream, ReceivingMailbox mailbox) {
      long deadlineMs = extractDeadlineMs(stream);
      try (stream) {
        while (stream.next()) {
          VectorSchemaRoot root = stream.getRoot();
          DictionaryProvider dictProvider = stream.getDictionaryProvider();

          // Transfer buffer ownership from the Flight stream's allocator to our own allocator.
          // This is O(1) per vector — no data bytes are copied, only buffer references are moved.
          BufferAllocator blockAllocator = _arrowBuffers.newQueryAllocator("flight-stream-block");
          VectorSchemaRoot ownedRoot = VectorSchemaRoot.create(root.getSchema(), blockAllocator);
          for (int i = 0; i < root.getFieldVectors().size(); i++) {
            root.getVector(i).makeTransferPair(ownedRoot.getVector(i)).transfer();
          }
          ownedRoot.setRowCount(root.getRowCount());

          DictionaryProvider copiedProvider =
              ArrowDataBlock.copyDictionaryProvider(dictProvider, blockAllocator);
          ArrowBlock block = new ArrowBlock(new ArrowDataBlock(ownedRoot, copiedProvider));

          long timeoutMs = deadlineMs - System.currentTimeMillis();
          ReceivingMailbox.ReceivingMailboxStatus status =
              mailbox.offer(block, Collections.emptyList(), Math.max(0, timeoutMs));

          if (status == ReceivingMailbox.ReceivingMailboxStatus.LAST_BLOCK
              || status == ReceivingMailbox.ReceivingMailboxStatus.ALREADY_TERMINATED) {
            LOGGER.warn("Mailbox {} returned status {}, stopping stream", mailbox.getId(), status);
            return;
          }
        }
      } catch (Exception e) {
        LOGGER.error("Error processing Flight stream for mailbox: {}", mailbox.getId(), e);
        long timeoutMs = deadlineMs - System.currentTimeMillis();
        mailbox.offer(ErrorMseBlock.fromException(e), Collections.emptyList(), Math.max(0, timeoutMs));
      }
    }
  }
}
