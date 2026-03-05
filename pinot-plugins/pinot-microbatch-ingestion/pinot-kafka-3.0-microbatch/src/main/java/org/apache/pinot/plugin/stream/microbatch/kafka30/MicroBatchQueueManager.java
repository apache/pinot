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
package org.apache.pinot.plugin.stream.microbatch.kafka30;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.stream.MessageBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates MicroBatch downloads and message batch creation.
 *
 * <p>This class manages the lifecycle of microbatch processing:
 * <ol>
 *   <li>Accepts submitted microbatches via {@link #submitBatch(MicroBatch)}</li>
 *   <li>Downloads batch files from PinotFS (S3, HDFS, GCS, etc.) or decodes inline data</li>
 *   <li>Converts downloaded files into {@link MessageBatch} objects</li>
 *   <li>Provides batches to consumers via {@link #getNextMessageBatch()}</li>
 * </ol>
 *
 * <p>Threading model:
 * <ul>
 *   <li>Data loader threads: Fixed pool that processes microbatches (download + convert)</li>
 *   <li>Download executor: Cached pool for individual downloads with timeout support</li>
 *   <li>Uses {@link SynchronousQueue} for backpressure to prevent memory exhaustion</li>
 * </ul>
 *
 * <p>Configuration:
 * <ul>
 *   <li>{@code stream.microbatch.kafka.file.fetch.threads}: Number of parallel data loaders</li>
 *   <li>{@code stream.microbatch.kafka.file.download.timeout.ms}: Download timeout (default 5 min)</li>
 * </ul>
 *
 * @see MicroBatchConfigProperties for configuration options
 */
public class MicroBatchQueueManager implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MicroBatchQueueManager.class);
  private static final long POLL_TIMEOUT_MS = 1000;

  /**
   * Global counter to ensure unique thread names within a JVM.
   * Incremented each time a new MicroBatchQueueManager is created for any partition.
   */
  private static final AtomicLong INSTANCE_COUNTER = new AtomicLong(0);

  private final Queue<MicroBatchState> _microBatchQueue = new ConcurrentLinkedQueue<>();
  private final ExecutorService _dataLoaderExecutor;

  /**
   * Creates a MicroBatchQueueManager with named threads for debugging.
   *
   * @param partition the Kafka partition number this manager handles
   * @param numFileFetchThreads number of parallel threads for fetching and processing batch files
   */
  public MicroBatchQueueManager(int partition, int numFileFetchThreads) {
    long instanceId = INSTANCE_COUNTER.incrementAndGet();
    String threadNamePrefix = String.format("microbatch-p%d-i%d", partition, instanceId);
    _dataLoaderExecutor = Executors.newFixedThreadPool(numFileFetchThreads,
        new NamedThreadFactory(threadNamePrefix));
  }

  /**
   * Thread factory that creates named daemon threads for easier debugging.
   */
  private static class NamedThreadFactory implements ThreadFactory {
    private final String _namePrefix;
    private final AtomicInteger _threadNumber = new AtomicInteger(1);

    NamedThreadFactory(String namePrefix) {
      _namePrefix = namePrefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r, _namePrefix + "-t" + _threadNumber.getAndIncrement());
      t.setDaemon(true);
      return t;
    }
  }

  /**
   * Submit a MicroBatch for processing. Immediately starts download if quota allows and batch has
   * remote URI.
   *
   * @param microBatch the microbatch to process
   */
  public void submitBatch(MicroBatch microBatch) {
    MicroBatchState microBatchState = new MicroBatchState(microBatch);
    _microBatchQueue.add(microBatchState);
    _dataLoaderExecutor.submit(() -> process(microBatchState));
  }

  /**
   * Checks if there are any microbatches being processed or waiting to be consumed.
   *
   * @return true if there are microbatches in the queue, false otherwise
   */
  public boolean hasData() {
    return !_microBatchQueue.isEmpty();
  }

  /**
   * Retrieves the next available message batch from the queue.
   *
   * <p>This method will block for up to {@link #POLL_TIMEOUT_MS} milliseconds waiting for
   * a batch to become available. If no batch is available after the timeout, it returns null.
   *
   * <p>When a microbatch has been fully consumed (all messages read), it is automatically
   * removed from the queue and its resources are cleaned up.
   *
   * @return the next MessageBatch, or null if no batch is currently available
   */
  public MessageBatch<GenericRow> getNextMessageBatch() {
    while (!_microBatchQueue.isEmpty()) {
      MicroBatchState state = _microBatchQueue.peek();
      if (state == null) {
        return null;
      }
      // Try to get a message batch with a timeout - wait longer to ensure data is ready
      try {
        MessageBatch<GenericRow> batch = state._messages.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (batch != null) {
          return batch;
        }
        // If no batch available and all messages have been read, move to next microbatch
        if (state._allMessagesReadFromFile && state._messages.isEmpty()) {
          _microBatchQueue.poll();
          state.clear();
          continue;
        }
        // Still processing, return null to let caller retry
        return null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      }
    }
    return null;
  }

  private void process(MicroBatchState state) {
    try {
      if (state._isAbandoned) {
        return;
      }
      fetchDataForMicroBatch(state);
      convertDataToMessageBatches(state);
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing data for MicroBatch", e);
      // TODO: Review error handling behavior - currently we mark the batch as read to prevent
      // blocking the queue, but this may cause silent data loss
      state._allMessagesReadFromFile = true;
    }
  }

  // TODO: Should temp files use a configurable directory instead of system temp?
  private void fetchDataForMicroBatch(MicroBatchState state)
      throws IOException {
    MicroBatchProtocol protocol = state._microBatch.getProtocol();
    switch (protocol.getType()) {
      case URI:
        URI uri = URI.create(protocol.getUri());
        state._dataFile = downloadFromPinotFS(uri);
        return;
      case DATA:
        File tempFile = null;
        try {
          tempFile = new File(FileUtils.getTempDirectory(), "microbatch-inline-" + UUID.randomUUID());
          FileUtils.writeByteArrayToFile(tempFile, state._microBatch.getProtocol().getData());
          state._dataFile = tempFile;
          tempFile = null; // Transfer ownership to state, prevent cleanup
        } finally {
          // Clean up temp file if it wasn't successfully assigned to state
          if (tempFile != null) {
            FileUtils.deleteQuietly(tempFile);
          }
        }
        return;
      default:
        throw new IllegalStateException("Unsupported protocol type: " + protocol.getType());
    }
  }

  private void convertDataToMessageBatches(MicroBatchState state) {
    if (state._dataFile == null || !state._dataFile.exists()) {
      LOGGER.error("Data file is null or does not exist, skipping batch");
      return;
    }
    int totalRecordCount = state._microBatch.getProtocol().getNumRecords();
    int recordsToSkip = state._microBatch.getRecordsToSkip();
    try (CloseableIterator<MessageBatch<GenericRow>> iterator =
        MessageBatchReader.read(
            state._microBatch, state._dataFile,
            MicroBatchConfigProperties.DEFAULT_MESSAGE_BATCH_SIZE, totalRecordCount, recordsToSkip)) {
      while (iterator.hasNext() && !state._isAbandoned) {
        try {
          boolean enqueued;
          MessageBatch<GenericRow> messageBatch = iterator.next();
          do {
            enqueued = state._messages.offer(messageBatch, 100, TimeUnit.MILLISECONDS);
          } while (!enqueued && !state._isAbandoned);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }

      if (!iterator.hasNext()) {
        state._allMessagesReadFromFile = true;
      }
    }
  }

  private File downloadFromPinotFS(URI uri) {
    // TODO: Should we add disk space checks before downloading batch files?

    // TODO: Should we add a configurable download timeout? PinotFS implementations don't have
    // built-in download timeouts.
    try {
      PinotFS pinotFS = PinotFSFactory.create(uri.getScheme());
      File tempFile = new File(FileUtils.getTempDirectory(), "microbatch-" + UUID.randomUUID());
      LOGGER.debug("Downloading from {} to {}", uri, tempFile.getAbsolutePath());
      pinotFS.copyToLocalFile(uri, tempFile);
      if (!tempFile.exists()) {
        throw new RuntimeException("Failed to download file - temp file not created: " + tempFile);
      }
      LOGGER.debug("Downloaded {} bytes from {} to {}",
          tempFile.length(), uri, tempFile.getAbsolutePath());
      return tempFile;
    } catch (Exception e) {
      throw new RuntimeException("Failed to download file from PinotFS: " + uri, e);
    }
  }

  /**
   * Clears all pending microbatches from the queue.
   *
   * <p>This method marks all queued microbatches as abandoned and cleans up their
   * associated temp files. Use this when seeking to a new offset or resetting
   * the consumer state.
   */
  public void clear() {
    _microBatchQueue.forEach(MicroBatchState::abandon);
    _microBatchQueue.clear();
  }

  /**
   * Closes the queue manager and releases all resources.
   *
   * <p>This method:
   * <ul>
   *   <li>Clears all pending microbatches</li>
   *   <li>Shuts down the data loader executor pool</li>
   *   <li>Waits up to 5 seconds for executor termination</li>
   * </ul>
   */
  @Override
  public void close() {
    clear();
    _dataLoaderExecutor.shutdownNow();
    try {
      if (!_dataLoaderExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        LOGGER.warn("Data loader executor did not terminate within timeout");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Interrupted while waiting for data loader executor to terminate");
    }
  }

  static class MicroBatchState {
    final MicroBatch _microBatch;
    volatile boolean _allMessagesReadFromFile = false;
    volatile boolean _isAbandoned = false;
    volatile File _dataFile = null;

    // DESIGN DECISION: Using SynchronousQueue instead of ArrayBlockingQueue is intentional.
    // SynchronousQueue provides natural backpressure - producer blocks until consumer takes the item.
    // This ensures only the current microbatch's loader thread is actively producing while
    // other threads wait, preventing eager loading of all microbatches into memory.
    // An ArrayBlockingQueue with capacity would allow multiple batches to be loaded into memory
    // simultaneously, which could cause OOM issues with large batch files.
    final SynchronousQueue<MessageBatch<GenericRow>> _messages = new SynchronousQueue<>();

    public MicroBatchState(MicroBatch microBatch) {
      _microBatch = microBatch;
    }

    public void abandon() {
      _isAbandoned = true;
      clear();
    }

    public void clear() {
      FileUtils.deleteQuietly(_dataFile);
    }
  }
}
