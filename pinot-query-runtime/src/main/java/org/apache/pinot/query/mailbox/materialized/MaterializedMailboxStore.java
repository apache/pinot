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
package org.apache.pinot.query.mailbox.materialized;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.proto.Worker;


/// Thread-safe producer-local store for consumer-independent materialized partitions.
///
/// Writers publish with an atomic rename. Readers wait for that publication until their absolute deadline, then
/// stream framed records and delete the committed file only after reaching a clean end of file.
public final class MaterializedMailboxStore implements AutoCloseable {
  private static final String DATA_FILE_SUFFIX = ".data";
  private static final String TEMPORARY_FILE_SUFFIX = ".part";

  private final Path _root;
  private final String _hostname;
  private final int _port;
  private final Object _lifecycleLock = new Object();
  private final Map<MaterializedMailboxKey, Entry> _entries = new HashMap<>();
  // Reject late callbacks without scanning all cancelled requests on each partition read/write.
  private final Cache<Long, Boolean> _cleanedRequests =
      CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.HOURS).build();
  private boolean _closed;

  public MaterializedMailboxStore(Path root, String hostname, int port)
      throws IOException {
    _root = root;
    _hostname = hostname;
    _port = port;
    Files.createDirectories(root);
  }

  /// Returns the default process-local root, namespaced by the mailbox endpoint.
  public static Path defaultRoot(String hostname, int port) {
    String safeHostname = hostname.replaceAll("[^A-Za-z0-9._-]", "_");
    return Path.of(System.getProperty("java.io.tmpdir"), "pinot-materialized-mailbox",
        safeHostname + "-" + port);
  }

  /// Creates the sole writer for a partition. A reader may already be waiting for the same key.
  public MaterializedMailboxWriter createWriter(MaterializedMailboxKey key,
      Consumer<Worker.MaterializedPartitionHandle> onCommit)
      throws IOException {
    Path temporaryPath = getTemporaryPath(key);
    synchronized (_lifecycleLock) {
      ensureRequestActive(key.getRequestId());
      Entry entry = _entries.computeIfAbsent(key, ignored -> new Entry());
      if (entry._writerCreated) {
        throw new IllegalStateException("Materialized partition already has a writer: " + key);
      }
      entry._writerCreated = true;
      try {
        Files.createDirectories(temporaryPath.getParent());
        Files.deleteIfExists(temporaryPath);
        return new MaterializedMailboxWriter(this, key, temporaryPath, onCommit);
      } catch (IOException | RuntimeException e) {
        deleteAfterFailedWriteOpen(temporaryPath, e);
        failEntryLocked(key, entry, e);
        throw e;
      }
    }
  }

  /// Waits for commit and returns an iterator over serialized data-block records.
  public RecordIterator read(MaterializedMailboxKey key, long deadlineMs)
      throws IOException {
    Entry entry;
    synchronized (_lifecycleLock) {
      ensureRequestActive(key.getRequestId());
      entry = _entries.computeIfAbsent(key, ignored -> new Entry());
    }
    awaitCommit(key, entry, deadlineMs);
    synchronized (_lifecycleLock) {
      ensureRequestActive(key.getRequestId());
      if (_entries.get(key) != entry) {
        throw new IOException("Materialized partition was cleaned up before read: " + key);
      }
      FramedRecordIterator iterator = new FramedRecordIterator(key, entry, getCommittedPath(key));
      entry._readers.add(iterator);
      return iterator;
    }
  }

  /// Cancels pending readers and deletes every temporary or committed file for one request.
  public void cleanupRequest(long requestId) {
    CancellationException cancellation =
        new CancellationException("Materialized mailbox request was cleaned up: " + requestId);
    synchronized (_lifecycleLock) {
      if (_closed) {
        return;
      }
      _cleanedRequests.put(requestId, true);
      _entries.entrySet().removeIf(entry -> {
        if (entry.getKey().getRequestId() != requestId) {
          return false;
        }
        cancelEntryLocked(entry.getValue(), cancellation);
        return true;
      });
    }
    FileUtils.deleteQuietly(_root.resolve(Long.toString(requestId)).toFile());
  }

  @Override
  public void close() {
    synchronized (_lifecycleLock) {
      if (_closed) {
        return;
      }
      _closed = true;
      CancellationException cancellation = new CancellationException("Materialized mailbox store is closed");
      _entries.values().forEach(entry -> cancelEntryLocked(entry, cancellation));
      _entries.clear();
      _cleanedRequests.invalidateAll();
    }
    FileUtils.deleteQuietly(_root.toFile());
  }

  Worker.MaterializedPartitionHandle commit(MaterializedMailboxKey key, Path temporaryPath, long rowCount,
      Consumer<Worker.MaterializedPartitionHandle> onCommit)
      throws IOException {
    Path committedPath = getCommittedPath(key);
    synchronized (_lifecycleLock) {
      ensureRequestActive(key.getRequestId());
      Entry entry = _entries.get(key);
      if (entry == null || !entry._writerCreated) {
        throw new IOException("Materialized partition was cleaned up before commit: " + key);
      }
      try {
        Files.move(temporaryPath, committedPath, StandardCopyOption.ATOMIC_MOVE);
        Worker.MaterializedPartitionHandle handle = Worker.MaterializedPartitionHandle.newBuilder()
            .setRequestId(key.getRequestId())
            .setProducerStageId(key.getProducerStageId())
            .setProducerWorkerId(key.getProducerWorkerId())
            .setLogicalPartitionId(key.getLogicalPartitionId())
            .setHost(_hostname)
            .setTransferPort(_port)
            .setOpaqueFileId(key.toOpaqueFileId())
            .setRowCount(rowCount)
            .setByteCount(Files.size(committedPath))
            .build();
        onCommit.accept(handle);
        entry._committed.complete(handle);
        return handle;
      } catch (IOException | RuntimeException e) {
        deleteAfterFailedCommit(temporaryPath, committedPath, e);
        failEntryLocked(key, entry, e);
        if (e instanceof IOException) {
          throw (IOException) e;
        }
        throw new IOException("Failed to publish materialized partition: " + key, e);
      }
    }
  }

  void abort(MaterializedMailboxKey key, Path temporaryPath)
      throws IOException {
    IOException deletionFailure = null;
    try {
      Files.deleteIfExists(temporaryPath);
    } catch (IOException e) {
      deletionFailure = e;
    }
    synchronized (_lifecycleLock) {
      Entry entry = _entries.remove(key);
      if (entry != null) {
        cancelEntryLocked(entry,
            new CancellationException("Materialized partition was aborted: " + key));
      }
    }
    deleteEmptyParents(temporaryPath.getParent());
    if (deletionFailure != null) {
      throw deletionFailure;
    }
  }

  @VisibleForTesting
  public Path getCommittedPath(MaterializedMailboxKey key) {
    return partitionDirectory(key).resolve(key.getLogicalPartitionId() + DATA_FILE_SUFFIX);
  }

  private Path getTemporaryPath(MaterializedMailboxKey key) {
    return partitionDirectory(key).resolve(key.getLogicalPartitionId() + TEMPORARY_FILE_SUFFIX);
  }

  private Path partitionDirectory(MaterializedMailboxKey key) {
    return _root.resolve(Long.toString(key.getRequestId()))
        .resolve(Integer.toString(key.getProducerStageId()))
        .resolve(Integer.toString(key.getProducerWorkerId()));
  }

  private void awaitCommit(MaterializedMailboxKey key, Entry entry, long deadlineMs)
      throws IOException {
    long remainingMs = deadlineMs - System.currentTimeMillis();
    if (remainingMs <= 0) {
      throw timeout(key);
    }
    try {
      entry._committed.get(remainingMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw timeout(key);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for materialized partition: " + key, e);
    } catch (ExecutionException | CancellationException e) {
      throw new IOException("Materialized partition is unavailable: " + key, e);
    }
  }

  private void ensureRequestActive(long requestId)
      throws IOException {
    if (_closed) {
      throw new IOException("Materialized mailbox store is closed");
    }
    if (_cleanedRequests.getIfPresent(requestId) != null) {
      throw new IOException("Materialized mailbox request was cleaned up: " + requestId);
    }
  }

  private static SocketTimeoutException timeout(MaterializedMailboxKey key) {
    return new SocketTimeoutException("Timed out waiting for materialized partition commit: " + key);
  }

  private void consume(MaterializedMailboxKey key, Entry entry, Path path, FramedRecordIterator reader)
      throws IOException {
    synchronized (_lifecycleLock) {
      entry._readers.remove(reader);
      Files.deleteIfExists(path);
      _entries.remove(key, entry);
      deleteEmptyParents(path.getParent());
    }
  }

  private void unregisterReader(Entry entry, FramedRecordIterator reader) {
    synchronized (_lifecycleLock) {
      entry._readers.remove(reader);
    }
  }

  private void cancelEntryLocked(Entry entry, CancellationException cancellation) {
    for (FramedRecordIterator reader : List.copyOf(entry._readers)) {
      reader.closeWithoutConsume();
    }
    entry._committed.completeExceptionally(cancellation);
  }

  private void failEntryLocked(MaterializedMailboxKey key, Entry entry, Throwable failure) {
    entry._committed.completeExceptionally(failure);
    _entries.remove(key, entry);
  }

  private static void deleteAfterFailedCommit(Path temporaryPath, Path committedPath, Throwable failure) {
    try {
      Files.deleteIfExists(temporaryPath);
      Files.deleteIfExists(committedPath);
    } catch (IOException cleanupFailure) {
      failure.addSuppressed(cleanupFailure);
    }
  }

  private static void deleteAfterFailedWriteOpen(Path temporaryPath, Throwable failure) {
    try {
      Files.deleteIfExists(temporaryPath);
    } catch (IOException cleanupFailure) {
      failure.addSuppressed(cleanupFailure);
    }
  }

  private void deleteEmptyParents(Path directory) {
    Path current = directory;
    while (current != null && !current.equals(_root)) {
      try (Stream<Path> children = Files.list(current)) {
        if (children.findAny().isPresent()) {
          return;
        }
        Files.deleteIfExists(current);
      } catch (IOException e) {
        return;
      }
      current = current.getParent();
    }
  }

  private static final class Entry {
    private final CompletableFuture<Worker.MaterializedPartitionHandle> _committed = new CompletableFuture<>();
    private final HashSet<FramedRecordIterator> _readers = new HashSet<>();
    private boolean _writerCreated;
  }

  /// Closeable iterator over one committed partition. Explicit close abandons the read without consuming the file.
  public interface RecordIterator extends Iterator<byte[]>, AutoCloseable {
    @Override
    void close()
        throws IOException;
  }

  private final class FramedRecordIterator implements RecordIterator {
    private final MaterializedMailboxKey _key;
    private final Entry _entry;
    private final Path _path;
    private final DataInputStream _input;
    private long _remainingBytes;
    private byte[] _next;
    private volatile boolean _finished;

    private FramedRecordIterator(MaterializedMailboxKey key, Entry entry, Path path)
        throws IOException {
      _key = key;
      _entry = entry;
      _path = path;
      _remainingBytes = Files.size(path);
      _input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
    }

    @Override
    public boolean hasNext() {
      if (_finished) {
        return false;
      }
      if (_next != null) {
        return true;
      }
      try {
        if (_remainingBytes == 0) {
          finish();
          return false;
        }
        int length = _input.readInt();
        _remainingBytes -= Integer.BYTES;
        if (length <= 0 || length > _remainingBytes) {
          throw new IOException("Invalid materialized record length " + length + " in " + _path);
        }
        _next = new byte[length];
        _input.readFully(_next);
        _remainingBytes -= length;
        return true;
      } catch (IOException e) {
        closeWithoutConsume();
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public byte[] next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      byte[] next = _next;
      _next = null;
      return next;
    }

    private void finish()
        throws IOException {
      _finished = true;
      _next = null;
      try {
        _input.close();
      } catch (IOException e) {
        unregisterReader(_entry, this);
        throw e;
      }
      consume(_key, _entry, _path, this);
    }

    @Override
    public void close()
        throws IOException {
      if (_finished) {
        return;
      }
      _finished = true;
      _next = null;
      try {
        _input.close();
      } finally {
        unregisterReader(_entry, this);
      }
    }

    private void closeWithoutConsume() {
      try {
        close();
      } catch (IOException ignored) {
        // Cancellation and read failures preserve their original cause.
      }
    }
  }
}
