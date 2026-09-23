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
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
import org.apache.pinot.common.proto.Worker;


/// Thread-safe producer-local store for consumer-independent materialized partitions.
///
/// Writers publish with an atomic rename. Readers wait for that publication until their absolute deadline, then
/// stream framed records and delete the committed file only after reaching a clean end of file.
public final class MaterializedMailboxStore implements AutoCloseable {
  // Query work is deadline-bounded. One hour keeps stale callbacks rejected well beyond the default query timeout
  // without retaining canceled request IDs for the process lifetime.
  static final long CLEANED_REQUEST_RETENTION_MS = TimeUnit.HOURS.toMillis(1);
  private static final String DATA_FILE_SUFFIX = ".data";
  private static final String TEMPORARY_FILE_SUFFIX = ".part";

  private final Path _root;
  private final String _hostname;
  private final int _port;
  private final OutputStreamFactory _outputStreamFactory;
  private final LongSupplier _currentTimeMillis;
  private final Object _lifecycleLock = new Object();
  private final Map<MaterializedMailboxKey, Entry> _entries = new HashMap<>();
  private final LinkedHashMap<Long, Long> _cleanedRequests = new LinkedHashMap<>();
  private boolean _closed;

  public MaterializedMailboxStore(Path root, String hostname, int port)
      throws IOException {
    this(root, hostname, port, Files::newOutputStream, System::currentTimeMillis);
  }

  @VisibleForTesting
  MaterializedMailboxStore(Path root, String hostname, int port, OutputStreamFactory outputStreamFactory)
      throws IOException {
    this(root, hostname, port, outputStreamFactory, System::currentTimeMillis);
  }

  @VisibleForTesting
  MaterializedMailboxStore(Path root, String hostname, int port, LongSupplier currentTimeMillis)
      throws IOException {
    this(root, hostname, port, Files::newOutputStream, currentTimeMillis);
  }

  private MaterializedMailboxStore(Path root, String hostname, int port, OutputStreamFactory outputStreamFactory,
      LongSupplier currentTimeMillis)
      throws IOException {
    _root = root;
    _hostname = hostname;
    _port = port;
    _outputStreamFactory = outputStreamFactory;
    _currentTimeMillis = currentTimeMillis;
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
    Consumer<Worker.MaterializedPartitionHandle> commitConsumer = Objects.requireNonNull(onCommit);
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
        return new MaterializedMailboxWriter(this, key, temporaryPath,
            _outputStreamFactory.open(temporaryPath), commitConsumer);
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
      entry._waitingReaders++;
    }
    try {
      awaitCommit(key, entry, deadlineMs);
    } finally {
      synchronized (_lifecycleLock) {
        entry._waitingReaders--;
      }
    }
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
      rememberCleanedRequestLocked(requestId);
      List<MaterializedMailboxKey> requestKeys = new ArrayList<>();
      for (MaterializedMailboxKey key : _entries.keySet()) {
        if (key.getRequestId() == requestId) {
          requestKeys.add(key);
        }
      }
      for (MaterializedMailboxKey key : requestKeys) {
        Entry entry = _entries.remove(key);
        if (entry != null) {
          cancelEntryLocked(entry, cancellation);
        }
      }
    }
    deleteRecursively(_root.resolve(Long.toString(requestId)));
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
      _cleanedRequests.clear();
    }
    deleteRecursively(_root);
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
        try {
          Files.move(temporaryPath, committedPath, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
          throw new IOException("Materialized mailbox root does not support atomic commit: " + _root, e);
        }
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
        if (!entry._committed.complete(handle)) {
          throw new IOException("Materialized partition was cleaned up before commit: " + key);
        }
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

  @VisibleForTesting
  Path getTemporaryPath(MaterializedMailboxKey key) {
    return partitionDirectory(key).resolve(key.getLogicalPartitionId() + TEMPORARY_FILE_SUFFIX);
  }

  @VisibleForTesting
  int getWaitingReaderCount(MaterializedMailboxKey key) {
    synchronized (_lifecycleLock) {
      Entry entry = _entries.get(key);
      return entry != null ? entry._waitingReaders : 0;
    }
  }

  @VisibleForTesting
  int getCleanedRequestCount() {
    synchronized (_lifecycleLock) {
      pruneCleanedRequestsLocked(_currentTimeMillis.getAsLong());
      return _cleanedRequests.size();
    }
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
    pruneCleanedRequestsLocked(_currentTimeMillis.getAsLong());
    if (_cleanedRequests.containsKey(requestId)) {
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

  private void rememberCleanedRequestLocked(long requestId) {
    long nowMs = _currentTimeMillis.getAsLong();
    pruneCleanedRequestsLocked(nowMs);
    _cleanedRequests.put(requestId, nowMs + CLEANED_REQUEST_RETENTION_MS);
  }

  private void pruneCleanedRequestsLocked(long nowMs) {
    _cleanedRequests.entrySet().removeIf(entry -> entry.getValue() <= nowMs);
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

  private static void deleteRecursively(Path path) {
    if (!Files.exists(path)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(path)) {
      paths.sorted(Comparator.reverseOrder()).forEach(current -> {
        try {
          Files.deleteIfExists(current);
        } catch (IOException ignored) {
          // Best-effort cleanup; query execution has already ended.
        }
      });
    } catch (IOException ignored) {
      // Best-effort cleanup; query execution has already ended.
    }
  }

  private static final class Entry {
    private final CompletableFuture<Worker.MaterializedPartitionHandle> _committed = new CompletableFuture<>();
    private final HashSet<FramedRecordIterator> _readers = new HashSet<>();
    private boolean _writerCreated;
    private int _waitingReaders;
  }

  /// Closeable iterator over one committed partition. Explicit close abandons the read without consuming the file.
  public interface RecordIterator extends Iterator<byte[]>, AutoCloseable {
    @Override
    void close()
        throws IOException;
  }

  @FunctionalInterface
  interface OutputStreamFactory {
    OutputStream open(Path path)
        throws IOException;
  }

  private final class FramedRecordIterator implements RecordIterator {
    private final MaterializedMailboxKey _key;
    private final Entry _entry;
    private final Path _path;
    private final DataInputStream _input;
    private byte[] _next;
    private boolean _finished;

    private FramedRecordIterator(MaterializedMailboxKey key, Entry entry, Path path)
        throws IOException {
      _key = key;
      _entry = entry;
      _path = path;
      _input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
    }

    @Override
    public boolean hasNext() {
      if (_next != null) {
        return true;
      }
      if (_finished) {
        return false;
      }
      try {
        int length;
        try {
          length = _input.readInt();
        } catch (EOFException e) {
          finish();
          return false;
        }
        if (length < 0) {
          throw new IOException("Negative materialized record length " + length + " in " + _path);
        }
        _next = new byte[length];
        _input.readFully(_next);
        return true;
      } catch (IOException e) {
        closeWithoutDelete();
        throw new java.io.UncheckedIOException(e);
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
      try {
        _input.close();
      } catch (IOException e) {
        unregisterReader(_entry, this);
        throw e;
      }
      consume(_key, _entry, _path, this);
    }

    private void closeWithoutDelete() {
      closeWithoutConsume();
    }

    @Override
    public void close()
        throws IOException {
      if (_finished) {
        return;
      }
      _finished = true;
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
