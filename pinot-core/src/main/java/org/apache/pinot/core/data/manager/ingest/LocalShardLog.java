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
package org.apache.pinot.core.data.manager.ingest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pinot.spi.ingest.ShardLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * File-based append-only implementation of {@link ShardLog}.
 *
 * <p>Data is written as length-prefixed entries to a log file:
 * <pre>
 *   [4 bytes: entry length][N bytes: entry data]
 * </pre>
 *
 * <p>The offset returned by {@link #append(byte[])} is the byte offset in the log file where the
 * entry begins. Statement metadata (prepared/committed/aborted state and offset ranges) is tracked
 * in a separate metadata file.
 *
 * <p>This implementation is thread-safe for concurrent appends via a read-write lock.
 * Prepare/commit/abort operations for a given statement are expected to be serialized externally.
 */
public class LocalShardLog implements ShardLog {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalShardLog.class);
  private static final String LOG_FILE_NAME = "data.log";
  private static final String META_FILE_NAME = "meta.log";

  private final File _logDir;
  private final File _logFile;
  private final File _metaFile;
  private final ReadWriteLock _lock = new ReentrantReadWriteLock();
  private final AtomicLong _writeOffset = new AtomicLong(0);
  private final Map<String, StatementMeta> _statementMetas = new ConcurrentHashMap<>();

  /**
   * Creates a new local shard log under the given directory.
   *
   * @param logDir the directory for this shard log's files
   * @throws IOException if the directory cannot be created or the log file cannot be initialized
   */
  public LocalShardLog(File logDir)
      throws IOException {
    _logDir = logDir;
    if (!_logDir.exists() && !_logDir.mkdirs()) {
      throw new IOException("Failed to create log directory: " + _logDir);
    }
    _logFile = new File(_logDir, LOG_FILE_NAME);
    _metaFile = new File(_logDir, META_FILE_NAME);

    if (_logFile.exists()) {
      _writeOffset.set(_logFile.length());
    }

    // Recover statement metadata from the meta file
    recoverMetadata();
  }

  @Override
  public long append(byte[] data) {
    _lock.writeLock().lock();
    try {
      long offset = _writeOffset.get();
      try (FileOutputStream fos = new FileOutputStream(_logFile, true);
           FileChannel channel = fos.getChannel()) {
        ByteBuffer header = ByteBuffer.allocate(4);
        header.putInt(data.length);
        header.flip();
        channel.write(header);
        channel.write(ByteBuffer.wrap(data));
        channel.force(true);
      } catch (IOException e) {
        throw new RuntimeException("Failed to append to shard log: " + _logFile, e);
      }
      _writeOffset.addAndGet(4 + data.length);
      return offset;
    } finally {
      _lock.writeLock().unlock();
    }
  }

  @Override
  public void prepare(String statementId, long startOffset, long endOffset) {
    StatementMeta meta = new StatementMeta(statementId, startOffset, endOffset, StatementStatus.PREPARED);
    _statementMetas.put(statementId, meta);
    persistMetaEntry(statementId, startOffset, endOffset, StatementStatus.PREPARED);
  }

  @Override
  public void commit(String statementId) {
    StatementMeta existing = _statementMetas.get(statementId);
    if (existing == null) {
      LOGGER.warn("Attempted to commit unknown statement: {}", statementId);
      return;
    }
    StatementMeta committed = new StatementMeta(
        statementId, existing._startOffset, existing._endOffset, StatementStatus.COMMITTED);
    _statementMetas.put(statementId, committed);
    persistMetaEntry(statementId, existing._startOffset, existing._endOffset, StatementStatus.COMMITTED);
  }

  @Override
  public void abort(String statementId) {
    StatementMeta existing = _statementMetas.get(statementId);
    long startOffset = existing != null ? existing._startOffset : -1;
    long endOffset = existing != null ? existing._endOffset : -1;
    StatementMeta aborted = new StatementMeta(statementId, startOffset, endOffset, StatementStatus.ABORTED);
    _statementMetas.put(statementId, aborted);
    persistMetaEntry(statementId, startOffset, endOffset, StatementStatus.ABORTED);
  }

  @Override
  public Iterator<byte[]> read(long fromOffset) {
    return new LogIterator(_logFile, fromOffset, _writeOffset.get());
  }

  /**
   * Returns the current write offset (end of log).
   */
  public long getWriteOffset() {
    return _writeOffset.get();
  }

  /**
   * Returns the metadata for the given statement, or {@code null} if not found.
   */
  public StatementMeta getStatementMeta(String statementId) {
    return _statementMetas.get(statementId);
  }

  private void persistMetaEntry(String statementId, long startOffset, long endOffset, StatementStatus status) {
    try (FileOutputStream fos = new FileOutputStream(_metaFile, true);
         FileChannel channel = fos.getChannel()) {
      byte[] line = (statementId + "," + startOffset + "," + endOffset + "," + status.name() + "\n")
          .getBytes(java.nio.charset.StandardCharsets.UTF_8);
      channel.write(ByteBuffer.wrap(line));
      channel.force(true);
    } catch (IOException e) {
      String message = "Failed to persist metadata for statement: " + statementId;
      LOGGER.error(message, e);
      throw new IllegalStateException(message, e);
    }
  }

  private void recoverMetadata() {
    if (!_metaFile.exists()) {
      return;
    }
    try (BufferedReader reader = new BufferedReader(new FileReader(_metaFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty()) {
          continue;
        }
        String[] parts = line.split(",", 4);
        if (parts.length == 4) {
          String stmtId = parts[0];
          long start = Long.parseLong(parts[1]);
          long end = Long.parseLong(parts[2]);
          StatementStatus status = StatementStatus.valueOf(parts[3]);
          _statementMetas.put(stmtId, new StatementMeta(stmtId, start, end, status));
        }
      }
    } catch (IOException e) {
      LOGGER.error("Failed to recover metadata from: {}", _metaFile, e);
    }
  }

  /**
   * Lifecycle status of a statement within the shard log.
   */
  public enum StatementStatus {
    PREPARED,
    COMMITTED,
    ABORTED
  }

  /**
   * Metadata for a statement's offset range and status in the shard log.
   */
  public static class StatementMeta {
    final String _statementId;
    final long _startOffset;
    final long _endOffset;
    final StatementStatus _status;

    public StatementMeta(String statementId, long startOffset, long endOffset, StatementStatus status) {
      _statementId = statementId;
      _startOffset = startOffset;
      _endOffset = endOffset;
      _status = status;
    }

    public String getStatementId() {
      return _statementId;
    }

    public long getStartOffset() {
      return _startOffset;
    }

    public long getEndOffset() {
      return _endOffset;
    }

    public StatementStatus getStatus() {
      return _status;
    }
  }

  /**
   * Iterator that reads length-prefixed entries from the log file.
   */
  private static class LogIterator implements Iterator<byte[]> {
    private final File _file;
    private final long _limit;
    private long _currentOffset;

    LogIterator(File file, long fromOffset, long limit) {
      _file = file;
      _currentOffset = fromOffset;
      _limit = limit;
    }

    @Override
    public boolean hasNext() {
      // Need at least 4 bytes for the length header
      return _currentOffset + 4 <= _limit;
    }

    @Override
    public byte[] next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      try (RandomAccessFile raf = new RandomAccessFile(_file, "r")) {
        raf.seek(_currentOffset);
        int length = raf.readInt();
        if (length < 0 || _currentOffset + 4 + length > _limit) {
          throw new NoSuchElementException("Corrupt or truncated log entry at offset " + _currentOffset);
        }
        byte[] data = new byte[length];
        raf.readFully(data);
        _currentOffset += 4 + length;
        return data;
      } catch (IOException e) {
        throw new RuntimeException("Failed to read log entry at offset " + _currentOffset, e);
      }
    }
  }
}
