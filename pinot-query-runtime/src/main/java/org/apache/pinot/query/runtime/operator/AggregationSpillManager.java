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
package org.apache.pinot.query.runtime.operator;

import com.google.common.annotations.VisibleForTesting;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SerializedDataBlock;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Manages hash-partitioned aggregation spill files for one operator. The caller must finish reading before calling
/// [#close()], which recursively removes the operator-scoped directory. This class is not thread-safe.
@SuppressWarnings("rawtypes")
class AggregationSpillManager implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregationSpillManager.class);
  private static final String SPILL_FILE_PREFIX = "partition-";
  private static final String SPILL_FILE_SUFFIX = ".spill";
  private static final String SPILL_SCOPE = "AggregationSpillManager#spill";
  private static final String RESTORE_SCOPE = "AggregationSpillManager#consumePartition";
  private static final int MAX_BUFFERED_ROWS = 1024;
  private static final int MAX_BUFFERED_PARTITIONS = 8;

  private final int _numPartitions;
  private final int _numGroupKeys;
  private final DataSchema _spillSchema;
  private final AggregationFunction[] _aggFunctions;
  private final Path _spillDirectory;
  private final Map<Integer, FileChannel> _spillWriters = new HashMap<>();
  private final ByteBuffer _recordLengthBuffer = ByteBuffer.allocate(Integer.BYTES);

  AggregationSpillManager(int numPartitions, int numGroupKeys, DataSchema spillSchema,
      AggregationFunction[] aggFunctions) {
    if (numPartitions <= 0 || numPartitions > Server.MAX_MSE_AGGREGATION_SPILL_PARTITIONS) {
      throw new IllegalArgumentException(
          "Number of spill partitions must be between 1 and " + Server.MAX_MSE_AGGREGATION_SPILL_PARTITIONS);
    }
    if (numGroupKeys < 0 || numGroupKeys > spillSchema.size()) {
      throw new IllegalArgumentException("Invalid number of group keys: " + numGroupKeys);
    }
    _numPartitions = numPartitions;
    _numGroupKeys = numGroupKeys;
    _spillSchema = spillSchema;
    _aggFunctions = aggFunctions;
    try {
      _spillDirectory = Files.createTempDirectory("pinot-aggregation-spill-");
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create aggregation spill directory", e);
    }
  }

  SpillResult spill(Iterator<Object[]> rows) {
    List<Object[]>[] partitions = createPartitions();
    boolean[] touched = new boolean[_numPartitions];
    int[] touchedPartitions = new int[_numPartitions];
    int numTouchedPartitions = 0;
    int numRows = 0;
    int numBufferedRows = 0;
    long serializedBytes = 0;
    int numRowsProcessed = 0;
    int maxBufferedRows = MAX_BUFFERED_ROWS * Math.min(_numPartitions, MAX_BUFFERED_PARTITIONS);
    while (rows.hasNext()) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(numRowsProcessed++, SPILL_SCOPE);
      Object[] row = rows.next();
      int partitionId = getPartition(row);
      List<Object[]> partition = partitions[partitionId];
      if (partition == null) {
        partition = new ArrayList<>();
        partitions[partitionId] = partition;
      }
      if (!touched[partitionId]) {
        touched[partitionId] = true;
        touchedPartitions[numTouchedPartitions++] = partitionId;
      }
      partition.add(row);
      numRows++;
      numBufferedRows++;
      if (partition.size() == MAX_BUFFERED_ROWS) {
        serializedBytes += appendPartition(partitionId, partition);
        partition.clear();
        numBufferedRows -= MAX_BUFFERED_ROWS;
      }
      if (numBufferedRows >= maxBufferedRows) {
        serializedBytes += flushPartitions(partitions, touched, touchedPartitions, numTouchedPartitions);
        numTouchedPartitions = 0;
        numBufferedRows = 0;
      }
    }
    if (numTouchedPartitions > 0) {
      serializedBytes += flushPartitions(partitions, touched, touchedPartitions, numTouchedPartitions);
    }
    return new SpillResult(numRows, serializedBytes);
  }

  boolean hasPartition(int partitionId) {
    checkPartitionId(partitionId);
    return Files.exists(getSpillFile(partitionId));
  }

  /// Consumes all records from a partition and deletes its file after the attempt, including when reading or
  /// processing fails.
  void consumePartition(int partitionId, Consumer<MseBlock.Data> consumer) {
    checkPartitionId(partitionId);
    Path spillFile = getSpillFile(partitionId);
    if (!Files.exists(spillFile)) {
      return;
    }

    RuntimeException failure = null;
    closeSpillWriter(partitionId);
    try (DataInputStream input =
        new DataInputStream(new BufferedInputStream(Files.newInputStream(spillFile)))) {
      long remainingBytes = Files.size(spillFile);
      int numRecordsRead = 0;
      while (remainingBytes > 0) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(numRecordsRead++, RESTORE_SCOPE);
        if (remainingBytes < Integer.BYTES) {
          throw new IOException("Truncated spill record length in: " + spillFile);
        }
        int recordLength = input.readInt();
        remainingBytes -= Integer.BYTES;
        if (recordLength < 0 || recordLength > remainingBytes) {
          throw new IOException("Invalid spill record length " + recordLength + " in: " + spillFile);
        }
        byte[] bytes = input.readNBytes(recordLength);
        if (bytes.length != recordLength) {
          throw new IOException("Truncated spill record in: " + spillFile);
        }
        remainingBytes -= recordLength;
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        DataBlock dataBlock = DataBlockUtils.readFrom(buffer);
        if (buffer.hasRemaining()) {
          throw new IOException("Trailing bytes in aggregation spill record: " + spillFile);
        }
        consumer.accept(new SerializedDataBlock(dataBlock));
      }
    } catch (IOException e) {
      failure = new UncheckedIOException("Failed to read aggregation spill partition: " + partitionId, e);
      throw failure;
    } catch (RuntimeException e) {
      failure = e;
      throw e;
    } finally {
      try {
        deleteSpillFile(spillFile);
      } catch (RuntimeException e) {
        if (failure != null) {
          failure.addSuppressed(e);
        } else {
          LOGGER.warn("Failed to delete consumed aggregation spill partition; close will retry: {}", spillFile, e);
        }
      }
    }
  }

  int getNumPartitions() {
    return _numPartitions;
  }

  Path getSpillDirectory() {
    return _spillDirectory;
  }

  @VisibleForTesting
  int getNumOpenSpillWriters() {
    return _spillWriters.size();
  }

  @Override
  public void close() {
    if (!Files.exists(_spillDirectory)) {
      return;
    }
    RuntimeException failure = null;
    try {
      closeSpillWriters();
    } catch (RuntimeException e) {
      failure = e;
    }
    try {
      Files.walkFileTree(_spillDirectory, new SimpleFileVisitor<>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
            throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path directory, IOException exception)
            throws IOException {
          if (exception != null) {
            throw exception;
          }
          Files.delete(directory);
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (IOException e) {
      RuntimeException cleanupFailure =
          new UncheckedIOException("Failed to delete aggregation spill directory", e);
      if (failure != null) {
        failure.addSuppressed(cleanupFailure);
      } else {
        failure = cleanupFailure;
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  @SuppressWarnings("unchecked")
  private List<Object[]>[] createPartitions() {
    return new List[_numPartitions];
  }

  private long flushPartitions(List<Object[]>[] partitions, boolean[] touched, int[] touchedPartitions,
      int numTouchedPartitions) {
    long serializedBytes = 0;
    for (int i = 0; i < numTouchedPartitions; i++) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(i, SPILL_SCOPE);
      int partitionId = touchedPartitions[i];
      List<Object[]> partition = partitions[partitionId];
      if (!partition.isEmpty()) {
        serializedBytes += appendPartition(partitionId, partition);
        partition.clear();
      }
      touched[partitionId] = false;
    }
    return serializedBytes;
  }

  private int getPartition(Object[] row) {
    int hash = 1;
    for (int i = 0; i < _numGroupKeys; i++) {
      hash = 31 * hash + deepHashCode(row[i]);
    }
    return Math.floorMod(hash, _numPartitions);
  }

  @VisibleForTesting
  static int deepHashCode(Object value) {
    if (value instanceof Object[]) {
      return Arrays.deepHashCode((Object[]) value);
    }
    if (value instanceof byte[]) {
      return Arrays.hashCode((byte[]) value);
    }
    if (value instanceof short[]) {
      return Arrays.hashCode((short[]) value);
    }
    if (value instanceof int[]) {
      return Arrays.hashCode((int[]) value);
    }
    if (value instanceof long[]) {
      return Arrays.hashCode((long[]) value);
    }
    if (value instanceof char[]) {
      return Arrays.hashCode((char[]) value);
    }
    if (value instanceof float[]) {
      return Arrays.hashCode((float[]) value);
    }
    if (value instanceof double[]) {
      return Arrays.hashCode((double[]) value);
    }
    if (value instanceof boolean[]) {
      return Arrays.hashCode((boolean[]) value);
    }
    return value != null ? value.hashCode() : 0;
  }

  private long appendPartition(int partitionId, List<Object[]> rows) {
    DataBlock dataBlock = new RowHeapDataBlock(rows, _spillSchema, _aggFunctions).asSerialized().getDataBlock();
    try {
      List<ByteBuffer> buffers = dataBlock.serialize();
      int recordLength = getRecordLength(buffers);
      FileChannel output = getSpillWriter(partitionId);
      _recordLengthBuffer.clear();
      _recordLengthBuffer.putInt(recordLength).flip();
      writeFully(output, _recordLengthBuffer);
      for (ByteBuffer buffer : buffers) {
        writeFully(output, buffer);
      }
      return Integer.BYTES + (long) recordLength;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to spill aggregation partition: " + partitionId, e);
    }
  }

  private static int getRecordLength(List<ByteBuffer> buffers) {
    long length = 0;
    for (ByteBuffer buffer : buffers) {
      length += buffer.remaining();
    }
    if (length > Integer.MAX_VALUE) {
      throw new IllegalStateException("Aggregation spill record exceeds maximum length: " + length);
    }
    return (int) length;
  }

  private FileChannel getSpillWriter(int partitionId)
      throws IOException {
    FileChannel writer = _spillWriters.get(partitionId);
    if (writer != null) {
      return writer;
    }
    writer = FileChannel.open(getSpillFile(partitionId), StandardOpenOption.CREATE, StandardOpenOption.WRITE,
        StandardOpenOption.APPEND);
    _spillWriters.put(partitionId, writer);
    return writer;
  }

  private static void writeFully(FileChannel output, ByteBuffer buffer)
      throws IOException {
    while (buffer.hasRemaining()) {
      output.write(buffer);
    }
  }

  private void closeSpillWriter(int partitionId) {
    FileChannel writer = _spillWriters.remove(partitionId);
    if (writer != null) {
      try {
        writer.close();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close aggregation spill partition: " + partitionId, e);
      }
    }
  }

  private void closeSpillWriters() {
    IOException failure = null;
    for (FileChannel writer : _spillWriters.values()) {
      try {
        writer.close();
      } catch (IOException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    _spillWriters.clear();
    if (failure != null) {
      throw new UncheckedIOException("Failed to close aggregation spill files", failure);
    }
  }

  private void checkPartitionId(int partitionId) {
    if (partitionId < 0 || partitionId >= _numPartitions) {
      throw new IllegalArgumentException("Invalid spill partition id: " + partitionId);
    }
  }

  private Path getSpillFile(int partitionId) {
    return _spillDirectory.resolve(SPILL_FILE_PREFIX + partitionId + SPILL_FILE_SUFFIX);
  }

  void deleteSpillFile(Path spillFile) {
    try {
      Files.deleteIfExists(spillFile);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to delete aggregation spill file: " + spillFile, e);
    }
  }

  static final class SpillResult {
    private final int _rows;
    private final long _serializedBytes;

    SpillResult(int rows, long serializedBytes) {
      _rows = rows;
      _serializedBytes = serializedBytes;
    }

    int getRows() {
      return _rows;
    }

    long getBytes() {
      return _serializedBytes;
    }
  }
}
