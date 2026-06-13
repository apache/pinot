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
package org.apache.pinot.plugin.inputformat.arrow;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.ColumnReaderFactory;


/**
 * {@link ColumnReaderFactory} backed by an Apache Arrow IPC file on disk.
 *
 * <p>File-specialised convenience over {@link ArrowColumnReaderFactory}: this class opens a private
 * {@link RootAllocator} sized by {@link #CONFIG_ALLOCATOR_LIMIT}, opens the file via
 * {@link ArrowFileReader}, concatenates all record batches into per-column accumulators via the
 * shared {@link ArrowAccumulators} helper, and closes the file, reader, and allocator on
 * {@link #close}. Callers that already manage an {@link org.apache.arrow.vector.ipc.ArrowReader}
 * and {@link org.apache.arrow.memory.BufferAllocator} themselves should use
 * {@link ArrowColumnReaderFactory} directly.
 *
 * <p>{@link #getAvailableColumns()} reports the columns actually present in the Arrow source. A
 * target-schema column that is NOT present is absent from that set, and {@link
 * #getColumnReader(String)} returns {@code null} for it; supplying schema-evolution defaults for
 * such columns is the columnar build driver's responsibility.
 *
 * <p><b>Dictionary-encoded columns are supported</b> (dictionary encoding is the standard Arrow
 * representation for low-cardinality strings): the shared {@link ArrowAccumulators} decodes each
 * batch against the bound dictionary via {@code DictionaryEncoder.decode} — mirroring the row-major
 * {@link ArrowRecordExtractor} — and accumulates the decoded logical values, so the produced segment
 * matches the row-major path for the same file.
 *
 * <p>This class is not thread-safe. For very large inputs the per-column accumulators materialise
 * the full column set in the Arrow allocator; partition upstream so each factory instance handles
 * one shard.
 *
 * <p>{@code @SuppressWarnings("serial")}: {@link ColumnReaderFactory} extends {@link
 * java.io.Serializable} by SPI contract, but this factory holds non-serializable Arrow handles and
 * is never actually serialized — it exists only for the duration of a columnar segment build.
 */
@SuppressWarnings("serial")
public class ArrowFileColumnReaderFactory implements ColumnReaderFactory {

  /// Default allocator limit when no `configs` override is supplied. Matches
  /// {@link ArrowRecordReaderConfig#DEFAULT_ALLOCATOR_LIMIT} so users get the same
  /// memory ceiling whether they pick the row-major or column-major reader path.
  public static final String CONFIG_ALLOCATOR_LIMIT = "arrowAllocatorLimit";
  public static final long DEFAULT_ALLOCATOR_LIMIT = ArrowRecordReaderConfig.DEFAULT_ALLOCATOR_LIMIT;

  /// Config key (mirrors {@link ArrowRecordExtractorConfig#EXTRACT_RAW_TIME_VALUES} on the row-major
  /// path): when `true`, temporal columns surface raw epoch values rather than canonical JDK types.
  public static final String CONFIG_EXTRACT_RAW_TIME_VALUES = ArrowRecordExtractorConfig.EXTRACT_RAW_TIME_VALUES;

  private final File _dataFile;

  private transient RootAllocator _allocator;
  private transient FileInputStream _fileInputStream;
  private transient ArrowFileReader _arrowFileReader;
  // Per-column accumulator vectors holding values concatenated across all input batches.
  // Owned by this factory; released in close() before the allocator.
  private transient Map<String, FieldVector> _accumulatorVectors;
  // Cached ColumnReader instances keyed by Pinot column name.
  private transient Map<String, ColumnReader> _columnReaders;
  private transient Set<String> _availableColumnNames;
  private transient boolean _initialized;

  /**
   * Construct a factory reading from the given Arrow IPC file.
   *
   * @param dataFile Path to the Arrow IPC file to read
   */
  public ArrowFileColumnReaderFactory(File dataFile) {
    _dataFile = dataFile;
  }

  @Override
  public void init(Schema targetSchema)
      throws IOException {
    init(targetSchema, null, Collections.emptyMap());
  }

  @Override
  public void init(Schema targetSchema, Set<String> colsToRead)
      throws IOException {
    init(targetSchema, colsToRead, Collections.emptyMap());
  }

  /**
   * Initialise the factory. {@code colsToRead == null} or an empty set both mean "read all
   * non-virtual columns from {@code targetSchema} that the Arrow file actually contains"; pass a
   * non-empty set to restrict to a subset.
   */
  @Override
  public void init(Schema targetSchema, @Nullable Set<String> colsToRead, Map<String, String> configs)
      throws IOException {
    long allocatorLimit = parseAllocatorLimit(configs);
    boolean extractRawTimeValues =
        configs != null && Boolean.parseBoolean(configs.get(CONFIG_EXTRACT_RAW_TIME_VALUES));
    try {
      _allocator = new RootAllocator(allocatorLimit);
      _fileInputStream = new FileInputStream(_dataFile);
      _arrowFileReader = new ArrowFileReader(_fileInputStream.getChannel(), _allocator);

      ArrowAccumulators.Result built = ArrowAccumulators.populate(_arrowFileReader, _allocator, targetSchema,
          colsToRead, extractRawTimeValues);
      _accumulatorVectors = built.getAccumulators();
      _columnReaders = built.getReaders();
      _availableColumnNames = built.getAvailableColumns();

      _initialized = true;
    } catch (RuntimeException | IOException e) {
      // init() opens the allocator, file stream, and reader before populate() runs. If anything
      // throws part-way, release whatever was opened — callers typically do not close a factory
      // whose init() failed, so without this the file handle, ArrowFileReader, and RootAllocator
      // would leak. close() is null-safe over the partially-assigned fields.
      try {
        close();
      } catch (IOException closeFailure) {
        e.addSuppressed(closeFailure);
      }
      throw e;
    }
  }

  private long parseAllocatorLimit(Map<String, String> configs) {
    if (configs == null) {
      return DEFAULT_ALLOCATOR_LIMIT;
    }
    String raw = configs.get(CONFIG_ALLOCATOR_LIMIT);
    if (raw == null) {
      return DEFAULT_ALLOCATOR_LIMIT;
    }
    try {
      return Long.parseLong(raw);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid value '" + raw + "' for config '" + CONFIG_ALLOCATOR_LIMIT + "': expected a long",
          e);
    }
  }

  @Override
  public Set<String> getAvailableColumns() {
    requireInitialized();
    return _availableColumnNames;
  }

  @Override
  @Nullable
  public ColumnReader getColumnReader(String columnName) {
    requireInitialized();
    return _columnReaders.get(columnName);
  }

  @Override
  public Map<String, ColumnReader> getAllColumnReaders() {
    requireInitialized();
    return Collections.unmodifiableMap(_columnReaders);
  }

  private void requireInitialized() {
    if (!_initialized) {
      throw new IllegalStateException("ArrowFileColumnReaderFactory must be initialized before use");
    }
  }

  @Override
  public void close()
      throws IOException {
    // Close ordering: accumulator vectors first, then the file resources we opened (reader,
    // stream, allocator). Each step's first failure is preserved and re-thrown at the end so
    // later steps still run.
    IOException firstException = ArrowAccumulators.closeAll(_accumulatorVectors);
    _accumulatorVectors = null;

    if (_arrowFileReader != null) {
      try {
        _arrowFileReader.close();
      } catch (IOException e) {
        if (firstException == null) {
          firstException = e;
        }
      } finally {
        _arrowFileReader = null;
      }
    }

    if (_fileInputStream != null) {
      try {
        _fileInputStream.close();
      } catch (IOException e) {
        if (firstException == null) {
          firstException = e;
        }
      } finally {
        _fileInputStream = null;
      }
    }

    if (_allocator != null) {
      try {
        _allocator.close();
      } catch (Exception e) {
        if (firstException == null) {
          firstException = new IOException("Failed to close Arrow allocator", e);
        }
      } finally {
        _allocator = null;
      }
    }

    _columnReaders = null;
    _availableColumnNames = null;
    _initialized = false;

    if (firstException != null) {
      throw firstException;
    }
  }
}
