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
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.ColumnReaderFactory;


/**
 * {@link ColumnReaderFactory} backed by an Apache Arrow IPC file on disk.
 *
 * <p>File-specialised companion to {@link ArrowColumnReaderFactory} (which takes a caller-managed
 * reader): this class opens and owns the file, the {@link org.apache.arrow.vector.ipc.ArrowFileReader},
 * and a private allocator sized by {@link #CONFIG_ALLOCATOR_LIMIT}, all wrapped in a
 * {@link BatchedArrowFileSource}, and closes them on {@link #close}.
 *
 * <p><b>Memory model — one record batch resident.</b> See {@link BatchedArrowFileSource} for the
 * batch-bounded reading model and its read-amplification trade-off; the per-column {@link
 * BatchedArrowColumnReader}s created here share that source's single batch cursor. A pathological
 * single oversized batch is bounded by {@link #CONFIG_ALLOCATOR_LIMIT}, which throws a catchable
 * Arrow {@code OutOfMemoryException} at the ceiling rather than exhausting the heap.
 *
 * <p>{@link #getAvailableColumns()} reports the columns actually present in the Arrow source. A
 * target-schema column that is NOT present is absent from that set, and {@link
 * #getColumnReader(String)} returns {@code null} for it; supplying schema-evolution defaults for such
 * columns is the columnar build driver's responsibility.
 *
 * <p>Dictionary-encoded columns are supported; see {@link BatchedArrowFileSource} for how each batch
 * is decoded against its bound dictionary.
 *
 * <p>This class is not thread-safe. {@code @SuppressWarnings("serial")}: {@link ColumnReaderFactory}
 * is {@link java.io.Serializable} by SPI contract, but this factory holds non-serializable Arrow
 * handles and is never serialized.
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

  private transient BatchedArrowFileSource _source;
  // ColumnReader instances (one per wanted, present column) over the shared batch-bounded source.
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
    if (_source != null) {
      // Defensive: a second init() would otherwise overwrite _source and leak the prior source's
      // file handle and allocator. Release it first.
      close();
    }
    long allocatorLimit = parseAllocatorLimit(configs);
    boolean extractRawTimeValues =
        configs != null && Boolean.parseBoolean(configs.get(CONFIG_EXTRACT_RAW_TIME_VALUES));
    try {
      _source = new BatchedArrowFileSource(_dataFile, allocatorLimit, extractRawTimeValues);
      _availableColumnNames = _source.getAvailableColumns();
      Set<String> wantedColumns = ArrowAccumulators.computeWantedColumns(targetSchema, colsToRead);
      Map<String, ColumnReader> readers = new LinkedHashMap<>();
      for (String name : _availableColumnNames) {
        if (wantedColumns.isEmpty() || wantedColumns.contains(name)) {
          readers.put(name, new BatchedArrowColumnReader(_source, name));
        }
      }
      _columnReaders = readers;
      _initialized = true;
    } catch (RuntimeException | IOException e) {
      // The BatchedArrowFileSource opens the file, reader, and allocator in its constructor and
      // releases them on its own failure; if a later step here throws, close() releases the source.
      // Callers typically do not close a factory whose init() failed, so this prevents a leak.
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
    _columnReaders = null;
    _availableColumnNames = null;
    _initialized = false;
    // The source owns the file, reader, and allocator; closing it releases them (and any current
    // batch's decoded vectors). The per-column readers hold only a reference to it.
    if (_source != null) {
      try {
        _source.close();
      } finally {
        _source = null;
      }
    }
  }
}
