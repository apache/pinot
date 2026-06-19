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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.ColumnReaderFactory;


/**
 * {@link ColumnReaderFactory} backed by a caller-managed {@link ArrowReader}.
 *
 * <p>The caller supplies an already-open {@link ArrowReader} (any subclass — {@code
 * ArrowFileReader}, {@code ArrowStreamReader}, or a custom subclass that yields in-process record
 * batches) plus the {@link BufferAllocator} the reader was opened against. Per-column accumulator
 * vectors are allocated against that allocator and concatenated from the reader's batches via the
 * shared {@link ArrowAccumulators} helper, then exposed as one {@link ArrowColumnReader} per
 * accumulator.
 *
 * <p><b>Ownership:</b> the supplied {@link ArrowReader} and {@link BufferAllocator} are owned by
 * the caller and are NOT closed by this factory. Per-column accumulator vectors created during
 * {@link #init} ARE owned by the factory and are released on {@link #close}. For the file-backed
 * convenience that opens and owns its own reader and allocator, see {@link
 * ArrowFileColumnReaderFactory}.
 *
 * <p>This class is not thread-safe.
 *
 * <p>{@code @SuppressWarnings("serial")}: {@link ColumnReaderFactory} is {@link java.io.Serializable}
 * by SPI contract, but this factory holds non-serializable Arrow handles and is never serialized.
 */
@SuppressWarnings("serial")
public class ArrowColumnReaderFactory implements ColumnReaderFactory {

  /// Config key (mirrors {@link ArrowRecordExtractorConfig#EXTRACT_RAW_TIME_VALUES} on the row-major
  /// path): when `true`, temporal columns surface raw epoch values rather than canonical JDK types.
  public static final String CONFIG_EXTRACT_RAW_TIME_VALUES = ArrowRecordExtractorConfig.EXTRACT_RAW_TIME_VALUES;

  private final ArrowReader _reader;
  private final BufferAllocator _allocator;

  private transient Map<String, FieldVector> _accumulatorVectors;
  private transient Map<String, ColumnReader> _columnReaders;
  private transient Set<String> _availableColumnNames;
  private transient boolean _initialized;

  /**
   * Construct a factory reading from the given Arrow reader and allocator.
   *
   * @param reader Caller-managed Arrow reader (e.g. {@code ArrowStreamReader} over a byte buffer,
   *               or a custom reader yielding pre-populated record batches). Not closed by this
   *               factory.
   * @param allocator Caller-managed Arrow allocator used for per-column accumulator allocations.
   *                  Not closed by this factory.
   */
  public ArrowColumnReaderFactory(ArrowReader reader, BufferAllocator allocator) {
    _reader = reader;
    _allocator = allocator;
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
   * non-virtual columns from {@code targetSchema} that the Arrow source actually contains"; pass a
   * non-empty set to restrict to a subset. Allocator sizing is the caller's responsibility for this
   * factory; the only honored {@code configs} key is {@link #CONFIG_EXTRACT_RAW_TIME_VALUES}.
   */
  @Override
  public void init(Schema targetSchema, @Nullable Set<String> colsToRead, Map<String, String> configs)
      throws IOException {
    if (_initialized) {
      // Defensive: a second init() would otherwise overwrite _accumulatorVectors and leak the prior
      // init's off-heap accumulator vectors (close() would only ever release the latest set). Release
      // them first, mirroring ArrowFileColumnReaderFactory.init().
      close();
    }
    boolean extractRawTimeValues =
        configs != null && Boolean.parseBoolean(configs.get(CONFIG_EXTRACT_RAW_TIME_VALUES));
    ArrowAccumulators.Result built =
        ArrowAccumulators.populate(_reader, _allocator, targetSchema, colsToRead, extractRawTimeValues);
    _accumulatorVectors = built.getAccumulators();
    _columnReaders = built.getReaders();
    _availableColumnNames = built.getAvailableColumns();
    _initialized = true;
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
      throw new IllegalStateException("ArrowColumnReaderFactory must be initialized before use");
    }
  }

  @Override
  public void close()
      throws IOException {
    // _reader and _allocator are caller-owned; only release the accumulator vectors we created.
    IOException accumulatorCloseFailure = ArrowAccumulators.closeAll(_accumulatorVectors);
    _accumulatorVectors = null;
    _columnReaders = null;
    _availableColumnNames = null;
    _initialized = false;
    if (accumulatorCloseFailure != null) {
      throw accumulatorCloseFailure;
    }
  }
}
