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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.util.VectorAppender;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReader;


/**
 * Package-private helper shared by {@link ArrowColumnReaderFactory} and
 * {@link InMemoryArrowColumnReaderFactory}: walks every record batch in an {@link ArrowReader},
 * concatenates each wanted column's values into a per-column accumulator {@link FieldVector} via
 * Arrow's {@link VectorAppender}, and produces one {@link ArrowColumnReader} per accumulator.
 *
 * <p>Accumulator vectors are allocated against the caller-supplied {@link BufferAllocator}. The
 * caller (factory) owns and closes them via {@link Result#getAccumulators()}; this helper does
 * not retain references.
 */
final class ArrowAccumulators {

  private ArrowAccumulators() {
  }

  static Result populate(ArrowReader reader, BufferAllocator allocator, Schema targetSchema,
      @Nullable Set<String> colsToRead)
      throws IOException {
    Set<String> wantedColumns = computeWantedColumns(targetSchema, colsToRead);

    VectorSchemaRoot perBatchRoot = reader.getVectorSchemaRoot();
    Set<String> availableColumns = Collections.unmodifiableSet(collectAvailableNames(perBatchRoot));

    Map<String, FieldVector> accumulators = new LinkedHashMap<>();
    Map<String, VectorAppender> appenders = new LinkedHashMap<>();
    for (FieldVector source : perBatchRoot.getFieldVectors()) {
      String name = source.getField().getName();
      if (!wantedColumns.isEmpty() && !wantedColumns.contains(name)) {
        continue;
      }
      // Dictionary-encoded columns surface as their index type (e.g. Int32) rather than the
      // decoded logical type. Reject loudly so we don't silently produce a wrong segment. The
      // row-major ArrowRecordExtractor decodes via DictionaryEncoder.decode; adding the same
      // here is left as a follow-up once a real use case appears.
      Preconditions.checkArgument(source.getField().getDictionary() == null,
          "Dictionary-encoded Arrow column '%s' is not supported by Arrow column-major build. "
              + "Use ArrowRecordReader (row-major) for files containing dictionary-encoded columns.",
          name);
      FieldVector accumulator = source.getField().createVector(allocator);
      // Pre-allocate buffers so VectorAppender can read offsets / validity from them on the
      // first append (otherwise BaseVariableWidthVector visits IOOBE on an empty offset buffer).
      accumulator.allocateNew();
      accumulators.put(name, accumulator);
      appenders.put(name, new VectorAppender(accumulator));
    }

    // Walk every record batch and bulk-append each wanted column into its accumulator via
    // Arrow's VectorAppender (Visitor-based; grows offset / data buffers once per batch and
    // bulk-copies, rather than per-row copyValueSafe). Single-batch and multi-batch inputs go
    // through the same code path — VectorAppender handles either correctly.
    boolean anyBatchSeen = false;
    while (reader.loadNextBatch()) {
      if (perBatchRoot.getRowCount() == 0) {
        continue;
      }
      anyBatchSeen = true;
      for (FieldVector source : perBatchRoot.getFieldVectors()) {
        VectorAppender appender = appenders.get(source.getField().getName());
        if (appender != null) {
          source.accept(appender, null);
        }
      }
    }
    if (!anyBatchSeen) {
      throw new IOException("Arrow source contains no non-empty record batches");
    }

    Map<String, ColumnReader> readers = new LinkedHashMap<>();
    for (Map.Entry<String, FieldVector> entry : accumulators.entrySet()) {
      readers.put(entry.getKey(), new ArrowColumnReader(entry.getKey(), entry.getValue()));
    }

    return new Result(accumulators, readers, availableColumns);
  }

  private static Set<String> computeWantedColumns(Schema targetSchema, @Nullable Set<String> colsToRead) {
    if (colsToRead != null && !colsToRead.isEmpty()) {
      return new HashSet<>(colsToRead);
    }
    Set<String> targetColumns = new HashSet<>();
    for (FieldSpec fieldSpec : targetSchema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn()) {
        targetColumns.add(fieldSpec.getName());
      }
    }
    return targetColumns;
  }

  private static Set<String> collectAvailableNames(VectorSchemaRoot root) {
    Set<String> names = new HashSet<>();
    for (FieldVector vector : root.getFieldVectors()) {
      names.add(vector.getField().getName());
    }
    return names;
  }

  /**
   * Close each accumulator vector, accumulating the first failure as an {@link IOException}.
   * Used by both factory {@code close()} paths so the per-vector close loop lives once.
   *
   * @param accumulators per-column accumulator vectors to close; may be {@code null}
   * @return the first close failure encountered, or {@code null} if all closes succeeded
   */
  @Nullable
  static IOException closeAll(@Nullable Map<String, FieldVector> accumulators) {
    if (accumulators == null) {
      return null;
    }
    IOException firstException = null;
    for (FieldVector vector : accumulators.values()) {
      try {
        vector.close();
      } catch (Exception e) {
        if (firstException == null) {
          firstException = new IOException("Failed to close Arrow accumulator vector", e);
        }
      }
    }
    return firstException;
  }

  static final class Result {
    private final Map<String, FieldVector> _accumulators;
    private final Map<String, ColumnReader> _readers;
    private final Set<String> _availableColumns;

    Result(Map<String, FieldVector> accumulators, Map<String, ColumnReader> readers,
        Set<String> availableColumns) {
      _accumulators = accumulators;
      _readers = readers;
      _availableColumns = availableColumns;
    }

    Map<String, FieldVector> getAccumulators() {
      return _accumulators;
    }

    Map<String, ColumnReader> getReaders() {
      return _readers;
    }

    Set<String> getAvailableColumns() {
      return _availableColumns;
    }
  }
}
