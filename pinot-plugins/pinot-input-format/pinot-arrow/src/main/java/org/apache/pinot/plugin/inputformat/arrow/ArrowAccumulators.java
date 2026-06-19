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
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.VectorAppender;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReader;


/**
 * Package-private helper shared by {@link ArrowColumnReaderFactory} and
 * {@link ArrowFileColumnReaderFactory}: walks every record batch in an {@link ArrowReader},
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
      @Nullable Set<String> colsToRead, boolean extractRawTimeValues)
      throws IOException {
    Set<String> wantedColumns = computeWantedColumns(targetSchema, colsToRead);

    VectorSchemaRoot perBatchRoot = reader.getVectorSchemaRoot();
    Set<String> availableColumns = Collections.unmodifiableSet(collectAvailableNames(perBatchRoot));
    // Bound dictionaries, resolved up front (available once the reader's schema/footer is read, before
    // any record batch). Used to decode dictionary-encoded columns per batch, mirroring the row-major
    // ArrowRecordExtractor (setReader + prepareBatch); empty when no column is dictionary-encoded.
    Map<Long, Dictionary> dictionaries = reader.getDictionaryVectors();

    // Nothing else holds these accumulators until we return the Result, so on any failure after the
    // first allocateNew() we must release them here (see the catch below); otherwise the off-heap
    // vectors leak and the allocator's own close() trips on the outstanding allocation.
    Map<String, FieldVector> accumulators = new LinkedHashMap<>();
    try {
      Map<String, VectorAppender> appenders = new LinkedHashMap<>();
      for (FieldVector source : perBatchRoot.getFieldVectors()) {
        String name = source.getField().getName();
        if (!wantedColumns.isEmpty() && !wantedColumns.contains(name)) {
          continue;
        }
        // Dictionary-encoded columns (the standard Arrow representation for low-cardinality strings)
        // surface as their index type (e.g. Int32) rather than the decoded logical type. The
        // accumulator therefore holds the DECODED logical values, typed from the bound dictionary's
        // value vector; each batch is decoded via DictionaryEncoder.decode before append, mirroring the
        // row-major ArrowRecordExtractor. Non-dictionary columns accumulate the source type directly.
        DictionaryEncoding encoding = source.getField().getDictionary();
        FieldVector accumulator;
        if (encoding == null) {
          accumulator = source.getField().createVector(allocator);
        } else {
          Dictionary dictionary = dictionaries.get(encoding.getId());
          Preconditions.checkArgument(dictionary != null,
              "Arrow column '%s' is dictionary-encoded (dictionary id %s) but no matching dictionary is "
                  + "present in the source", name, encoding.getId());
          Field valueField = dictionary.getVector().getField();
          accumulator = new Field(name, valueField.getFieldType(), valueField.getChildren())
              .createVector(allocator);
        }
        // Pre-allocate buffers so VectorAppender can read offsets / validity from them on the
        // first append (otherwise BaseVariableWidthVector visits IOOBE on an empty offset buffer).
        accumulator.allocateNew();
        accumulators.put(name, accumulator);
        appenders.put(name, new VectorAppender(accumulator));
      }

      // Walk every record batch and bulk-append each wanted column into its accumulator via
      // Arrow's VectorAppender (Visitor-based; grows offset / data buffers once per batch and
      // bulk-copies, rather than per-row copyValueSafe). Single-batch and multi-batch inputs go
      // through the same code path — VectorAppender handles either correctly. An input with no
      // non-empty batches yields zero-doc accumulators, mirroring the row-major path's handling of
      // an empty source (a zero-doc segment) rather than throwing.
      while (reader.loadNextBatch()) {
        if (perBatchRoot.getRowCount() == 0) {
          continue;
        }
        for (FieldVector source : perBatchRoot.getFieldVectors()) {
          VectorAppender appender = appenders.get(source.getField().getName());
          if (appender == null) {
            continue;
          }
          DictionaryEncoding encoding = source.getField().getDictionary();
          if (encoding == null) {
            source.accept(appender, null);
          } else {
            // Decode this batch's indices into logical values, append, then release the transient
            // decoded copy (the accumulator retains its own values via the appender's bulk-copy).
            try (FieldVector decoded =
                (FieldVector) DictionaryEncoder.decode(source, dictionaries.get(encoding.getId()))) {
              decoded.accept(appender, null);
            }
          }
        }
      }

      Map<String, ColumnReader> readers = new LinkedHashMap<>();
      for (Map.Entry<String, FieldVector> entry : accumulators.entrySet()) {
        readers.put(entry.getKey(),
            new ArrowColumnReader(entry.getKey(), entry.getValue(), extractRawTimeValues));
      }

      return new Result(accumulators, readers, availableColumns);
    } catch (RuntimeException | IOException e) {
      IOException closeFailure = closeAll(accumulators);
      if (closeFailure != null) {
        e.addSuppressed(closeFailure);
      }
      throw e;
    }
  }

  /**
   * Resolve the set of columns to read: an explicit non-empty {@code colsToRead}, otherwise every
   * non-virtual column in {@code targetSchema}. Shared by both columnar factories (materialized and
   * file-backed) so the selection rule lives in one place.
   */
  static Set<String> computeWantedColumns(Schema targetSchema, @Nullable Set<String> colsToRead) {
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
