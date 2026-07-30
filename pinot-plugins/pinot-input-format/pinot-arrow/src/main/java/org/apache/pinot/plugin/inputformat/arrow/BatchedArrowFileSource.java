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
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;


/**
 * Reads an Apache Arrow IPC file <b>one record batch at a time</b> through the seekable
 * {@link ArrowFileReader} ({@link ArrowFileReader#getRecordBlocks()} +
 * {@link ArrowFileReader#loadRecordBatch}), so peak resident off-heap memory is a single record
 * batch rather than the whole materialised column set. Shared by the per-column
 * {@link BatchedArrowColumnReader}s created over it: at most one batch is loaded at any instant.
 *
 * <p>Peak memory therefore equals the largest record batch, which is a write-time, producer-chosen
 * property — the same memory model as the row-major {@link ArrowRecordReader}. Reading a column to
 * completion re-loads every batch (Arrow Java loads whole batches), so the column-major consumer
 * pays {@code N x} batch loads across {@code N} columns; this read amplification is the accepted
 * cost of Arrow's horizontal record-batch layout. Dictionary-encoded columns are decoded per batch
 * (mirroring the row-major {@link ArrowRecordExtractor}); the transient decoded vectors are released
 * when the next batch loads.
 *
 * <p>This class is not thread-safe.
 */
final class BatchedArrowFileSource implements Closeable {

  private final RootAllocator _allocator;
  private final FileInputStream _fileInputStream;
  private final ArrowFileReader _reader;
  private final VectorSchemaRoot _root;
  private final List<ArrowBlock> _blocks;
  private final Map<Long, Dictionary> _dictionaries;
  private final Set<String> _availableColumns;
  private final boolean _extractRawTimeValues;
  // Cumulative first global docId of each batch; length numBatches + 1, last entry == total docs.
  private final int[] _batchStartDoc;
  private final int _totalDocs;

  private int _loadedBatchIdx = -1;
  // Decoded vectors for the currently-loaded batch, keyed by column; closed when the next batch loads.
  private final Map<String, FieldVector> _decodedCache = new HashMap<>();

  BatchedArrowFileSource(File dataFile, long allocatorLimit, boolean extractRawTimeValues)
      throws IOException {
    _allocator = new RootAllocator(allocatorLimit);
    _extractRawTimeValues = extractRawTimeValues;
    boolean initialized = false;
    try {
      _fileInputStream = new FileInputStream(dataFile);
      _reader = new ArrowFileReader(_fileInputStream.getChannel(), _allocator);
      _root = _reader.getVectorSchemaRoot();
      _blocks = _reader.getRecordBlocks();
      _dictionaries = _reader.getDictionaryVectors();

      Set<String> names = new LinkedHashSet<>();
      for (FieldVector vector : _root.getFieldVectors()) {
        names.add(vector.getField().getName());
      }
      _availableColumns = Collections.unmodifiableSet(names);

      // One up-front pass over the batch footers to learn each batch's row count, giving total docs
      // and the per-batch global docId offsets. This loads one batch at a time (one-batch-resident);
      // its cost is amortised into the column-major consumer's per-column re-reads.
      _batchStartDoc = new int[_blocks.size() + 1];
      int total = 0;
      for (int i = 0; i < _blocks.size(); i++) {
        _batchStartDoc[i] = total;
        _reader.loadRecordBatch(_blocks.get(i));
        total += _root.getRowCount();
      }
      _batchStartDoc[_blocks.size()] = total;
      _totalDocs = total;
      // The footer pass above left the last batch resident; record it so the first columnVector()
      // call can skip a reload when it targets that batch.
      _loadedBatchIdx = _blocks.isEmpty() ? -1 : _blocks.size() - 1;
      initialized = true;
    } finally {
      if (!initialized) {
        closeQuietly();
      }
    }
  }

  int getTotalDocs() {
    return _totalDocs;
  }

  Set<String> getAvailableColumns() {
    return _availableColumns;
  }

  boolean hasColumn(String columnName) {
    return _availableColumns.contains(columnName);
  }

  boolean extractRawTimeValues() {
    return _extractRawTimeValues;
  }

  int batchStartDoc(int batchIdx) {
    return _batchStartDoc[batchIdx];
  }

  /** The batch index currently resident, or -1 if none. A column reader's cached per-batch delegate
   * is valid only while this equals the batch it was built for (the shared cursor may have moved). */
  int loadedBatchIdx() {
    return _loadedBatchIdx;
  }

  /** Index of the batch owning {@code docId} (binary search over the cumulative offsets). */
  int batchIndexForDoc(int docId) {
    int lo = 0;
    int hi = _blocks.size() - 1;
    while (lo < hi) {
      int mid = (lo + hi + 1) >>> 1;
      if (_batchStartDoc[mid] <= docId) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }
    return lo;
  }

  /**
   * The effective Arrow {@link Field} of a column — the decoded value field for dictionary-encoded
   * columns, the raw field otherwise. Used for type predicates without loading any batch data.
   */
  Field effectiveField(String columnName) {
    FieldVector source = _root.getVector(columnName);
    Dictionary dictionary = resolveDictionary(source);
    if (dictionary == null) {
      return source.getField();
    }
    Field valueField = dictionary.getVector().getField();
    return new Field(columnName, valueField.getFieldType(), valueField.getChildren());
  }

  /**
   * Ensure {@code batchIdx} is the loaded batch, then return the column's {@link FieldVector} for it
   * (decoded if dictionary-encoded). The returned vector is valid only until the next batch loads.
   */
  FieldVector columnVector(String columnName, int batchIdx)
      throws IOException {
    ensureBatchLoaded(batchIdx);
    FieldVector cached = _decodedCache.get(columnName);
    if (cached != null) {
      return cached;
    }
    FieldVector source = _root.getVector(columnName);
    Dictionary dictionary = resolveDictionary(source);
    if (dictionary == null) {
      // Raw vector owned by the VectorSchemaRoot; refilled in place on the next loadRecordBatch.
      return source;
    }
    FieldVector decoded = (FieldVector) DictionaryEncoder.decode(source, dictionary);
    _decodedCache.put(columnName, decoded);
    return decoded;
  }

  /**
   * The bound dictionary for {@code source} if it is dictionary-encoded, or {@code null} otherwise.
   * Throws if the column is dictionary-encoded but its dictionary is missing from the source.
   */
  @Nullable
  private Dictionary resolveDictionary(FieldVector source) {
    DictionaryEncoding encoding = source.getField().getDictionary();
    if (encoding == null) {
      return null;
    }
    Dictionary dictionary = _dictionaries.get(encoding.getId());
    Preconditions.checkArgument(dictionary != null,
        "Arrow column '%s' is dictionary-encoded (dictionary id %s) but no matching dictionary is present",
        source.getField().getName(), encoding.getId());
    return dictionary;
  }

  private void ensureBatchLoaded(int batchIdx)
      throws IOException {
    if (_loadedBatchIdx == batchIdx) {
      return;
    }
    closeDecoded();
    _reader.loadRecordBatch(_blocks.get(batchIdx));
    _loadedBatchIdx = batchIdx;
  }

  private void closeDecoded() {
    for (FieldVector vector : _decodedCache.values()) {
      try {
        vector.close();
      } catch (Exception ignored) {
        // best effort; the allocator close below is the backstop
      }
    }
    _decodedCache.clear();
  }

  @Override
  public void close()
      throws IOException {
    IOException firstException = null;
    closeDecoded();
    if (_reader != null) {
      try {
        _reader.close();
      } catch (IOException e) {
        firstException = e;
      }
    }
    if (_fileInputStream != null) {
      try {
        _fileInputStream.close();
      } catch (IOException e) {
        if (firstException == null) {
          firstException = e;
        }
      }
    }
    if (_allocator != null) {
      try {
        _allocator.close();
      } catch (Exception e) {
        if (firstException == null) {
          firstException = new IOException("Failed to close Arrow allocator", e);
        }
      }
    }
    if (firstException != null) {
      throw firstException;
    }
  }

  private void closeQuietly() {
    try {
      close();
    } catch (IOException ignored) {
      // constructor cleanup path
    }
  }
}
