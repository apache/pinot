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
package org.apache.pinot.segment.local.segment.index.column;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.readers.text.MultiColumnLuceneTextIndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Index readers of one physical column of an immutable segment.
///
/// Readers are keyed by the numeric index id assigned by [IndexService#getNumericId(IndexType)] and stored as a
/// presence bit mask plus a dense array holding only the readers that exist, ordered by id. A lookup is a shift, a
/// mask and a popcount, so it stays O(1) on the query path while the per-column footprint is exactly one array slot
/// per present reader. A server holding tens of thousands of wide segments creates one of these per (segment, column),
/// which is why the layout matters: the mask replaces a nested map object and a reader array spanning the whole
/// numeric-id range of the present readers.
///
/// The mask is one `long`, so it covers at most 64 index types. [IndexService#MAX_INDEX_TYPES] states that limit in
/// the index SPI and [IndexService] enforces it when it is built; the two must change together.
///
/// Thread safety: immutable after construction except for the multi-column text reader reference, which is set
/// once during segment load and cleared on [#close()].
public final class PhysicalColumnIndexContainer implements ColumnIndexContainer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalColumnIndexContainer.class);

  private static final Set<String> FORWARD_INDEX_ONLY_TYPES =
      Set.of(StandardIndexes.FORWARD_ID, StandardIndexes.DICTIONARY_ID, StandardIndexes.NULL_VALUE_VECTOR_ID);
  private static final IndexReader[] EMPTY_READERS = new IndexReader[0];

  // Bit i is set when the index type with numeric id i has a reader in this column. IndexService.MAX_INDEX_TYPES keeps
  // numeric ids below 64 and is tied to this mask: widening the mask is what raises that SPI limit.
  private final long _presentMask;
  // Readers ordered by numeric index id, holding only the present ones: the reader for id i sits at the number of
  // bits set in _presentMask below bit i.
  private final IndexReader[] _readers;
  @Nullable
  private final VectorIndexConfig _vectorIndexConfig;

  // Reference to shared segment-level multi-column text index reader.
  // This reader is closed on segment destroy() and not in this class' close() method.
  private MultiColumnLuceneTextIndexReader _multiColTextReader;

  public PhysicalColumnIndexContainer(SegmentDirectory.Reader segmentReader, ColumnMetadata metadata,
      IndexLoadingConfig indexLoadingConfig)
      throws IOException {
    this(segmentReader, metadata, indexLoadingConfig.getFieldIndexConfig(metadata.getColumnName()),
        indexLoadingConfig.isForwardIndexOnly());
  }

  /// Creates the container from the column's own index configs (`null` meaning none, i.e. every index type at its
  /// default) and the forward-index-only flag, without a reference to the loading config they were taken from.
  public PhysicalColumnIndexContainer(SegmentDirectory.Reader segmentReader, ColumnMetadata metadata,
      @Nullable FieldIndexConfigs fieldIndexConfigs, boolean forwardIndexOnly)
      throws IOException {
    String columnName = metadata.getColumnName();

    if (fieldIndexConfigs == null) {
      fieldIndexConfigs = FieldIndexConfigs.EMPTY;
    }
    _vectorIndexConfig = fieldIndexConfigs.getConfig(StandardIndexes.vector());

    IndexService indexService = IndexService.getInstance();
    List<IndexType<?, ?, ?>> allIndexes = indexService.getAllIndexes();

    // Scratch array indexed by numeric id; compacted into the exactly-sized _readers below. Numeric ids are dense in
    // [0, allIndexes.size()), and IndexService caps that size at IndexService.MAX_INDEX_TYPES, the width of the mask.
    IndexReader[] readersById = new IndexReader[allIndexes.size()];
    long presentMask = 0L;
    try {
      for (IndexType<?, ?, ?> indexType : allIndexes) {
        if (forwardIndexOnly && !FORWARD_INDEX_ONLY_TYPES.contains(indexType.getId())) {
          continue;
        }
        if (segmentReader.hasIndexFor(columnName, indexType)) {
          // Resolve the id the same way getIndex() does, and before creating the reader, so that nothing can throw
          // between creating a reader and recording it for cleanup.
          short indexId = indexService.getNumericId(indexType);
          IndexReaderFactory<?> readerProvider = indexType.getReaderFactory();
          try {
            IndexReader reader = readerProvider.createIndexReader(segmentReader, fieldIndexConfigs, metadata);
            if (reader != null) {
              readersById[indexId] = reader;
              presentMask |= 1L << indexId;
            }
          } catch (IndexReaderConstraintException ex) {
            LOGGER.warn("Constraint violation when indexing {} with {} index", columnName, indexType, ex);
          }
        }
      }
    } catch (Throwable t) {
      for (IndexReader reader : readersById) {
        if (reader != null) {
          try {
            reader.close();
          } catch (Throwable ct) {
            LOGGER.warn("Can't close reader on init error, column: " + columnName + " reader: " + reader.getClass(),
                ct);
          }
        }
      }
      throw t;
    }

    _presentMask = presentMask;
    int numReaders = Long.bitCount(presentMask);
    if (numReaders == 0) {
      _readers = EMPTY_READERS;
    } else {
      _readers = new IndexReader[numReaders];
      int pos = 0;
      for (IndexReader reader : readersById) {
        if (reader != null) {
          _readers[pos++] = reader;
        }
      }
    }
  }

  @Nullable
  @Override
  public <I extends IndexReader, T extends IndexType<?, I, ?>> I getIndex(T indexType) {
    short indexId = IndexService.getInstance().getNumericId(indexType);
    if (((_presentMask >>> indexId) & 1L) == 0) {
      return null;
    }
    return (I) _readers[Long.bitCount(_presentMask & ((1L << indexId) - 1))];
  }

  @Nullable
  @Override
  public VectorIndexConfig getVectorIndexConfig() {
    return _vectorIndexConfig != null && _vectorIndexConfig.isEnabled() ? _vectorIndexConfig : null;
  }

  @Override
  public void close()
      throws IOException {
    // TODO (index-spi): Verify that readers can be closed in any order
    // Close every reader even if one fails, then rethrow the first failure with the others suppressed.
    Exception failure = null;
    for (IndexReader reader : _readers) {
      try {
        reader.close();
      } catch (IOException | RuntimeException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
    }

    // This reader is closed on segment destroy()
    _multiColTextReader = null;

    if (failure instanceof IOException) {
      throw (IOException) failure;
    }
    if (failure != null) {
      throw (RuntimeException) failure;
    }
  }

  public MultiColumnLuceneTextIndexReader getMultiColumnTextIndex() {
    return _multiColTextReader;
  }

  public void setMultiColumnTextIndex(
      MultiColumnLuceneTextIndexReader multiColTextReader) {
    _multiColTextReader = multiColTextReader;
  }
}
