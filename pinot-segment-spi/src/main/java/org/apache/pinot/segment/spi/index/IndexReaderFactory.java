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

package org.apache.pinot.segment.spi.index;

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.IndexConfig;


public interface IndexReaderFactory<R extends IndexReader> {

  /// Tries to create an index reader for the given column and segment.
  ///
  /// It may be the case that the configuration indicates that the index should be disabled but it is actually there.
  /// Also, it may be the case that the configuration says that the index should be there but it is not in the reader.
  /// In both cases the source of truth is the segment reader.
  ///
  /// @throws IndexReaderConstraintException if the constraints of the index reader are not matched. For example, some
  /// indexes may require the column to be dictionary based.
  /// @return the index reader or null if there is no index for that column
  @Nullable
  R createIndexReader(SegmentDirectory.Reader segmentReader, FieldIndexConfigs fieldIndexConfigs,
      ColumnMetadata metadata)
      throws IOException, IndexReaderConstraintException;

  /// Creates a reader for a column whose segment stores no buffer for this index type, that is, when
  /// [SegmentDirectory.Reader#hasIndexFor] is `false` and [#createIndexReader] is not called. This lets an index type
  /// serve a reader whose content is derived from other data of the column, for example its forward index, instead of
  /// read from a stored buffer.
  ///
  /// It is called after the readers of the column's stored indexes are created, which `storedReaders` exposes. The
  /// default returns `null`: without a stored buffer the index is absent.
  ///
  /// @throws IndexReaderConstraintException if the constraints of the index reader are not matched
  /// @return the index reader, or null if the index is absent for that column
  @Nullable
  default R createIndexReaderWithoutStoredIndex(SegmentDirectory.Reader segmentReader,
      FieldIndexConfigs fieldIndexConfigs, ColumnMetadata metadata, StoredIndexReaders storedReaders)
      throws IOException, IndexReaderConstraintException {
    return null;
  }

  /// The readers created for a column from its stored indexes, as passed to [#createIndexReaderWithoutStoredIndex].
  interface StoredIndexReaders {

    /// Returns the column's reader of the given index type, or null when the column stores no such index.
    @Nullable
    <I extends IndexReader, T extends IndexType<?, I, ?>> I getIndex(T indexType);
  }

  abstract class Default<C extends IndexConfig, R extends IndexReader> implements IndexReaderFactory<R> {

    protected abstract IndexType<C, R, ?> getIndexType();

    protected abstract R createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata, C indexConfig)
        throws IOException, IndexReaderConstraintException;

    @Override
    public R createIndexReader(SegmentDirectory.Reader segmentReader, FieldIndexConfigs fieldIndexConfigs,
        ColumnMetadata metadata)
        throws IOException, IndexReaderConstraintException {
      IndexType<C, R, ?> indexType = getIndexType();

      if (!segmentReader.hasIndexFor(metadata.getColumnName(), indexType)) { // there is no buffer for this index
        return null;
      }

      PinotDataBuffer buffer = segmentReader.getIndexFor(metadata.getColumnName(), indexType);
      try {
        return createIndexReader(buffer, metadata, fieldIndexConfigs.getConfig(indexType));
      } catch (RuntimeException ex) {
        throw new RuntimeException(
            "Cannot read index " + indexType + " for column " + metadata.getColumnName(), ex);
      }
    }
  }
}
