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
package org.apache.pinot.server.api.resources;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsContribution;
import org.apache.pinot.common.restlet.resources.SegmentCompressionStatsContribution;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;


/// Reads persisted compression metadata using one policy shared by all server APIs.
///
/// Concurrent REST handlers may call this class. The weak-key cache is thread-safe, and duplicate computations are
/// side-effect free. A published [SegmentMetadata] key is treated as immutable; segment reload replaces the metadata
/// instance, so a cached summary is never reused across a metadata update.
final class SegmentCompressionStatsReader {
  private static final Cache<SegmentMetadata, SegmentCompressionStatsContribution> SUMMARY_CACHE =
      CacheBuilder.newBuilder().weakKeys().build();

  private SegmentCompressionStatsReader() {
  }

  static SegmentCompressionStatsContribution read(SegmentMetadata segmentMetadata,
      boolean includeColumnCompressionStats) {
    return read(segmentMetadata, includeColumnCompressionStats, null);
  }

  static SegmentCompressionStatsContribution read(SegmentMetadata segmentMetadata,
      boolean includeColumnCompressionStats, @Nullable Set<String> columnFilter) {
    if (!includeColumnCompressionStats) {
      SegmentCompressionStatsContribution cached = SUMMARY_CACHE.getIfPresent(segmentMetadata);
      if (cached != null) {
        return cached;
      }
      SegmentCompressionStatsContribution computed = readUncached(segmentMetadata, false, null);
      SegmentCompressionStatsContribution existing = SUMMARY_CACHE.asMap().putIfAbsent(segmentMetadata, computed);
      return existing != null ? existing : computed;
    }
    return readUncached(segmentMetadata, true, columnFilter);
  }

  private static SegmentCompressionStatsContribution readUncached(SegmentMetadata segmentMetadata,
      boolean includeColumnCompressionStats, @Nullable Set<String> columnFilter) {
    long uncompressedValueSizeInBytes = 0;
    long forwardIndexAndDictionaryStorageSizeInBytes = 0;
    int eligibleColumns = 0;
    int availableColumns = 0;
    Map<String, ColumnCompressionStatsContribution> columnCompressionStats =
        includeColumnCompressionStats ? new HashMap<>() : null;
    IndexService indexService = includeColumnCompressionStats ? IndexService.getInstance() : null;

    for (ColumnMetadata columnMetadata : segmentMetadata.getColumnMetadataMap().values()) {
      long forwardIndexSize = getIndexSize(segmentMetadata, columnMetadata, StandardIndexes.forward());
      if (forwardIndexSize < 0) {
        continue;
      }
      eligibleColumns++;

      long columnUncompressedValueSize;
      long columnForwardIndexAndDictionaryStorageSize = forwardIndexSize;
      EncodingType encoding = columnMetadata.getForwardIndexEncoding();
      ChunkCompressionType chunkCompressionType;
      if (encoding == EncodingType.DICTIONARY) {
        long dictionaryUncompressedValueSize = columnMetadata.getDictionaryEncodedUncompressedValueSizeInBytes();
        long dictionaryFileSize = getIndexSize(segmentMetadata, columnMetadata, StandardIndexes.dictionary());
        if (dictionaryUncompressedValueSize < 0 || dictionaryFileSize < 0) {
          if (!includeColumnCompressionStats) {
            return incomplete(segmentMetadata);
          }
          continue;
        }
        columnUncompressedValueSize = dictionaryUncompressedValueSize;
        columnForwardIndexAndDictionaryStorageSize += dictionaryFileSize;
        chunkCompressionType = null;
      } else {
        long uncompressedValueSize = columnMetadata.getRawForwardIndexUncompressedValueSizeInBytes();
        if (uncompressedValueSize < 0 || columnMetadata.getRawForwardIndexChunkCompressionType() == null) {
          if (!includeColumnCompressionStats) {
            return incomplete(segmentMetadata);
          }
          continue;
        }
        columnUncompressedValueSize = uncompressedValueSize;
        chunkCompressionType = columnMetadata.getRawForwardIndexChunkCompressionType();
      }

      availableColumns++;
      uncompressedValueSizeInBytes += columnUncompressedValueSize;
      forwardIndexAndDictionaryStorageSizeInBytes += columnForwardIndexAndDictionaryStorageSize;
      if (columnCompressionStats != null
          && (columnFilter == null || columnFilter.contains(columnMetadata.getColumnName()))) {
        List<String> indexNames = new ArrayList<>(columnMetadata.getNumIndexes());
        for (int i = 0, n = columnMetadata.getNumIndexes(); i < n; i++) {
          indexNames.add(indexService.get(columnMetadata.getIndexType(i)).getId());
        }
        ColumnCompressionStatsContribution.EncodingContribution encodingContribution =
            new ColumnCompressionStatsContribution.EncodingContribution(encoding, chunkCompressionType, 1,
                columnUncompressedValueSize, columnForwardIndexAndDictionaryStorageSize);
        columnCompressionStats.put(columnMetadata.getColumnName(), new ColumnCompressionStatsContribution(
            columnMetadata.getColumnName(), columnUncompressedValueSize,
            columnForwardIndexAndDictionaryStorageSize, indexNames, List.of(encodingContribution), 1));
      }
    }

    if (columnCompressionStats != null && columnCompressionStats.isEmpty()) {
      columnCompressionStats = null;
    }
    boolean complete = availableColumns == eligibleColumns;
    return new SegmentCompressionStatsContribution(segmentMetadata.getName(), complete,
        complete ? uncompressedValueSizeInBytes : -1,
        complete ? forwardIndexAndDictionaryStorageSizeInBytes : -1, columnCompressionStats);
  }

  private static SegmentCompressionStatsContribution incomplete(SegmentMetadata segmentMetadata) {
    return new SegmentCompressionStatsContribution(segmentMetadata.getName(), false, -1, -1, null);
  }

  private static long getIndexSize(SegmentMetadata segmentMetadata, ColumnMetadata columnMetadata,
      IndexType<?, ?, ?> indexType) {
    long metadataSize = columnMetadata.getIndexSizeFor(indexType);
    if (metadataSize >= 0) {
      return metadataSize;
    }
    SegmentVersion version = segmentMetadata.getVersion();
    File indexDir = segmentMetadata.getIndexDir();
    if (version == null || version == SegmentVersion.v3 || indexDir == null) {
      return ColumnMetadata.UNAVAILABLE;
    }
    for (String extension : indexType.getFileExtensions(columnMetadata)) {
      File indexFile = new File(indexDir, columnMetadata.getColumnName() + extension);
      if (indexFile.isFile()) {
        return indexFile.length();
      }
    }
    return ColumnMetadata.UNAVAILABLE;
  }
}
