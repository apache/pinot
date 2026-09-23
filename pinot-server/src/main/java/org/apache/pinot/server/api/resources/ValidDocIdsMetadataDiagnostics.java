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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import javax.annotation.Nullable;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// Request-local observations around one bitmap read. This does not freeze ingestion, segment transitions or cleanup,
/// and the offsets do not describe the boundary at which a persisted snapshot was taken. Instances are not shared.
final class ValidDocIdsMetadataDiagnostics {
  private final long _captureStartTimeMs = System.currentTimeMillis();
  private final Map<String, Object> _metadata = new HashMap<>();
  @Nullable
  private final List<RealtimeSegmentDataManager> _consumers;
  @Nullable
  private final Path _snapshotFile;
  @Nullable
  private final BasicFileAttributes _snapshotAttributes;

  ValidDocIdsMetadataDiagnostics(IndexSegment segment, @Nullable String type,
      Map<Integer, List<RealtimeSegmentDataManager>> consumersByPartition) {
    LLCSegmentName name = LLCSegmentName.of(segment.getSegmentName());
    _consumers = name != null ? consumersByPartition.getOrDefault(name.getPartitionGroupId(), List.of()) : null;
    if (_consumers != null) {
      _metadata.put("consumingSegmentOffsetsBefore", getConsumerOffsets());
    }
    ValidDocIdsType bitmapType = type != null ? ValidDocIdsType.valueOf(type.toUpperCase()) : ValidDocIdsType.SNAPSHOT;
    String snapshotFileName = switch (bitmapType) {
      case SNAPSHOT -> V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME;
      case SNAPSHOT_WITH_DELETE -> V1Constants.QUERYABLE_DOC_IDS_SNAPSHOT_FILE_NAME;
      default -> null;
    };
    _snapshotFile = snapshotFileName != null
        ? new File(SegmentDirectoryPaths.findSegmentDirectory(segment.getSegmentMetadata().getIndexDir()),
            snapshotFileName).toPath() : null;
    _snapshotAttributes = readSnapshotAttributes();
  }

  /// Counts and the CRC must use the same detached bitmap. The capture window brackets the bitmap read and offset
  /// observations, not a transaction. Offsets are keyed by consumer segment name to preserve overlapping consumers.
  Map<String, Object> finish(MutableRoaringBitmap bitmap) {
    BasicFileAttributes after = readSnapshotAttributes();
    if (_consumers != null) {
      _metadata.put("consumingSegmentOffsetsAfter", getConsumerOffsets());
    }
    long captureEndTimeMs = System.currentTimeMillis();
    _metadata.put("captureStartTimeMs", _captureStartTimeMs);
    _metadata.put("captureEndTimeMs", captureEndTimeMs);
    _metadata.put("validDocIdsCrc32", computeCrc32(bitmap));
    // Snapshot files are replaced by rename. Suppress age if the file changed during the read or its identity is
    // unavailable. File age is not snapshot-attempt age: unchanged snapshots may legitimately retain old mtimes.
    if (_snapshotAttributes != null && after != null && _snapshotAttributes.fileKey() != null
        && _snapshotAttributes.fileKey().equals(after.fileKey())
        && _snapshotAttributes.lastModifiedTime().equals(after.lastModifiedTime())
        && _snapshotAttributes.size() == after.size()) {
      long modifiedTimeMs = after.lastModifiedTime().toMillis();
      if (modifiedTimeMs > 0 && modifiedTimeMs <= captureEndTimeMs) {
        _metadata.put("snapshotFileAgeMs", captureEndTimeMs - modifiedTimeMs);
      }
    }
    return _metadata;
  }

  /// CRC32 over ascending doc IDs, each encoded as four big-endian bytes. Roaring container layout and insertion
  /// history do not affect it. An equal CRC is a diagnostic hint, not a proof of equal membership or row payloads.
  static long computeCrc32(MutableRoaringBitmap bitmap) {
    CRC32 crc = new CRC32();
    IntIterator iterator = bitmap.getIntIterator();
    while (iterator.hasNext()) {
      int docId = iterator.next();
      crc.update(docId >>> 24);
      crc.update(docId >>> 16);
      crc.update(docId >>> 8);
      crc.update(docId);
    }
    return crc.getValue();
  }

  private Map<String, String> getConsumerOffsets() {
    Map<String, String> offsets = new HashMap<>();
    for (RealtimeSegmentDataManager consumer : _consumers) {
      StreamPartitionMsgOffset offset = consumer.getCurrentOffset();
      if (offset != null) {
        // This is the next offset to consume, not the last applied row or a persisted-snapshot offset.
        offsets.put(consumer.getSegmentName(), offset.toString());
      }
    }
    return offsets;
  }

  @Nullable
  private BasicFileAttributes readSnapshotAttributes() {
    if (_snapshotFile == null) {
      return null;
    }
    try {
      return Files.readAttributes(_snapshotFile, BasicFileAttributes.class);
    } catch (IOException e) {
      // The bitmap read retains its existing missing/corrupt-file behavior; optional diagnostics are best effort.
      return null;
    }
  }
}
