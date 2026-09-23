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
package org.apache.pinot.segment.local.upsert;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.utils.JsonUtils;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// A persisted bitmap and its optional capture diagnostics, read from the same file generation.
/// The Roaring bytes remain a prefix readable by existing snapshot readers. A versioned metadata trailer is published
/// with that prefix through the existing temporary-file rename, so a concurrent reader cannot pair different captures.
public record DocIdsSnapshot(MutableRoaringBitmap docIds, @Nullable Metadata metadata) {
  private static final Logger LOGGER = LoggerFactory.getLogger(DocIdsSnapshot.class);
  private static final int MAGIC = 0x5044534D; // PDSM: Pinot doc ids snapshot metadata
  private static final int VERSION = 1;

  /// This identifies the consumer startup that triggered a snapshot, not a partition consistency watermark.
  public record Trigger(String segmentName, String startOffset) {
  }

  public record Metadata(long validDocIdsCrc32, long snapshotCapturedAtMs,
                         @Nullable String snapshotTriggerSegmentName, @Nullable String snapshotTriggerStartOffset) {
    public Map<String, Object> toResponse(long nowMs) {
      Map<String, Object> response = new HashMap<>();
      response.put("validDocIdsCrc32", validDocIdsCrc32);
      response.put("snapshotCapturedAtMs", snapshotCapturedAtMs);
      // Do not turn clock skew into an apparently fresh snapshot.
      if (nowMs >= snapshotCapturedAtMs) {
        response.put("snapshotAgeMs", nowMs - snapshotCapturedAtMs);
      }
      if (snapshotTriggerSegmentName != null && snapshotTriggerStartOffset != null) {
        response.put("snapshotTriggerSegmentName", snapshotTriggerSegmentName);
        response.put("snapshotTriggerStartOffset", snapshotTriggerStartOffset);
      }
      return response;
    }
  }

  /// Capture the timestamp with the bitmap under its existing lock, then hash the detached bitmap outside that lock.
  public static ThreadSafeMutableRoaringBitmap.CardinalityAndBytes capture(ThreadSafeMutableRoaringBitmap bitmap,
      @Nullable Trigger trigger)
      throws IOException {
    ThreadSafeMutableRoaringBitmap.CardinalityAndBytes snapshot;
    long capturedAtMs;
    synchronized (bitmap) {
      snapshot = bitmap.getBytesAndCardinality();
      capturedAtMs = System.currentTimeMillis();
    }
    ImmutableRoaringBitmap captured = new ImmutableRoaringBitmap(ByteBuffer.wrap(snapshot.getBytes()));
    CRC32 crc = new CRC32();
    IntIterator iterator = captured.getIntIterator();
    // Canonical ascending doc IDs, four big-endian bytes per ID: independent of Roaring container layout.
    while (iterator.hasNext()) {
      int docId = iterator.next();
      crc.update(docId >>> 24);
      crc.update(docId >>> 16);
      crc.update(docId >>> 8);
      crc.update(docId);
    }
    Metadata metadata = new Metadata(crc.getValue(), capturedAtMs, trigger != null ? trigger.segmentName() : null,
        trigger != null ? trigger.startOffset() : null);
    byte[] json = JsonUtils.objectToString(metadata).getBytes(StandardCharsets.UTF_8);
    byte[] bytes = ByteBuffer.allocate(snapshot.getBytes().length + 2 * Integer.BYTES + json.length)
        .put(snapshot.getBytes()).putInt(MAGIC).putInt(VERSION).put(json).array();
    return new ThreadSafeMutableRoaringBitmap.CardinalityAndBytes(snapshot.getCardinality(), bytes);
  }

  public static DocIdsSnapshot fromBytes(byte[] bytes) {
    ImmutableRoaringBitmap bitmap = new ImmutableRoaringBitmap(ByteBuffer.wrap(bytes));
    Metadata metadata = null;
    int bitmapSize = bitmap.serializedSizeInBytes();
    if (bytes.length >= bitmapSize + 2 * Integer.BYTES) {
      ByteBuffer trailer = ByteBuffer.wrap(bytes, bitmapSize, bytes.length - bitmapSize);
      if (trailer.getInt() == MAGIC && trailer.getInt() == VERSION) {
        try {
          Metadata parsed = JsonUtils.stringToObject(
              new String(bytes, trailer.position(), trailer.remaining(), StandardCharsets.UTF_8), Metadata.class);
          if (parsed.snapshotCapturedAtMs() > 0 && parsed.validDocIdsCrc32() >= 0
              && parsed.validDocIdsCrc32() <= 0xFFFFFFFFL) {
            metadata = parsed;
          }
        } catch (Exception e) {
          // Diagnostics must not prevent recovery of a valid bitmap.
          LOGGER.warn("Ignoring unreadable doc ids snapshot diagnostics", e);
        }
      }
    }
    return new DocIdsSnapshot(bitmap.toMutableRoaringBitmap(), metadata);
  }
}
