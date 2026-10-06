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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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

  /// The consumer startup that triggered a snapshot. consumedUpToOffset is exclusive and per replica: the consumer's
  /// start offset, or where the previous segment stopped if it was still unsealed. It is not a consistency watermark.
  public record Trigger(String consumingSegmentName, String consumedUpToOffset) {
  }

  /// Which bitmap a snapshot file holds, so its CRC says what it covers.
  public enum DocIdsType {
    VALID_DOC_IDS, QUERYABLE_DOC_IDS
  }

  // Ignore unknown fields so an older server still reads diagnostics written by a newer one.
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record Metadata(long docIdsCrc, DocIdsType docIdsType, long snapshotCapturedAtMs,
                         @Nullable String snapshotConsumingSegmentName, @Nullable String snapshotConsumedUpToOffset) {
    public Map<String, Object> toResponse(long nowMs) {
      Map<String, Object> response = new HashMap<>();
      response.put("docIdsCrc", docIdsCrc);
      response.put("docIdsType", docIdsType.name());
      response.put("snapshotCapturedAtMs", snapshotCapturedAtMs);
      // Do not turn clock skew into an apparently fresh snapshot.
      if (nowMs >= snapshotCapturedAtMs) {
        response.put("snapshotAgeMs", nowMs - snapshotCapturedAtMs);
      }
      if (snapshotConsumingSegmentName != null && snapshotConsumedUpToOffset != null) {
        response.put("snapshotConsumingSegmentName", snapshotConsumingSegmentName);
        response.put("snapshotConsumedUpToOffset", snapshotConsumedUpToOffset);
      }
      return response;
    }
  }

  /// Capture the timestamp with the bitmap under its existing lock, then hash the detached bitmap outside that lock.
  public static ThreadSafeMutableRoaringBitmap.CardinalityAndBytes capture(ThreadSafeMutableRoaringBitmap bitmap,
      DocIdsType docIdsType, @Nullable Trigger trigger)
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
    Metadata metadata = new Metadata(crc.getValue(), docIdsType, capturedAtMs,
        trigger != null ? trigger.consumingSegmentName() : null, trigger != null ? trigger.consumedUpToOffset() : null);
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
          if (parsed.docIdsType() != null && parsed.snapshotCapturedAtMs() > 0 && parsed.docIdsCrc() >= 0
              && parsed.docIdsCrc() <= 0xFFFFFFFFL) {
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
