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
package org.apache.pinot.plugin.minion.tasks.materializedview;

import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.materializedview.metadata.PartitionFingerprint;
import org.apache.pinot.materializedview.metadata.PartitionInfo;
import org.apache.pinot.materializedview.metadata.PartitionState;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Unit tests for [MaterializedViewTaskExecutor] helpers.
///
/// This is the single-most-important correctness gate in the MV executor: if the query result
/// saturates the declared LIMIT, we MUST fail the task so the runtime watermark / partitions map
/// is not advanced against incomplete data.
public class MaterializedViewTaskExecutorTest {

  private static final String TABLE = "mv_orders";
  private static final long WINDOW_START = 1_700_000_000_000L;
  private static final long WINDOW_END = 1_700_086_400_000L;

  @Test
  public void testUnderLimitPasses() {
    Map<String, String> configs = configsWithLimit(1_000);
    MaterializedViewTaskExecutor.verifyResultNotTruncated(
        configs, TABLE, WINDOW_START, WINDOW_END, 999);
  }

  @Test
  public void testAtLimitFails() {
    Map<String, String> configs = configsWithLimit(1_000);
    try {
      MaterializedViewTaskExecutor.verifyResultNotTruncated(
          configs, TABLE, WINDOW_START, WINDOW_END, 1_000);
      fail("Expected completeness gate to fail when rows == LIMIT");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("saturated LIMIT"),
          "Unexpected message: " + e.getMessage());
      assertTrue(e.getMessage().contains(TABLE));
    }
  }

  @Test
  public void testOverLimitFails() {
    // Defensive: the broker should cap at LIMIT, but an ill-behaved executor could return more.
    Map<String, String> configs = configsWithLimit(1_000);
    try {
      MaterializedViewTaskExecutor.verifyResultNotTruncated(
          configs, TABLE, WINDOW_START, WINDOW_END, 1_500);
      fail("Expected completeness gate to fail when rows > LIMIT");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("saturated LIMIT"),
          "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testMissingLimitKeyFailsLoudly() {
    // Pre-upgrade tasks lacking EFFECTIVE_LIMIT_KEY must NOT be silently passed through —
    // a missing key would let the broker's small default-LIMIT truncate the MV window
    // without the saturation gate detecting it.  Helix will retry; the retry sees a new
    // task config (post-upgrade) and succeeds.
    Map<String, String> configs = new HashMap<>();
    try {
      MaterializedViewTaskExecutor.verifyResultNotTruncated(
          configs, TABLE, WINDOW_START, WINDOW_END, 42);
      fail("Expected IllegalStateException for missing EFFECTIVE_LIMIT_KEY");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Missing"),
          "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testInvalidLimitKeyThrows() {
    Map<String, String> configs = new HashMap<>();
    configs.put(MaterializedViewTask.EFFECTIVE_LIMIT_KEY, "not-a-number");
    try {
      MaterializedViewTaskExecutor.verifyResultNotTruncated(
          configs, TABLE, WINDOW_START, WINDOW_END, 42);
      fail("Expected IllegalStateException for malformed effectiveLimit");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Invalid"),
          "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testZeroLimitFailsLoudly() {
    // The generator always emits a positive effectiveLimit (user-declared or DEFAULT_MV_QUERY_LIMIT).
    // A 0 here means corrupted task config — fail loud so the bug surfaces.
    Map<String, String> configs = configsWithLimit(0);
    try {
      MaterializedViewTaskExecutor.verifyResultNotTruncated(
          configs, TABLE, WINDOW_START, WINDOW_END, 0);
      fail("Expected IllegalStateException for non-positive effectiveLimit");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("must be positive"),
          "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testNegativeLimitFailsLoudly() {
    Map<String, String> configs = configsWithLimit(-1);
    try {
      MaterializedViewTaskExecutor.verifyResultNotTruncated(
          configs, TABLE, WINDOW_START, WINDOW_END, 0);
      fail("Expected IllegalStateException for negative effectiveLimit");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("must be positive"),
          "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testZeroRowsPasses() {
    // Empty windows are legitimate and must not be flagged as truncated.
    Map<String, String> configs = configsWithLimit(1_000);
    MaterializedViewTaskExecutor.verifyResultNotTruncated(
        configs, TABLE, WINDOW_START, WINDOW_END, 0);
  }

  private static Map<String, String> configsWithLimit(int limit) {
    Map<String, String> configs = new HashMap<>();
    configs.put(MaterializedViewTask.EFFECTIVE_LIMIT_KEY, String.valueOf(limit));
    return configs;
  }

  // -----------------------------------------------------------------------
  //  decodeBytesValue
  // -----------------------------------------------------------------------

  @Test
  public void testDecodeBytesValueFromHex() {
    assertEquals((byte[]) MaterializedViewTaskExecutor.decodeBytesValue("raw_hll", "01020a0f"),
        new byte[]{1, 2, 10, 15});
  }

  @Test
  public void testDecodeBytesValueFromBase64() {
    String value = Base64.getEncoder().encodeToString(new byte[]{1, 2, 10, 15});
    assertEquals((byte[]) MaterializedViewTaskExecutor.decodeBytesValue("raw_theta", value),
        new byte[]{1, 2, 10, 15});
  }

  // -----------------------------------------------------------------------
  //  computeContiguousUpperMs
  // -----------------------------------------------------------------------

  private static final long BUCKET_MS = 86_400_000L; // 1d

  @Test
  public void testContiguousEmptyMap() {
    long result = MaterializedViewTaskExecutor.computeContiguousUpperMs(
        WINDOW_START, new LinkedHashMap<>(), BUCKET_MS);
    assertEquals(result, WINDOW_START, "Empty map: cursor unchanged");
  }

  @Test
  public void testContiguousSingleValid() {
    Map<Long, PartitionInfo> partitions = new LinkedHashMap<>();
    partitions.put(WINDOW_START, validInfo());
    long result = MaterializedViewTaskExecutor.computeContiguousUpperMs(
        WINDOW_START, partitions, BUCKET_MS);
    assertEquals(result, WINDOW_START + BUCKET_MS, "Single VALID: advances by one bucket");
  }

  @Test
  public void testContiguousChainOfThree() {
    Map<Long, PartitionInfo> partitions = new LinkedHashMap<>();
    partitions.put(WINDOW_START, validInfo());
    partitions.put(WINDOW_START + BUCKET_MS, validInfo());
    partitions.put(WINDOW_START + 2 * BUCKET_MS, validInfo());
    long result = MaterializedViewTaskExecutor.computeContiguousUpperMs(
        WINDOW_START, partitions, BUCKET_MS);
    assertEquals(result, WINDOW_START + 3 * BUCKET_MS, "Three VALIDs: advances three buckets");
  }

  @Test
  public void testContiguousStopsAtGap() {
    // [START] VALID, [START+1d] missing, [START+2d] VALID — chain stops at the gap.
    Map<Long, PartitionInfo> partitions = new LinkedHashMap<>();
    partitions.put(WINDOW_START, validInfo());
    partitions.put(WINDOW_START + 2 * BUCKET_MS, validInfo());
    long result = MaterializedViewTaskExecutor.computeContiguousUpperMs(
        WINDOW_START, partitions, BUCKET_MS);
    assertEquals(result, WINDOW_START + BUCKET_MS, "Gap after first VALID: stops there");
  }

  @Test
  public void testContiguousStopsAtNonValidState() {
    Map<Long, PartitionInfo> partitions = new LinkedHashMap<>();
    partitions.put(WINDOW_START, validInfo());
    partitions.put(WINDOW_START + BUCKET_MS, staleInfo());
    long result = MaterializedViewTaskExecutor.computeContiguousUpperMs(
        WINDOW_START, partitions, BUCKET_MS);
    assertEquals(result, WINDOW_START + BUCKET_MS, "STALE breaks the chain like a gap");
  }

  @Test
  public void testContiguousFromMsAlreadyMissing() {
    // Cursor starts at a missing key — return immediately without advancing.
    Map<Long, PartitionInfo> partitions = new LinkedHashMap<>();
    partitions.put(WINDOW_START + BUCKET_MS, validInfo());
    long result = MaterializedViewTaskExecutor.computeContiguousUpperMs(
        WINDOW_START, partitions, BUCKET_MS);
    assertEquals(result, WINDOW_START, "fromMs not present: no advance");
  }

  @Test
  public void testWindowFingerprintIncludesSegmentIdentity() {
    SegmentZKMetadata first = segment("segA", WINDOW_START, WINDOW_START + BUCKET_MS, 1234L);
    SegmentZKMetadata second = segment("segB", WINDOW_START, WINDOW_START + BUCKET_MS, 1234L);

    PartitionFingerprint firstFingerprint =
        MaterializedViewTaskExecutor.computeWindowFingerprint(List.of(first), WINDOW_START, WINDOW_START + BUCKET_MS);
    PartitionFingerprint secondFingerprint =
        MaterializedViewTaskExecutor.computeWindowFingerprint(List.of(second), WINDOW_START, WINDOW_START + BUCKET_MS);

    assertTrue(!firstFingerprint.equals(secondFingerprint),
        "Fingerprint must change when segment identity changes even if CRC is reused");
  }

  @Test
  public void testWindowFingerprintOnlyCountsOverlappingSegments() {
    SegmentZKMetadata overlapping = segment("overlap", WINDOW_START, WINDOW_START + BUCKET_MS, 10L);
    SegmentZKMetadata outside = segment("outside", WINDOW_START + 2 * BUCKET_MS, WINDOW_START + 3 * BUCKET_MS, 20L);

    PartitionFingerprint fingerprint = MaterializedViewTaskExecutor.computeWindowFingerprint(
        List.of(overlapping, outside), WINDOW_START, WINDOW_START + BUCKET_MS);

    assertEquals(fingerprint.getSegmentCount(), 1);
    assertEquals(fingerprint,
        MaterializedViewTaskExecutor.computeWindowFingerprint(List.of(overlapping), WINDOW_START,
            WINDOW_START + BUCKET_MS));
  }

  private static PartitionInfo validInfo() {
    return new PartitionInfo(PartitionState.VALID, new PartitionFingerprint(0, 0), 0L);
  }

  private static PartitionInfo staleInfo() {
    return new PartitionInfo(PartitionState.STALE, new PartitionFingerprint(0, 0), 0L);
  }

  private static SegmentZKMetadata segment(String name, long startMs, long endMs, long crc) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(name);
    segmentZKMetadata.setTimeUnit(TimeUnit.MILLISECONDS);
    segmentZKMetadata.setStartTime(startMs);
    segmentZKMetadata.setEndTime(endMs);
    segmentZKMetadata.setCrc(crc);
    return segmentZKMetadata;
  }
}
