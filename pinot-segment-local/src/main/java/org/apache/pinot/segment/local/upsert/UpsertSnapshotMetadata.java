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
import java.util.List;
import javax.annotation.Nullable;


/// Immutable, constant-size evidence about a snapshot attempt and its declared immutable-segment population.
/// Content hashes describe saved file bytes, not canonical bitmap membership or a verified source boundary.
/// Local epochs and capture IDs identify observations; they are not alignment keys between replicas.
@JsonIgnoreProperties(ignoreUnknown = true)
public record UpsertSnapshotMetadata(int formatVersion, int partitionId, String consumingSegmentName,
                                    String startOffset, long capturedAtMillis, long finishedAtMillis,
                                    String runtimeEpoch, long captureId, Attempt attempt, Counters counters,
                                    Content content, CleanupProgress cleanupBefore, CleanupProgress cleanupAfter,
                                    Activity lifecycleBefore, Activity lifecycleAfter,
                                    @Nullable String configurationFingerprint, List<String> comparisonIssues) {
  public static final int FORMAT_VERSION = 2;

  public UpsertSnapshotMetadata {
    comparisonIssues = List.copyOf(comparisonIssues);
  }

  public String getBoundaryStatus() {
    return "UNVERIFIED";
  }

  /// Counts each selected segment once. Deferred work is distinct from a direct lock-acquisition failure.
  public record Attempt(int selectedSegments, int writtenSegments, int lockSkippedSegments,
                        int otherSkippedSegments, int failedSegments, boolean aborted) {
  }

  /// Monotonic within runtimeEpoch; the latest report can otherwise hide failures between controller polls.
  public record Counters(long attemptsTotal, long attemptsWithLockSkipsTotal, long failedAttemptsTotal) {
  }

  /// Hash coverage for the declared population, including cached contributions of retained snapshot files.
  /// Unknown files prevent publication of an aggregate saved-files hash.
  public record Content(String algorithm, String scope, @Nullable String populationFingerprint,
                        @Nullable String savedFilesFingerprint, int expectedFiles, int knownFiles) {
    public static final String ALGORITHM = "SHA-256-LENGTH-PREFIXED-V1";
    public static final String SCOPE = "TRACKED_IMMUTABLE_SEGMENTS";

    public static Content unavailable(int expectedFiles) {
      return new Content(ALGORITHM, SCOPE, null, null, expectedFiles, 0);
    }
  }

  /// A coherent local lifecycle observation. Failures remain visible even after the operation has stopped.
  public record Activity(long version, int activeOperations, long failedOperations) {
  }

  /// Historical observations must not be replaced by the producer's current cleanup state when serving the API.
  /// A completed watermark is absent until an actual successful pass has established it.
  public record CleanupProgress(long version, String phase, @Nullable Double lastCompletedWatermark,
                                @Nullable Double targetWatermark, boolean mayAffectValidDocIds, long failedPasses) {
    public static CleanupProgress unknown() {
      return new CleanupProgress(0, "UNKNOWN", null, null, true, 0);
    }

    public static CleanupProgress disabled() {
      return new CleanupProgress(0, "DISABLED", null, null, false, 0);
    }
  }

  /// Current-process evidence remains readable when sidecar persistence fails. A saved file is historical fallback.
  public record Status(UpsertSnapshotMetadata snapshot, String persistenceStatus, long publicationFailures,
                       CleanupProgress cleanupNow) {
  }
}
