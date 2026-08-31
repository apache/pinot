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
package org.apache.pinot.common.tier;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.utils.TimeUtils;


/// A [TierSegmentSelector] strategy which selects segments for a tier based on the age of the segment.
///
/// The age reference is controlled by the tier's `segmentAgeField`:
///   - `endTime` (default, backward-compatible): uses `SegmentZKMetadata#getEndTimeMs()`, the segment's
///     max data timestamp. Suitable when segment age tracks data recency, e.g. streaming ingest where
///     endTime is close to wall-clock now.
///   - `creationTime`: uses `SegmentZKMetadata#getCreationTime()`, when the segment file was built.
///     Suitable when segment age should track ingestion recency, e.g. batch ingest of historical data
///     where endTime lies far in the past regardless of when the segment was created.
public class TimeBasedTierSegmentSelector implements TierSegmentSelector {

  /// Which timestamp field on [SegmentZKMetadata] is compared against the age threshold.
  public enum AgeField {
    END_TIME, CREATION_TIME;

    public static AgeField fromConfig(@Nullable String value) {
      if (value == null || value.isEmpty()) {
        return END_TIME;
      }
      String normalized = value.trim();
      if ("endtime".equalsIgnoreCase(normalized) || "end_time".equalsIgnoreCase(normalized)) {
        return END_TIME;
      }
      if ("creationtime".equalsIgnoreCase(normalized) || "creation_time".equalsIgnoreCase(normalized)) {
        return CREATION_TIME;
      }
      throw new IllegalArgumentException(
          "Unsupported segmentAgeField: '" + value + "'. Expected 'endTime' or 'creationTime'.");
    }
  }

  private final long _segmentAgeMillis;
  private final AgeField _ageField;

  public TimeBasedTierSegmentSelector(String segmentAge) {
    this(segmentAge, AgeField.END_TIME);
  }

  public TimeBasedTierSegmentSelector(String segmentAge, AgeField ageField) {
    _segmentAgeMillis = TimeUtils.convertPeriodToMillis(segmentAge);
    _ageField = ageField != null ? ageField : AgeField.END_TIME;
  }

  @Override
  public String getType() {
    return TierFactory.TIME_SEGMENT_SELECTOR_TYPE;
  }

  @Override
  public boolean selectSegment(String tableNameWithType, SegmentZKMetadata segmentZKMetadata) {
    // don't try to move consuming segments
    if (!segmentZKMetadata.getStatus().isCompleted()) {
      return false;
    }

    long referenceMs;
    switch (_ageField) {
      case CREATION_TIME:
        referenceMs = segmentZKMetadata.getCreationTime();
        // creationTime may be absent for very old segments predating the field; skip rather than throw
        // so a partially-populated table doesn't fail every tier evaluation.
        if (referenceMs <= 0) {
          return false;
        }
        break;
      case END_TIME:
      default:
        referenceMs = segmentZKMetadata.getEndTimeMs();
        Preconditions.checkState(referenceMs > 0, "Invalid endTimeMs: %s for segment: %s of table: %s", referenceMs,
            segmentZKMetadata.getSegmentName(), tableNameWithType);
        break;
    }
    return (System.currentTimeMillis() - referenceMs) > _segmentAgeMillis;
  }

  /// Gets the age cutoff for segments accepted by this strategy
  public long getSegmentAgeMillis() {
    return _segmentAgeMillis;
  }

  /// The [SegmentZKMetadata] field this selector compares against the age threshold.
  public AgeField getAgeField() {
    return _ageField;
  }

  @Override
  public String toString() {
    return "TimeBasedTierSegmentSelector{_segmentAgeMillis=" + _segmentAgeMillis
        + ", _ageField=" + _ageField + "}";
  }
}
