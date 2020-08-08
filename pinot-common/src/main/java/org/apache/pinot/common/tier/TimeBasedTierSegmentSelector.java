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
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.tier.TierFactory.TierSegmentSelectorType;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * A {@link TierSegmentSelector} strategy which selects segments for a tier based on the age of the segment
 */
public class TimeBasedTierSegmentSelector implements TierSegmentSelector {
  private final String _type = TierSegmentSelectorType.TIME.toString();
  private final long _segmentAgeMillis;
  private final HelixManager _helixManager;

  public TimeBasedTierSegmentSelector(HelixManager helixManager, String segmentAge) {
    _segmentAgeMillis = TimeUtils.convertPeriodToMillis(segmentAge);
    _helixManager = helixManager;
  }

  @Override
  public String getType() {
    return _type;
  }

  /**
   * Checks if a segment is eligible for the tier based on the segment age i.e. the end time of the segment from zk metadata
   * @param tableNameWithType Name of the table
   * @param segmentName Name of the segment
   * @return true if eligible
   */
  @Override
  public boolean selectSegment(String tableNameWithType, String segmentName) {

    SegmentZKMetadata segmentZKMetadata;
    if (TableNameBuilder.isOfflineTableResource(tableNameWithType)) {
      segmentZKMetadata = ZKMetadataProvider
          .getOfflineSegmentZKMetadata(_helixManager.getHelixPropertyStore(), tableNameWithType, segmentName);
    } else {
      segmentZKMetadata = ZKMetadataProvider
          .getRealtimeSegmentZKMetadata(_helixManager.getHelixPropertyStore(), tableNameWithType, segmentName);
    }
    Preconditions
        .checkNotNull(segmentZKMetadata, "Could not find zk metadata for segment: {} of table: {}", segmentName,
            tableNameWithType);

    // get segment end time to decide if segment gets selected
    TimeUnit timeUnit = segmentZKMetadata.getTimeUnit();
    Preconditions
        .checkNotNull(timeUnit, "Time unit is not set for segment: %s of table: %s", segmentName, tableNameWithType);
    long endTimeMs = timeUnit.toMillis(segmentZKMetadata.getEndTime());
    long now = System.currentTimeMillis();
    return (now - endTimeMs) > _segmentAgeMillis;
  }

  /**
   * Gets the age cutoff for segments accepted by this strategy
   */
  public long getSegmentAgeMillis() {
    return _segmentAgeMillis;
  }
}
