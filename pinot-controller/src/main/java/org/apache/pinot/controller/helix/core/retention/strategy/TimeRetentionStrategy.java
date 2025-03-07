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
package org.apache.pinot.controller.helix.core.retention.strategy;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>TimeRetentionStrategy</code> class uses segment end time to manage the retention for segments.
 */
public class TimeRetentionStrategy implements RetentionStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeRetentionStrategy.class);

  private final long _retentionMs;

  public TimeRetentionStrategy(TimeUnit timeUnit, long timeValue) {
    _retentionMs = timeUnit.toMillis(timeValue);
  }

  @Override
  public boolean isPurgeable(String tableNameWithType, SegmentZKMetadata segmentZKMetadata) {
    return isPurgeable(tableNameWithType, segmentZKMetadata.getSegmentName(), segmentZKMetadata.getEndTimeMs());
  }

  @Override
  public boolean isPurgeable(String tableNameWithType, String segmentName, long segmentTimeMs) {

    // Check that the end time is between 1971 and 2071
    if (!TimeUtils.timeValueInValidRange(segmentTimeMs)) {
      LOGGER.warn("Segment: {} of table: {} has invalid end time in millis: {}", segmentName,
          tableNameWithType, segmentTimeMs);
      return false;
    }

    return System.currentTimeMillis() - segmentTimeMs > _retentionMs;
  }
}
