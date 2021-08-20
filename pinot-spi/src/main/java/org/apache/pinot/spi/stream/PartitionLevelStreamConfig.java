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
package org.apache.pinot.spi.stream;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link StreamConfig} for LLC partition level stream, which overrides some properties for low-level consumer.
 * This can be removed once we remove HLC implementation from the code
 */
public class PartitionLevelStreamConfig extends StreamConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionLevelStreamConfig.class);

  public PartitionLevelStreamConfig(String tableNameWithType, Map<String, String> streamConfigMap) {
    super(tableNameWithType, streamConfigMap);
  }

  @Override
  protected int extractFlushThresholdRows(Map<String, String> streamConfigMap) {
    String flushThresholdRowsKey = StreamConfigProperties.DEPRECATED_SEGMENT_FLUSH_THRESHOLD_ROWS + StreamConfigProperties.LLC_SUFFIX;
    String flushThresholdRowsStr = streamConfigMap.get(flushThresholdRowsKey);
    if (flushThresholdRowsStr != null) {
      try {
        int flushThresholdRows = Integer.parseInt(flushThresholdRowsStr);
        // Flush threshold rows 0 means using segment size based flush threshold
        Preconditions.checkState(flushThresholdRows >= 0);
        return flushThresholdRows;
      } catch (Exception e) {
        int defaultValue = super.extractFlushThresholdRows(streamConfigMap);
        LOGGER.warn("Invalid config {}: {}, defaulting to: {}", flushThresholdRowsKey, flushThresholdRowsStr, defaultValue);
        return defaultValue;
      }
    } else {
      return super.extractFlushThresholdRows(streamConfigMap);
    }
  }

  @Override
  protected long extractFlushThresholdTimeMillis(Map<String, String> streamConfigMap) {
    String flushThresholdTimeKey = StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME + StreamConfigProperties.LLC_SUFFIX;
    String flushThresholdTimeStr = streamConfigMap.get(flushThresholdTimeKey);
    if (flushThresholdTimeStr != null) {
      try {
        return TimeUtils.convertPeriodToMillis(flushThresholdTimeStr);
      } catch (Exception e) {
        long defaultValue = super.extractFlushThresholdTimeMillis(streamConfigMap);
        LOGGER.warn("Invalid config {}: {}, defaulting to: {}", flushThresholdTimeKey, flushThresholdTimeStr, defaultValue);
        return defaultValue;
      }
    } else {
      return super.extractFlushThresholdTimeMillis(streamConfigMap);
    }
  }
}
