/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.realtime.impl.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.DataSize;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Low level stream config, adds some overrides for llc-specific properties.
 */
public class KafkaLowLevelStreamProviderConfig extends KafkaHighLevelStreamProviderConfig {
  private static final int NOT_DEFINED = Integer.MIN_VALUE;
  private static final long DEFAULT_DESIRED_SEGMENT_SIZE_BYTES = 200 * 1024 * 1024;
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaLowLevelStreamProviderConfig.class);

  private long llcSegmentTimeInMillis = NOT_DEFINED;
  private int llcRealtimeRecordsThreshold = NOT_DEFINED;
  private long desiredSegmentSizeBytes = DEFAULT_DESIRED_SEGMENT_SIZE_BYTES;

  @Override
  public void init(TableConfig tableConfig, InstanceZKMetadata instanceMetadata, Schema schema) {
    super.init(tableConfig, instanceMetadata, schema);

    if (tableConfig.getIndexingConfig().getStreamConfigs().containsKey(CommonConstants.Helix.DataSource.Realtime.LLC_REALTIME_SEGMENT_FLUSH_SIZE)) {
      llcRealtimeRecordsThreshold =
          Integer.parseInt(tableConfig.getIndexingConfig().getStreamConfigs().get(CommonConstants.Helix.DataSource.Realtime.LLC_REALTIME_SEGMENT_FLUSH_SIZE));
    }

    if (tableConfig.getIndexingConfig().getStreamConfigs().containsKey(CommonConstants.Helix.DataSource.Realtime.LLC_REALTIME_SEGMENT_FLUSH_TIME)) {
      llcSegmentTimeInMillis =
          Long.parseLong(tableConfig.getIndexingConfig().getStreamConfigs().get(CommonConstants.Helix.DataSource.Realtime.LLC_REALTIME_SEGMENT_FLUSH_TIME));
    }

    String desiredSegmentSizeStr = tableConfig.getIndexingConfig().getStreamConfigs().get(CommonConstants.Helix.DataSource.Realtime.REALTIME_DESIRED_SEGMENT_SIZE);
    if (desiredSegmentSizeStr != null) {
      long longVal = DataSize.toBytes(tableConfig.getIndexingConfig().getStreamConfigs().get(CommonConstants.Helix.DataSource.Realtime.REALTIME_DESIRED_SEGMENT_SIZE));
      if (longVal > 0) {
        desiredSegmentSizeBytes = longVal;
      } else {
        LOGGER.warn("Invalid value '{}' for {}. Ignored", desiredSegmentSizeStr, CommonConstants.Helix.DataSource.Realtime.REALTIME_DESIRED_SEGMENT_SIZE);
      }
    }
  }

  @Override
  public void init(Map<String, String> properties, Schema schema) {
    super.init(properties, schema);

    if (properties.containsKey(CommonConstants.Helix.DataSource.Realtime.LLC_REALTIME_SEGMENT_FLUSH_SIZE)) {
      llcRealtimeRecordsThreshold =
          Integer.parseInt(properties.get(CommonConstants.Helix.DataSource.Realtime.LLC_REALTIME_SEGMENT_FLUSH_SIZE));
    }

    if (properties.containsKey(CommonConstants.Helix.DataSource.Realtime.LLC_REALTIME_SEGMENT_FLUSH_TIME)) {
      llcSegmentTimeInMillis =
          convertToMs(properties.get(CommonConstants.Helix.DataSource.Realtime.LLC_REALTIME_SEGMENT_FLUSH_TIME));
    }
  }

  @Override
  public int getSizeThresholdToFlushSegment() {
    if (llcRealtimeRecordsThreshold != NOT_DEFINED) {
      return llcRealtimeRecordsThreshold;
    } else {
      return super.getSizeThresholdToFlushSegment();
    }
  }

  @Override
  public long getTimeThresholdToFlushSegment() {
    if (llcSegmentTimeInMillis != NOT_DEFINED) {
      return llcSegmentTimeInMillis;
    } else {
      return super.getTimeThresholdToFlushSegment();
    }
  }

  public long getDesiredSegmentSizeBytes() {
    return desiredSegmentSizeBytes;
  }

  @VisibleForTesting
  public static long getDefaultDesiredSegmentSizeBytes() {
    return DEFAULT_DESIRED_SEGMENT_SIZE_BYTES;
  }
}
