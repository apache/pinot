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
package org.apache.pinot.controller.helix.core.periodictask;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class RealtimeOffsetAutoResetKafkaHandler extends RealtimeOffsetAutoResetHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeOffsetAutoResetKafkaHandler.class);
  private static final String STREAM_TYPE = "kafka";

  public RealtimeOffsetAutoResetKafkaHandler(PinotLLCRealtimeSegmentManager llcRealtimeSegmentManager,
      PinotHelixResourceManager pinotHelixResourceManager) {
    super(llcRealtimeSegmentManager, pinotHelixResourceManager);
  }

  /**
   * Trigger the job to backfill the skipped interval due to offset auto reset.
   * It is expected to backfill the [fromOffset, toOffset) interval.
   * @return true if successfully started the backfill job and its ingestion
   */
  @Override
  public boolean triggerBackfillJob(
      String tableNameWithType, StreamConfig streamConfig, String topicName, int partitionId, long fromOffset,
      long toOffset) {
    Map<String, String> newTopicStreamConfig = triggerDataReplicationAndGetTopicInfo(
        tableNameWithType, streamConfig, topicName, partitionId, fromOffset, toOffset);
    if (newTopicStreamConfig == null) {
      return false;
    }
    try {
      TableConfig currentTableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
      addNewTopicToTableConfig(newTopicStreamConfig, currentTableConfig);
      _pinotHelixResourceManager.setExistingTableConfig(currentTableConfig);
    } catch (IOException e) {
      LOGGER.error("Cannot add backfill topic to the table config", e);
      return false;
    }
    return true;
  }

  /**
   * Override this method to trigger Kafka Ecosystem APIs and replicate skipped offsets to the new topic.
   * Then refer to the lagged topic's StreamConfig and return the new topic's stream config map.
   */
  protected abstract Map<String, String> triggerDataReplicationAndGetTopicInfo(
      String tableNameWithType, StreamConfig streamConfig, String topicName, int partitionId, long fromOffset,
      long toOffset);

  public abstract void ensureBackfillJobsRunning(String tableNameWithType, List<String> topicNames);

  public Collection<String> cleanupCompletedBackfillJobs(String tableNameWithType, Collection<String> topicNames) {
    Collection<String> cleanedUpTopics = new ArrayList<>();
    for (String topicName : topicNames) {
      if (isTopicBackfillJobComplete(tableNameWithType, topicName)) {
        cleanedUpTopics.add(topicName);
      }
    }
    TableConfig currentTableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    for (String topicName : cleanedUpTopics) {
      removeTopicFromTableConfig(tableNameWithType, topicName, currentTableConfig);
    }
    try {
      _pinotHelixResourceManager.setExistingTableConfig(currentTableConfig);
    } catch (IOException e) {
      LOGGER.error("Cannot remove backfill topics {} from the table config", topicNames, e);
      cleanedUpTopics.clear();
    }
    return cleanedUpTopics;
  }

  public abstract boolean isTopicBackfillJobComplete(String tableNameWithType, String topicName);

  private void addNewTopicToTableConfig(Map<String, String> streamConfig, TableConfig tableConfig) {
    List<Map<String, String>> streamConfigs = IngestionConfigUtils.getStreamConfigMaps(tableConfig);
    String topicNameKey = StreamConfigProperties.constructStreamProperty(
        STREAM_TYPE, StreamConfigProperties.STREAM_TOPIC_NAME);
    String topicName = streamConfig.get(topicNameKey);
    Preconditions.checkNotNull(topicName);
    for (Map<String, String> config : streamConfigs) {
      if (topicName.equals(config.get(topicNameKey))) {
        LOGGER.info("Topic {} already added to table {}", topicName, tableConfig.getTableName());
        return;
      }
    }
    streamConfig.put(StreamConfigProperties.EPHEMERAL_BACKFILL_TOPIC, String.valueOf(true));
    streamConfig.put(
        StreamConfigProperties.constructStreamProperty(
            STREAM_TYPE, StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA),
        OffsetCriteria.SMALLEST_OFFSET_CRITERIA.getOffsetString());
    IngestionConfigUtils.getStreamConfigMaps(tableConfig).add(streamConfig);
  }

  private void removeTopicFromTableConfig(String tableNameWithType, String topicName, TableConfig tableConfig) {
    List<Map<String, String>> streamConfigMaps = IngestionConfigUtils.getStreamConfigMaps(tableConfig);
    for (int i = streamConfigMaps.size() - 1; i >= 0; i--) {
      StreamConfig config = new StreamConfig(tableNameWithType, streamConfigMaps.get(i));
      if (config.isEphemeralBackfillTopic() && topicName.equals(config.getTopicName())) {
        streamConfigMaps.remove(i);
        return;
      }
    }
  }
}
