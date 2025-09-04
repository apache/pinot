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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class for handling offset auto reset with multi-topic ingestion.
 * The handler would request Kafka Ecosystem APIs to replicate the skipped offsets and
 * add the new topic to the table config for backfilling.
 * After the backfill job is complete, it would remove the topic from the table config.
 */
public abstract class RealtimeOffsetAutoResetKafkaHandler implements RealtimeOffsetAutoResetHandler {

  protected PinotLLCRealtimeSegmentManager _llcRealtimeSegmentManager;
  protected PinotHelixResourceManager _pinotHelixResourceManager;
  private Map<String, Integer> _topicNameToIndexMap;
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeOffsetAutoResetKafkaHandler.class);
  private static final String STREAM_TYPE = "kafka";

  public RealtimeOffsetAutoResetKafkaHandler(PinotLLCRealtimeSegmentManager llcRealtimeSegmentManager,
      PinotHelixResourceManager pinotHelixResourceManager) {
    init(llcRealtimeSegmentManager, pinotHelixResourceManager);
  }

  @Override
  public void init(PinotLLCRealtimeSegmentManager llcRealtimeSegmentManager,
      PinotHelixResourceManager pinotHelixResourceManager) {
    _llcRealtimeSegmentManager = llcRealtimeSegmentManager;
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _topicNameToIndexMap = new HashMap<>();
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
    // Trigger the data replication and get the new topic's stream config.
    Map<String, String> newTopicStreamConfig = triggerDataReplicationAndGetTopicInfo(
        tableNameWithType, streamConfig, topicName, partitionId, fromOffset, toOffset);
    if (newTopicStreamConfig == null) {
      return false;
    }
    try {
      // Add the new topic to the table config.
      TableConfig currentTableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
      if (getOrAddBackfillTopic(newTopicStreamConfig, currentTableConfig)) {
        _pinotHelixResourceManager.setExistingTableConfig(currentTableConfig);
      }
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

  /**
   * Cleanup completed backfill jobs by checking if the topic is complete.
   * If it is complete, pause the topic consumption.
   *
   * @param tableNameWithType The name of the table with type
   * @param topicNames The collection of topic names to check for completion
   * @return Collection of cleaned up topic names
   */
  public Collection<String> cleanupCompletedBackfillJobs(String tableNameWithType, Collection<String> topicNames) {
    Collection<String> cleanedUpTopics = new ArrayList<>();
    for (String topicName : topicNames) {
      if (isTopicBackfillJobComplete(tableNameWithType, topicName)) {
        cleanedUpTopics.add(topicName);
      }
    }
    if (cleanedUpTopics.isEmpty()) {
      return cleanedUpTopics;
    }
    TableConfig currentTableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    updateTopicIndexMap(IngestionConfigUtils.getStreamConfigMaps(currentTableConfig));
    _llcRealtimeSegmentManager.pauseTopicsConsumption(currentTableConfig.getTableName(),
        cleanedUpTopics.stream().map(_topicNameToIndexMap::get).collect(Collectors.toList()));
    try {
      _pinotHelixResourceManager.setExistingTableConfig(currentTableConfig);
    } catch (IOException e) {
      LOGGER.error("Cannot remove backfill topics {} from the table config", topicNames, e);
      cleanedUpTopics.clear();
    }
    return cleanedUpTopics;
  }

  public abstract boolean isTopicBackfillJobComplete(String tableNameWithType, String topicName);

  /**
   * "Add the new topic to the table config" OR "resume the topic consumption if already added" for backfilling.
   * @param streamConfig the new topic's stream config map
   * @param tableConfig the table config to update
   * @return true if the topic is newly added, false if the topic is already added and resumed
   */
  private boolean getOrAddBackfillTopic(Map<String, String> streamConfig, TableConfig tableConfig) {
    List<Map<String, String>> streamConfigs = IngestionConfigUtils.getStreamConfigMaps(tableConfig);
    String topicNameKey = StreamConfigProperties.constructStreamProperty(
        STREAM_TYPE, StreamConfigProperties.STREAM_TOPIC_NAME);
    String topicName = streamConfig.get(topicNameKey);
    Preconditions.checkNotNull(topicName);
    updateTopicIndexMap(streamConfigs);
    if (_topicNameToIndexMap.containsKey(topicName)) {
      if (_llcRealtimeSegmentManager.isTopicConsumptionPaused(
          tableConfig.getTableName(), _topicNameToIndexMap.get(topicName))) {
        LOGGER.info("Resuming topic {} consumption for table {}", topicName, tableConfig.getTableName());
        _llcRealtimeSegmentManager.resumeTopicsConsumption(
            tableConfig.getTableName(), Arrays.asList(_topicNameToIndexMap.get(topicName)));
      }
      return false;
    }
    streamConfig.put(StreamConfigProperties.BACKFILL_TOPIC, String.valueOf(true));
    IngestionConfigUtils.getStreamConfigMaps(tableConfig).add(streamConfig);
    return true;
  }

  private void updateTopicIndexMap(List<Map<String, String>> streamConfigs) {
    if (streamConfigs.size() == _topicNameToIndexMap.size()) {
      return;
    }
    _topicNameToIndexMap.clear();
    for (int i = 0; i < streamConfigs.size(); i++) {
      String topicName = streamConfigs.get(i).get(
          StreamConfigProperties.constructStreamProperty(
              STREAM_TYPE, StreamConfigProperties.STREAM_TOPIC_NAME));
      if (topicName != null) {
        _topicNameToIndexMap.put(topicName, i);
      }
    }
  }
}
