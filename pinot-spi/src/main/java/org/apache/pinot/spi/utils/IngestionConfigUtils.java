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
package org.apache.pinot.spi.utils;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.stream.StreamConfig;


/**
 * Helper methods for extracting fields from IngestionConfig in a backward compatible manner
 */
public final class IngestionConfigUtils {
  private IngestionConfigUtils() {
  }

  public static final String DOT_SEPARATOR = ".";
  private static final String DEFAULT_SEGMENT_NAME_GENERATOR_TYPE =
      BatchConfigProperties.SegmentNameGeneratorType.SIMPLE;
  private static final String DEFAULT_SEGMENT_INGESTION_TYPE = "APPEND";
  private static final String DEFAULT_PUSH_MODE = "tar";
  private static final int DEFAULT_PUSH_ATTEMPTS = 5;
  private static final int DEFAULT_PUSH_PARALLELISM = 1;
  private static final long DEFAULT_PUSH_RETRY_INTERVAL_MILLIS = 1000L;
  // For partition from different tables, we pad then with an offset to avoid collision. The offset is far higher
  // than the normal max number of partitions on stream (e.g. 512).
  public static final int PARTITION_PADDING_OFFSET = 10000;
  public static final String DEFAULT_CONSUMER_FACTORY_CLASS_NAME_STRING =
      "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory";
  public static final String STREAM_TYPE = "streamType";
  public static final String STREAM_CONSUMER_FACTORY_CLASS = "stream.consumer.factory.class";

  /**
   * Fetches the streamConfig from the given realtime table.
   * First, the ingestionConfigs->stream->streamConfigs will be checked.
   * If not found, the indexingConfig->streamConfigs will be checked (which is deprecated).
   * @param tableConfig realtime table config
   * @return streamConfigs List of maps
   */
  public static List<Map<String, String>> getStreamConfigMaps(TableConfig tableConfig) {
    String tableNameWithType = tableConfig.getTableName();
    Preconditions.checkState(tableConfig.getTableType() == TableType.REALTIME,
        "Cannot fetch streamConfigs for OFFLINE table: %s", tableNameWithType);
    if (tableConfig.getIngestionConfig() != null
        && tableConfig.getIngestionConfig().getStreamIngestionConfig() != null) {
      List<Map<String, String>> streamConfigMaps =
          tableConfig.getIngestionConfig().getStreamIngestionConfig().getStreamConfigMaps();
      Preconditions.checkState(!streamConfigMaps.isEmpty(), "Table must have at least 1 stream");
      /*
      Apply the following checks if there are multiple streamConfigs
      1. Check if all streamConfigs have the same stream type. TODO: remove this limitation once we've tested it
      2. Ensure segment flush parameters consistent across all streamConfigs. We need this because Pinot is predefining
      the values before fetching stream partition info from stream. At the construction time, we don't know the value
      extracted from a streamConfig would be applied to which segment.
      TODO: remove this limitation once we've refactored the code and supported it.
       */
      Map<String, String> firstStreamConfigMap = streamConfigMaps.get(0);
      for (int i = 1; i < streamConfigMaps.size(); i++) {
        Map<String, String> map = streamConfigMaps.get(i);
        Preconditions.checkNotNull(map.get(STREAM_TYPE),
            "streamType must be defined for all streamConfigs for REALTIME table: %s", tableNameWithType);
        Preconditions.checkState(StringUtils.equals(map.get(STREAM_TYPE), firstStreamConfigMap.get(STREAM_TYPE))
                && StreamConfig.extractFlushThresholdRows(map) == StreamConfig.extractFlushThresholdRows(
            firstStreamConfigMap)
                && StreamConfig.extractFlushThresholdTimeMillis(map) == StreamConfig.extractFlushThresholdTimeMillis(
            firstStreamConfigMap)
                && StreamConfig.extractFlushThresholdVarianceFraction(map)
                == StreamConfig.extractFlushThresholdVarianceFraction(firstStreamConfigMap)
                && StreamConfig.extractFlushThresholdSegmentSize(map) == StreamConfig.extractFlushThresholdSegmentSize(
            firstStreamConfigMap)
                && StreamConfig.extractFlushThresholdSegmentRows(map) == StreamConfig.extractFlushThresholdSegmentRows(
            firstStreamConfigMap),
            "All streamConfigs must have the same stream type for REALTIME table: %s", tableNameWithType);
      }
      return streamConfigMaps;
    }
    if (tableConfig.getIndexingConfig() != null && tableConfig.getIndexingConfig().getStreamConfigs() != null) {
      return Arrays.asList(tableConfig.getIndexingConfig().getStreamConfigs());
    }
    throw new IllegalStateException("Could not find streamConfigs for REALTIME table: " + tableNameWithType);
  }

  /**
   * Getting the Pinot segment level partition id from the stream partition id.
   * @param partitionId the partition group id from the stream
   * @param index the index of the SteamConfig from the list of StreamConfigs
   * @return
   */
  public static int getPinotPartitionIdFromStreamPartitionId(int partitionId, int index) {
    return index * PARTITION_PADDING_OFFSET + partitionId;
  }

  /**
   * Getting the Stream partition id from the Pinot segment partition id.
   * @param partitionId the segment partition group id on Pinot
   * @return
   */
  public static int getStreamPartitionIdFromPinotPartitionId(int partitionId) {
    return partitionId % PARTITION_PADDING_OFFSET;
  }

  /**
   * Getting the StreamConfig index of StreamConfigs list from the Pinot segment partition id.
   * @param partitionId the segment partition group id on Pinot
   * @return
   */
  public static int getStreamConfigIndexFromPinotPartitionId(int partitionId) {
    return partitionId / PARTITION_PADDING_OFFSET;
  }

  /**
   * Fetches the streamConfig from the list of streamConfigs according to the partitonGroupId.
   * @param tableConfig realtime table config
   * @param partitionGroupId partitionGroupId
   * @return streamConfig map
   */
  public static Map<String, String> getStreamConfigMapWithPartitionGroupId(
      TableConfig tableConfig, int partitionGroupId) {
    String tableNameWithType = tableConfig.getTableName();
    Preconditions.checkState(tableConfig.getTableType() == TableType.REALTIME,
        "Cannot fetch streamConfigs for OFFLINE table: %s", tableNameWithType);
    Map<String, String> streamConfigMap = null;
    if (tableConfig.getIngestionConfig() != null
        && tableConfig.getIngestionConfig().getStreamIngestionConfig() != null) {
      List<Map<String, String>> streamConfigMaps =
          tableConfig.getIngestionConfig().getStreamIngestionConfig().getStreamConfigMaps();
      Preconditions.checkState(
          streamConfigMaps.size() > partitionGroupId / PARTITION_PADDING_OFFSET,
          "Table does not have enough number of stream");
      streamConfigMap = streamConfigMaps.get(partitionGroupId / PARTITION_PADDING_OFFSET);
    }
    if (partitionGroupId < PARTITION_PADDING_OFFSET
        && streamConfigMap == null && tableConfig.getIndexingConfig() != null) {
      streamConfigMap = tableConfig.getIndexingConfig().getStreamConfigs();
    }
    if (streamConfigMap == null) {
      throw new IllegalStateException("Could not find streamConfigs for REALTIME table: " + tableNameWithType);
    }
    return streamConfigMap;
  }

  public static List<AggregationConfig> getAggregationConfigs(TableConfig tableConfig) {
    String tableNameWithType = tableConfig.getTableName();
    Preconditions.checkState(tableConfig.getTableType() == TableType.REALTIME,
        "aggregationConfigs are only supported in REALTIME tables. Found a OFFLINE table: %s", tableNameWithType);

    if (tableConfig.getIngestionConfig() != null) {
      return tableConfig.getIngestionConfig().getAggregationConfigs();
    }
    return null;
  }

  /**
   * Fetches the configured consistentDataPush boolean from the table config
   */
  public static boolean getBatchSegmentIngestionConsistentDataPushEnabled(TableConfig tableConfig) {
    boolean consistentDataPush = false;
    if (tableConfig.getIngestionConfig() != null) {
      BatchIngestionConfig batchIngestionConfig = tableConfig.getIngestionConfig().getBatchIngestionConfig();
      if (batchIngestionConfig != null) {
        consistentDataPush = batchIngestionConfig.getConsistentDataPush();
      }
    }
    return consistentDataPush;
  }

  /**
   * Fetches the configured segmentIngestionType (APPEND/REFRESH) from the table config
   * First checks in the ingestionConfig. If not found, checks in the segmentsConfig (has been deprecated from here
   * in favor of ingestion
   * config)
   */
  public static String getBatchSegmentIngestionType(TableConfig tableConfig) {
    String segmentIngestionType = null;
    if (tableConfig.getIngestionConfig() != null) {
      BatchIngestionConfig batchIngestionConfig = tableConfig.getIngestionConfig().getBatchIngestionConfig();
      if (batchIngestionConfig != null) {
        segmentIngestionType = batchIngestionConfig.getSegmentIngestionType();
      }
    }
    if (segmentIngestionType == null) {
      segmentIngestionType = tableConfig.getValidationConfig().getSegmentPushType();
    }
    return (segmentIngestionType == null) ? DEFAULT_SEGMENT_INGESTION_TYPE : segmentIngestionType;
  }

  /**
   * Fetches the configured segmentIngestionFrequency from the table config
   * First checks in the ingestionConfig. If not found, checks in the segmentsConfig (has been deprecated from here
   * in favor of ingestion
   * config)
   */
  public static String getBatchSegmentIngestionFrequency(TableConfig tableConfig) {
    String segmentIngestionFrequency = null;
    if (tableConfig.getIngestionConfig() != null) {
      BatchIngestionConfig batchIngestionConfig = tableConfig.getIngestionConfig().getBatchIngestionConfig();
      if (batchIngestionConfig != null) {
        segmentIngestionFrequency = batchIngestionConfig.getSegmentIngestionFrequency();
      }
    }
    if (segmentIngestionFrequency == null) {
      segmentIngestionFrequency = tableConfig.getValidationConfig().getSegmentPushFrequency();
    }
    return segmentIngestionFrequency;
  }

  /**
   * Fetch the properties which belong to record reader, by removing the identifier prefix
   */
  public static Map<String, String> getRecordReaderProps(Map<String, String> batchConfigMap) {
    return getConfigMapWithPrefix(batchConfigMap, BatchConfigProperties.RECORD_READER_PROP_PREFIX);
  }

  /**
   * Fetch the properties which belong to segment name generator, by removing the identifier prefix
   */
  public static Map<String, String> getSegmentNameGeneratorProps(Map<String, String> batchConfigMap) {
    return getConfigMapWithPrefix(batchConfigMap,
        BatchConfigProperties.SEGMENT_NAME_GENERATOR_PROP_PREFIX);
  }

  public static PinotConfiguration getInputFsProps(Map<String, String> batchConfigMap) {
    return new PinotConfiguration(
        getPropsWithPrefix(batchConfigMap, BatchConfigProperties.INPUT_FS_PROP_PREFIX));
  }

  public static PinotConfiguration getOutputFsProps(Map<String, String> batchConfigMap) {
    return new PinotConfiguration(
        getPropsWithPrefix(batchConfigMap, BatchConfigProperties.OUTPUT_FS_PROP_PREFIX));
  }

  /**
   * Extracts entries where keys start with given prefix
   */
  public static Map<String, String> extractPropsMatchingPrefix(Map<String, String> batchConfigMap, String prefix) {
    Map<String, String> propsMatchingPrefix = new HashMap<>();
    for (Map.Entry<String, String> entry : batchConfigMap.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(prefix)) {
        propsMatchingPrefix.put(key, entry.getValue());
      }
    }
    return propsMatchingPrefix;
  }

  public static Map<String, Object> getPropsWithPrefix(Map<String, String> batchConfigMap, String prefix) {
    Map<String, Object> props = new HashMap<>();
    props.putAll(getConfigMapWithPrefix(batchConfigMap, prefix));
    return props;
  }

  public static Map<String, String> getConfigMapWithPrefix(Map<String, String> batchConfigMap, String prefix) {
    Map<String, String> props = new HashMap<>();
    if (!prefix.endsWith(DOT_SEPARATOR)) {
      prefix = prefix + DOT_SEPARATOR;
    }
    for (String configKey : batchConfigMap.keySet()) {
      if (configKey.startsWith(prefix)) {
        String[] splits = configKey.split(prefix, 2);
        if (splits.length > 1) {
          props.put(splits[1], batchConfigMap.get(configKey));
        }
      }
    }
    return props;
  }

  /**
   * Extracts the segment name generator type from the batchConfigMap, or returns default value if not found
   */
  public static String getSegmentNameGeneratorType(Map<String, String> batchConfigMap) {
    return batchConfigMap.getOrDefault(BatchConfigProperties.SEGMENT_NAME_GENERATOR_TYPE,
        DEFAULT_SEGMENT_NAME_GENERATOR_TYPE);
  }

  /**
   * Extracts the push mode from the batchConfigMap, or returns default value if not found
   */
  public static String getPushMode(Map<String, String> batchConfigMap) {
    return batchConfigMap.getOrDefault(BatchConfigProperties.PUSH_MODE, DEFAULT_PUSH_MODE);
  }

  /**
   * Extracts the push attempts from the batchConfigMap, or returns default value if not found
   */
  public static int getPushAttempts(Map<String, String> batchConfigMap) {
    String pushAttempts = batchConfigMap.get(BatchConfigProperties.PUSH_ATTEMPTS);
    if (StringUtils.isNumeric(pushAttempts)) {
      return Integer.parseInt(pushAttempts);
    }
    return DEFAULT_PUSH_ATTEMPTS;
  }

  /**
   * Extracts the push parallelism from the batchConfigMap, or returns default value if not found
   */
  public static int getPushParallelism(Map<String, String> batchConfigMap) {
    String pushParallelism = batchConfigMap.get(BatchConfigProperties.PUSH_PARALLELISM);
    if (StringUtils.isNumeric(pushParallelism)) {
      return Integer.parseInt(pushParallelism);
    }
    return DEFAULT_PUSH_PARALLELISM;
  }

  /**
   * Extracts the push return interval millis from the batchConfigMap, or returns default value if not found
   */
  public static long getPushRetryIntervalMillis(Map<String, String> batchConfigMap) {
    String pushRetryIntervalMillis = batchConfigMap.get(BatchConfigProperties.PUSH_RETRY_INTERVAL_MILLIS);
    if (StringUtils.isNumeric(pushRetryIntervalMillis)) {
      return Long.parseLong(pushRetryIntervalMillis);
    }
    return DEFAULT_PUSH_RETRY_INTERVAL_MILLIS;
  }
}
