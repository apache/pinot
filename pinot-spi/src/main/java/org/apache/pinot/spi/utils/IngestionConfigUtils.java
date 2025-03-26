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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.stream.StreamConfig;


/**
 * Helper methods for extracting fields from IngestionConfig in a backward compatible manner
 */
@SuppressWarnings("deprecation")
public final class IngestionConfigUtils {
  private IngestionConfigUtils() {
  }

  public static final String DOT_SEPARATOR = ".";
  public static final String DEFAULT_SEGMENT_NAME_GENERATOR_TYPE =
      BatchConfigProperties.SegmentNameGeneratorType.SIMPLE;
  public static final String DEFAULT_SEGMENT_INGESTION_TYPE = "APPEND";
  public static final String DEFAULT_PUSH_MODE = "tar";
  public static final int DEFAULT_PUSH_ATTEMPTS = 5;
  public static final int DEFAULT_PUSH_PARALLELISM = 1;
  public static final long DEFAULT_PUSH_RETRY_INTERVAL_MILLIS = 1000L;
  // For partition from different topics, we pad then with an offset to avoid collision. The offset is far higher
  // than the normal max number of partitions on stream (e.g. 512).
  public static final int PARTITION_PADDING_OFFSET = 10000;

  /**
   * Fetches the streamConfig from the given realtime table.
   * First, the ingestionConfigs->stream->streamConfigs will be checked.
   * If not found, the indexingConfig->streamConfigs will be checked (which is deprecated).
   * @param tableConfig realtime table config
   * @return streamConfigs List of maps
   */
  public static List<Map<String, String>> getStreamConfigMaps(TableConfig tableConfig) {
    String realtimeTableName = tableConfig.getTableName();
    Preconditions.checkState(tableConfig.getTableType() == TableType.REALTIME,
        "Cannot fetch streamConfigs for OFFLINE table: %s", realtimeTableName);
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    if (ingestionConfig != null && ingestionConfig.getStreamIngestionConfig() != null) {
      List<Map<String, String>> streamConfigMaps = ingestionConfig.getStreamIngestionConfig().getStreamConfigMaps();
      Preconditions.checkState(CollectionUtils.isNotEmpty(streamConfigMaps),
          "Cannot find streamConfigs in StreamIngestionConfig for table: %s", realtimeTableName);
      return streamConfigMaps;
    }
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    if (indexingConfig.getStreamConfigs() != null) {
      return List.of(indexingConfig.getStreamConfigs());
    }
    throw new IllegalStateException("Cannot find streamConfigs for table: " + realtimeTableName);
  }

  public static List<StreamConfig> getStreamConfigs(TableConfig tableConfig) {
    return getStreamConfigMaps(tableConfig).stream()
        .map(streamConfigMap -> new StreamConfig(tableConfig.getTableName(), streamConfigMap))
        .collect(Collectors.toList());
  }

  // TODO: Revisit the callers of this method. We should use the stream config for a given partition, instead of the
  //       first one.
  public static Map<String, String> getFirstStreamConfigMap(TableConfig tableConfig) {
    return getStreamConfigMaps(tableConfig).get(0);
  }

  // TODO: Revisit the callers of this method. We should use the stream config for a given partition, instead of the
  //       first one.
  public static StreamConfig getFirstStreamConfig(TableConfig tableConfig) {
    return new StreamConfig(tableConfig.getTableName(), getFirstStreamConfigMap(tableConfig));
  }

  /**
   * Getting the Pinot segment level partition id from the stream partition id.
   * @param partitionId the partition id from the stream
   * @param index the index of the SteamConfig from the list of StreamConfigs
   */
  public static int getPinotPartitionIdFromStreamPartitionId(int partitionId, int index) {
    return index * PARTITION_PADDING_OFFSET + partitionId;
  }

  /**
   * Getting the Stream partition id from the Pinot segment partition id.
   * @param partitionId the segment partition id on Pinot
   */
  public static int getStreamPartitionIdFromPinotPartitionId(int partitionId) {
    return partitionId % PARTITION_PADDING_OFFSET;
  }

  /**
   * Getting the StreamConfig index of StreamConfigs list from the Pinot segment partition id.
   * @param partitionId the segment partition id on Pinot
   */
  public static int getStreamConfigIndexFromPinotPartitionId(int partitionId) {
    return partitionId / PARTITION_PADDING_OFFSET;
  }

  /**
   * Fetches the streamConfig from the list of streamConfigs according to the partition id.
   */
  public static Map<String, String> getStreamConfigMap(TableConfig tableConfig, int partitionId) {
    if (partitionId < PARTITION_PADDING_OFFSET) {
      return getFirstStreamConfigMap(tableConfig);
    }

    String tableNameWithType = tableConfig.getTableName();
    Preconditions.checkState(tableConfig.getTableType() == TableType.REALTIME,
        "Cannot fetch streamConfigs for OFFLINE table: %s", tableNameWithType);
    int index = getStreamConfigIndexFromPinotPartitionId(partitionId);
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    Preconditions.checkState(ingestionConfig != null && ingestionConfig.getStreamIngestionConfig() != null
            && ingestionConfig.getStreamIngestionConfig().getStreamConfigMaps().size() > index,
        "Cannot find stream config of index: %s for table: %s", index, tableNameWithType);
    return ingestionConfig.getStreamIngestionConfig().getStreamConfigMaps().get(index);
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
    return getConfigMapWithPrefix(batchConfigMap, BatchConfigProperties.SEGMENT_NAME_GENERATOR_PROP_PREFIX);
  }

  public static PinotConfiguration getInputFsProps(Map<String, String> batchConfigMap) {
    return new PinotConfiguration(getPropsWithPrefix(batchConfigMap, BatchConfigProperties.INPUT_FS_PROP_PREFIX));
  }

  public static PinotConfiguration getOutputFsProps(Map<String, String> batchConfigMap) {
    return new PinotConfiguration(getPropsWithPrefix(batchConfigMap, BatchConfigProperties.OUTPUT_FS_PROP_PREFIX));
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
    //noinspection rawtypes,unchecked
    return (Map) getConfigMapWithPrefix(batchConfigMap, prefix);
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
