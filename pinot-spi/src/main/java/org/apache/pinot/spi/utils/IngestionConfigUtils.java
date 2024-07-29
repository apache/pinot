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
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;


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

  /**
   * Fetches the streamConfig from the given realtime table.
   * First, the ingestionConfigs->stream->streamConfigs will be checked.
   * If not found, the indexingConfig->streamConfigs will be checked (which is deprecated).
   * @param tableConfig realtime table config
   * @return streamConfigs map
   */
  public static Map<String, String> getStreamConfigMap(TableConfig tableConfig) {
    String tableNameWithType = tableConfig.getTableName();
    Preconditions.checkState(tableConfig.getTableType() == TableType.REALTIME,
        "Cannot fetch streamConfigs for OFFLINE table: %s", tableNameWithType);
    Map<String, String> streamConfigMap = null;
    if (tableConfig.getIngestionConfig() != null
        && tableConfig.getIngestionConfig().getStreamIngestionConfig() != null) {
      List<Map<String, String>> streamConfigMaps =
          tableConfig.getIngestionConfig().getStreamIngestionConfig().getStreamConfigMaps();
      Preconditions.checkState(streamConfigMaps.size() == 1, "Only 1 stream supported per table");
      streamConfigMap = streamConfigMaps.get(0);
    }
    if (streamConfigMap == null && tableConfig.getIndexingConfig() != null) {
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
