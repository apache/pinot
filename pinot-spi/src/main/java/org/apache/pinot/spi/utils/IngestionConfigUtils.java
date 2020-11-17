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
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;


/**
 * Helper methods for extracting fields from IngestionConfig in a backward compatible manner
 */
public final class IngestionConfigUtils {
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
    if (tableConfig.getIngestionConfig() != null && tableConfig.getIngestionConfig().getStreamIngestionConfig() != null) {
      List<Map<String, String>> streamConfigMaps = tableConfig.getIngestionConfig().getStreamIngestionConfig().getStreamConfigMaps();
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

  /**
   * Fetches the configured segmentPushType (APPEND/REFRESH) from the table config
   * First checks in the ingestionConfig. If not found, checks in the segmentsConfig (has been deprecated from here in favor of ingestion config)
   */
  public static String getBatchSegmentPushType(TableConfig tableConfig) {
    String segmentPushType = null;
    if (tableConfig.getIngestionConfig() != null) {
      BatchIngestionConfig batchIngestionConfig = tableConfig.getIngestionConfig().getBatchIngestionConfig();
      if (batchIngestionConfig != null) {
        segmentPushType = batchIngestionConfig.getSegmentPushType();
      }
    }
    if (segmentPushType == null) {
      segmentPushType = tableConfig.getValidationConfig().getSegmentPushType();
    }
    return segmentPushType;
  }

  /**
   * Fetches the configured segmentPushFrequency from the table config
   * First checks in the ingestionConfig. If not found, checks in the segmentsConfig (has been deprecated from here in favor of ingestion config)
   */
  public static String getBatchSegmentPushFrequency(TableConfig tableConfig) {
    String segmentPushFrequency = null;
    if (tableConfig.getIngestionConfig() != null) {
      BatchIngestionConfig batchIngestionConfig = tableConfig.getIngestionConfig().getBatchIngestionConfig();
      if (batchIngestionConfig != null) {
        segmentPushFrequency = batchIngestionConfig.getSegmentPushFrequency();
      }
    }
    if (segmentPushFrequency == null) {
      segmentPushFrequency = tableConfig.getValidationConfig().getSegmentPushFrequency();
    }
    return segmentPushFrequency;
  }

}
