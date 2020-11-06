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
package org.apache.pinot.core.util;

import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.core.data.function.FunctionEvaluator;
import org.apache.pinot.core.data.function.FunctionEvaluatorFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.Batch;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamConfig;


/**
 * Utility methods for extracting source and destination fields from ingestion configs
 */
public class IngestionUtils {

  /**
   * Extracts all fields required by the {@link org.apache.pinot.spi.data.readers.RecordExtractor} from the given TableConfig and Schema
   * Fields for ingestion come from 2 places:
   * 1. The schema
   * 2. The ingestion config in the table config. The ingestion config (e.g. filter) can have fields which are not in the schema.
   */
  public static Set<String> getFieldsForRecordExtractor(@Nullable IngestionConfig ingestionConfig, Schema schema) {
    Set<String> fieldsForRecordExtractor = new HashSet<>();
    extractFieldsFromIngestionConfig(ingestionConfig, fieldsForRecordExtractor);
    extractFieldsFromSchema(schema, fieldsForRecordExtractor);
    return fieldsForRecordExtractor;
  }

  /**
   * Extracts all the fields needed by the {@link org.apache.pinot.spi.data.readers.RecordExtractor} from the given Schema
   * TODO: for now, we assume that arguments to transform function are in the source i.e. no columns are derived from transformed columns
   */
  private static void extractFieldsFromSchema(Schema schema, Set<String> fields) {
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn()) {
        FunctionEvaluator functionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(fieldSpec);
        if (functionEvaluator != null) {
          fields.addAll(functionEvaluator.getArguments());
        }
        fields.add(fieldSpec.getName());
      }
    }
  }

  /**
   * Extracts the fields needed by a RecordExtractor from given {@link IngestionConfig}
   */
  private static void extractFieldsFromIngestionConfig(@Nullable IngestionConfig ingestionConfig, Set<String> fields) {
    if (ingestionConfig != null) {
      FilterConfig filterConfig = ingestionConfig.getFilterConfig();
      if (filterConfig != null) {
        String filterFunction = filterConfig.getFilterFunction();
        if (filterFunction != null) {
          FunctionEvaluator functionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(filterFunction);
          if (functionEvaluator != null) {
            fields.addAll(functionEvaluator.getArguments());
          }
        }
      }
      List<TransformConfig> transformConfigs = ingestionConfig.getTransformConfigs();
      if (transformConfigs != null) {
        for (TransformConfig transformConfig : transformConfigs) {
          FunctionEvaluator expressionEvaluator =
              FunctionEvaluatorFactory.getExpressionEvaluator(transformConfig.getTransformFunction());
          fields.addAll(expressionEvaluator.getArguments());
          fields.add(transformConfig
              .getColumnName()); // add the column itself too, so that if it is already transformed, we won't transform again
        }
      }
    }
  }

  /**
   * Returns false if the record contains key {@link GenericRow#SKIP_RECORD_KEY} with value true
   */
  public static boolean shouldIngestRow(GenericRow genericRow) {
    return !Boolean.TRUE.equals(genericRow.getValue(GenericRow.SKIP_RECORD_KEY));
  }

  public static Long extractTimeValue(Comparable time) {
    if (time != null) {
      if (time instanceof Number) {
        return ((Number) time).longValue();
      } else {
        String stringValue = time.toString();
        if (StringUtils.isNumeric(stringValue)) {
          return Long.parseLong(stringValue);
        }
      }
    }
    return null;
  }

  /**
   * Fetches the streamConfig from the given realtime table.
   * First, the ingestionConfigs->stream->streamConfigs will be checked.
   * If not found, the indexingConfig->streamConfigs will be checked (which is deprecated).
   * @param tableConfig realtime table config
   * @return streamConfigs map
   */
  public static Map<String, String> getStreamConfigsMap(TableConfig tableConfig) {
    String tableNameWithType = tableConfig.getTableName();
    Preconditions.checkState(tableConfig.getTableType() == TableType.REALTIME,
        "Cannot fetch streamConfigs for OFFLINE table: %s", tableNameWithType);
    Map<String, String> streamConfigsMap = null;
    if (tableConfig.getIngestionConfig() != null && tableConfig.getIngestionConfig().getStream() != null) {
      List<Map<String, String>> streamConfigs = tableConfig.getIngestionConfig().getStream().getStreamConfigs();
      if (!CollectionUtils.isEmpty(streamConfigs)) {
        Preconditions.checkState(streamConfigs.size() == 1, "Only 1 stream supported per table");
        streamConfigsMap = streamConfigs.get(0);
      }
    }
    if (streamConfigsMap == null && tableConfig.getIndexingConfig() != null) {
      streamConfigsMap = tableConfig.getIndexingConfig().getStreamConfigs();
    }
    if (streamConfigsMap == null) {
      throw new IllegalStateException("Could not find streamConfigs for REALTIME table: " + tableNameWithType);
    }
    return streamConfigsMap;
  }

  /**
   * Fetches the configured segmentPushType (APPEND/REFRESH) from the table config
   * First checks in the ingestionConfig. If not found, checks in the segmentsConfig (has been deprecated from here in favor of ingestion config)
   */
  public static String getBatchSegmentPushType(TableConfig tableConfig) {
    String segmentPushType = null;
    if (tableConfig.getIngestionConfig() != null) {
      Batch batch = tableConfig.getIngestionConfig().getBatch();
      if (batch != null) {
        segmentPushType = batch.getSegmentPushType();
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
      Batch batch = tableConfig.getIngestionConfig().getBatch();
      if (batch != null) {
        segmentPushFrequency = batch.getSegmentPushFrequency();
      }
    }
    if (segmentPushFrequency == null) {
      segmentPushFrequency = tableConfig.getValidationConfig().getSegmentPushFrequency();
    }
    return segmentPushFrequency;
  }
}
