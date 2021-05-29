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
package org.apache.pinot.plugin.minion.tasks.merge_rollup;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.minion.Granularity;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.segment.processing.collector.CollectorFactory;
import org.apache.pinot.core.segment.processing.collector.ValueAggregatorFactory;
import org.apache.pinot.pql.parsers.utils.Pair;
import org.apache.pinot.spi.utils.TimeUtils;


public class MergeRollupTaskUtils {

  private static final String[] validMergeProperties = {
      MinionConstants.MergeRollupTask.MERGE_TYPE_KEY,
      MinionConstants.MergeRollupTask.BUFFER_TIME,
      MinionConstants.MergeRollupTask.MAX_NUM_RECORDS_PER_SEGMENT,
      MinionConstants.MergeRollupTask.MAX_NUM_RECORDS_PER_TASK
  };

  private static final String[] validMergeType = {
      CollectorFactory.CollectorType.CONCAT.name(),
      CollectorFactory.CollectorType.ROLLUP.name()
  };

  public static Map<String, ValueAggregatorFactory.ValueAggregatorType> getRollupAggregationTypeMap(
      Map<String, String> mergeRollupConfig) {
    Map<String, ValueAggregatorFactory.ValueAggregatorType> rollupAggregationTypeMap = new HashMap<>();
    for (Map.Entry<String, String> entry : mergeRollupConfig.entrySet()) {
      if (entry.getKey().startsWith(MinionConstants.MergeRollupTask.AGGREGATE_KEY_PREFIX)) {
        rollupAggregationTypeMap.put(getAggregateColumn(entry.getKey()),
            ValueAggregatorFactory.ValueAggregatorType.valueOf(entry.getValue().toUpperCase()));
      }
    }
    return rollupAggregationTypeMap;
  }

  public static Map<Granularity, MergeProperties> getAllMergeProperties(Map<String, String> mergeRollupConfig) {
    Map<Granularity, Map<String, String>> mergePropertiesMap = new HashMap<>();
    for (Map.Entry<String, String> entry : mergeRollupConfig.entrySet()) {
      if (entry.getKey().startsWith(MinionConstants.MergeRollupTask.MERGE_KEY_PREFIX)) {
        Pair<Granularity, String> pair = getGranularityAndPropertyPair(entry.getKey(), entry.getValue());
        Granularity granularity = pair.getFirst();
        String mergeProperty = pair.getSecond();
        mergePropertiesMap.putIfAbsent(granularity, new HashMap<>());
        mergePropertiesMap.get(granularity).put(mergeProperty, entry.getValue());
      }
    }

    Map<Granularity, MergeProperties> allMergeProperties = new HashMap<>();
    for (Map.Entry<Granularity, Map<String, String>> entry : mergePropertiesMap.entrySet()) {
      Map<String, String> properties = entry.getValue();
      MergeProperties mergeProperties = new MergeProperties(
          properties.get(MinionConstants.MergeRollupTask.MERGE_TYPE_KEY).toUpperCase(),
          TimeUtils.convertPeriodToMillis(properties.get(MinionConstants.MergeRollupTask.BUFFER_TIME)),
          Long.parseLong(properties.get(MinionConstants.MergeRollupTask.MAX_NUM_RECORDS_PER_SEGMENT)),
          Long.parseLong(properties.get(MinionConstants.MergeRollupTask.MAX_NUM_RECORDS_PER_TASK)));
      allMergeProperties.put(entry.getKey(), mergeProperties);
    }
    return allMergeProperties;
  }

  private static String getAggregateColumn(String rollupAggregateConfigKey) {
    return rollupAggregateConfigKey.split(
        MinionConstants.MergeRollupTask.AGGREGATE_KEY_PREFIX + MinionConstants.DOT_SEPARATOR)[1];
  }

  private static Pair<Granularity, String> getGranularityAndPropertyPair(
      String mergePropertyConfigKey, String mergePropertyConfigValue) {
    String[] components = StringUtils.split(mergePropertyConfigKey, MinionConstants.DOT_SEPARATOR);
    Preconditions.checkState(components.length == 3);
    Preconditions.checkState(isValidMergeProperties(components[2]));
    if (components[2].equals(MinionConstants.MergeRollupTask.MERGE_TYPE_KEY)) {
      Preconditions.checkState(isValidMergeType(mergePropertyConfigValue));
    }
    return new Pair<>((Granularity.valueOf(components[1].toUpperCase())), components[2]);
  }

  private static boolean isValidMergeProperties(String property) {
    for (String validProperty : validMergeProperties) {
      if (property.equals(validProperty)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isValidMergeType(String mergeType) {
    for (String validMergeType : validMergeType) {
      if (mergeType.toUpperCase().equals(validMergeType)) {
        return true;
      }
    }
    return false;
  }
}
