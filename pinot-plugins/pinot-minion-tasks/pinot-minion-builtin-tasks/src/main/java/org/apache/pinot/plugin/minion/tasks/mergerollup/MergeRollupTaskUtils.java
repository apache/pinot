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
package org.apache.pinot.plugin.minion.tasks.mergerollup;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.MergeTask;


public class MergeRollupTaskUtils {
  private MergeRollupTaskUtils() {
  }

  //@formatter:off
  private static final String[] VALID_CONFIG_KEYS = {
      MergeTask.BUCKET_TIME_PERIOD_KEY,
      MergeTask.BUFFER_TIME_PERIOD_KEY,
      MergeTask.ROUND_BUCKET_TIME_PERIOD_KEY,
      MergeTask.MERGE_TYPE_KEY,
      MergeTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY,
      MergeTask.MAX_NUM_RECORDS_PER_TASK_KEY,
      MergeTask.MAX_NUM_PARALLEL_BUCKETS,
      MinionConstants.MergeRollupTask.ERASE_DIMENSION_VALUES_KEY,
  };
  //@formatter:on

  /**
   * Extracts a map from merge level to config from the task config.
   * <p>The config for a specific level should have key of format "{level}.{configKey}" within the task config.
   */
  public static Map<String, Map<String, String>> getLevelToConfigMap(Map<String, String> taskConfig) {
    Map<String, Map<String, String>> levelToConfigMap = new TreeMap<>();

    // Regex to match aggregation function parameter keys
    Pattern pattern = Pattern.compile("(\\w+)\\.aggregationFunctionParameters\\.(\\w+)\\.(\\w+)");

    for (Map.Entry<String, String> entry : taskConfig.entrySet()) {
      String key = entry.getKey();
      for (String configKey : VALID_CONFIG_KEYS) {
        if (key.endsWith(configKey)) {
          String level = key.substring(0, key.length() - configKey.length() - 1);
          levelToConfigMap.computeIfAbsent(level, k -> new TreeMap<>()).put(configKey, entry.getValue());
        } else {
          Matcher matcher = pattern.matcher(key);
          if (matcher.matches()) {
            String level = matcher.group(1).trim();  // e.g., "1day" or "1hour"
            String metric = matcher.group(2).trim(); // e.g., "metricColumnA" or "metricColumnB"
            String param = matcher.group(3).trim();  // e.g., "nominalEntries" or "p"
            String metricParam = MergeTask.AGGREGATION_FUNCTION_PARAMETERS_PREFIX + metric + "." + param;
            levelToConfigMap.computeIfAbsent(level, k -> new TreeMap<>()).put(metricParam, entry.getValue());
          }
        }
      }
    }
    return levelToConfigMap;
  }

  /**
   * Extracts an array of dimensions to reduce/erase from the task config.
   * <p>The config for the dimensions to erase should be a comma-separated string value.
   */
  public static Set<String> getDimensionsToErase(Map<String, String> taskConfig) {
    if (taskConfig == null || taskConfig.get(MinionConstants.MergeRollupTask.ERASE_DIMENSION_VALUES_KEY) == null) {
      return new HashSet<>();
    }
    return Arrays.stream(taskConfig.get(MinionConstants.MergeRollupTask.ERASE_DIMENSION_VALUES_KEY).split(","))
        .map(String::trim)
        .collect(Collectors.toSet());
  }
}
