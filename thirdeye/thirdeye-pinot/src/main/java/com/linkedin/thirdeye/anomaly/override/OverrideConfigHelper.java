/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.anomaly.override;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.dto.OverrideConfigDTO;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OverrideConfigHelper {
  private static final Logger LOG = LoggerFactory.getLogger(OverrideConfigHelper.class);

  public static final String TARGET_COLLECTION = "collection";
  public static final String TARGET_METRIC = "metric";
  public static final String TARGET_FUNCTION_ID = "functionId";
  public static final String EXCLUDED_COLLECTION = "excludedCollection";
  public static final String EXCLUDED_METRIC = "excludedMetric";
  public static final String EXCLUDED_FUNCTION_ID = "excludedFunctionId";

  private static final String[] TARGET_KEYS =
      new String[] { TARGET_COLLECTION, TARGET_METRIC, TARGET_FUNCTION_ID };
  private static final String[] EXCLUDED_KEYS =
      new String[] { EXCLUDED_COLLECTION, EXCLUDED_METRIC, EXCLUDED_FUNCTION_ID };

  public static final String ENTITY_TIME_SERIES = "TimeSeries";
  public static final String ENTITY_ALERT_FILTER = "AlertFilter";

  /**
   * Check if the override configuration should be enabled for the given collection name, metric
   * name, and function id of the entity.
   *
   * @param entityTargetLevel the map that provides the collection name, metric name, and function
   *                          id of the entity
   * @param configurationOverrideDTO the filter rule for the override configuration
   *
   * @return true if this override configuration should be enabled for the given entity level
   */
  public static boolean isEnabled(Map<String, String> entityTargetLevel,
      OverrideConfigDTO configurationOverrideDTO) {

    Map<String, List<String>> targetLevel = configurationOverrideDTO.getTargetLevel();
    if (MapUtils.isEmpty(targetLevel)) {
      return true;
    }

    // Check if the given entity should be excluded
    for (String excludedKey : EXCLUDED_KEYS) {
      List<String> elements = targetLevel.get(excludedKey);
      if (CollectionUtils.isNotEmpty(elements) && elements.contains(entityTargetLevel.get(excludedKey))) {
        return false;
      }
    }

    // If the entire include level is empty, then enable the override rule for everything
    boolean includeAll = true;
    for (String targetKey : TARGET_KEYS) {
      if (targetLevel.containsKey(targetKey)) {
        includeAll = false;
        break;
      }
    }
    if (includeAll) {
      return true;
    }

    // Check if the override rule should be enabled for the given entity
    for (String targetKey : TARGET_KEYS) {
      List<String> elements = targetLevel.get(targetKey);
      if (CollectionUtils.isNotEmpty(elements) && elements.contains(entityTargetLevel.get(targetKey))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns a map that provides the information of the entity, which consists of collection name,
   * metric name, and function id (if any).
   *
   * @param collection the collection name of the entity to be overridden
   * @param metric the metric name of the entity to be overridden
   * @param functionId the function id of the entity to be overridden
   * @return a map that provides the information of the entity
   */
  public static Map<String, String> getEntityTargetLevel(String collection, String metric,
      long functionId) {

    Map<String, String> targetEntity = new HashMap<>();
    targetEntity.put(TARGET_COLLECTION, collection);
    targetEntity.put(EXCLUDED_COLLECTION, collection);
    targetEntity.put(TARGET_METRIC, metric);
    targetEntity.put(EXCLUDED_METRIC, metric);
    String functionIdString = Long.toString(functionId);
    targetEntity.put(TARGET_FUNCTION_ID, functionIdString);
    targetEntity.put(EXCLUDED_FUNCTION_ID, functionIdString);
    return targetEntity;
  }

  /**
   * Convert a list of OverrideConfigDTO to a list of scaling factor, in which each scaling factor
   * are filtered through target level.
   *
   * @param overrideConfigDTOs the list of OverrideConfigDTO
   * @param timeSereisTargetLevel the
   *                              filtration rule for applying OverrideConfigDTO
   * @return a list of scaling factor
   */
  public static List<ScalingFactor> convertToScalingFactors(
      List<OverrideConfigDTO> overrideConfigDTOs, Map<String, String> timeSereisTargetLevel) {
    List<ScalingFactor> results = new ArrayList<>();
    for (OverrideConfigDTO overrideConfigDTO : overrideConfigDTOs) {
      if (OverrideConfigHelper.isEnabled(timeSereisTargetLevel, overrideConfigDTO)) {
        long startTime = overrideConfigDTO.getStartTime();
        long endTime = overrideConfigDTO.getEndTime();

        if (MapUtils.isNotEmpty(overrideConfigDTO.getOverrideProperties())) {
          try {
            double scalingFactor =
                Double.parseDouble(overrideConfigDTO.getOverrideProperties().get(ScalingFactor.SCALING_FACTOR));
            ScalingFactor sf = new ScalingFactor(startTime, endTime, scalingFactor);
            results.add(sf);
          } catch (Exception e) {
            LOG.warn("Failed to parse scaling factor from override config:{}, Exception: {}",
                overrideConfigDTO, e);
          }
        } else {
          LOG.warn("Unable to parse scaling factor due to empty override properties. Config:{}",
              overrideConfigDTO);
        }
      }
    }
    return results;
  }

  /**
   * Get a list of OverrideConfigDTOs according to the given start and end time ranges.
   *
   * @param startEndTimeRanges a list of start and end time ranges for retrieving override configs
   * @param overrideConfigDAO the data access object for retrieving override configs
   *
   * @return a list of OverrideConfigDTOs
   */
  public static List<OverrideConfigDTO> getTimeSeriesOverrideConfigs(
      List<Pair<Long, Long>> startEndTimeRanges, OverrideConfigManager overrideConfigDAO) {
    // The Set is used to prevent duplicate override configs are loaded, which could happen if
    // there exists an override config that overlaps both time ranges of current and baseline
    // values
    Set<OverrideConfigDTO> overrideConfigDTOSet = new HashSet<>();

    for (Pair<Long, Long> startEndTimeRange : startEndTimeRanges) {
      List<OverrideConfigDTO> overrideConfigDTOList = overrideConfigDAO
          .findAllConflictByTargetType(OverrideConfigHelper.ENTITY_TIME_SERIES,
              startEndTimeRange.getFirst(), startEndTimeRange.getSecond());
      for (OverrideConfigDTO overrideConfig : overrideConfigDTOList) {
        if (overrideConfig.isActive()) {
          overrideConfigDTOSet.add(overrideConfig);
        }
      }
    }

    List<OverrideConfigDTO> results = new ArrayList<>(overrideConfigDTOSet);
    return results;
  }

  /**
   * Returns the scaling factor for the given collectoin, metric, function id, and the time
   * ranges of current value and baseline values, which is specified in startEndTimeRanges.
   *
   * @param overrideConfigDAO the data access object for retrieving override configs
   * @param collection the target collection
   * @param metric the target metric
   * @param functionId the target function id
   * @param startEndTimeRanges the time ranges of current and baseline values
   *
   * @return the scaling factor for the given collectoin, metric, function id, and the time
   * ranges of current value and baseline values
   */
  public static List<ScalingFactor> getTimeSeriesScalingFactors(OverrideConfigManager
      overrideConfigDAO, String collection, String metric,
      long functionId, List<Pair<Long, Long>> startEndTimeRanges) {

    List<OverrideConfigDTO> overrideConfigs = OverrideConfigHelper.getTimeSeriesOverrideConfigs(
        startEndTimeRanges, overrideConfigDAO);

    // timeSeriesTargetLevel is used for check if the scaling factor should be apply on THIS
    // collection, metric, and function id
    Map<String, String> timeSeriesTargetLevel =
        OverrideConfigHelper.getEntityTargetLevel(collection, metric, functionId);

    // Convert override config to scaling factor
    List<ScalingFactor> scalingFactors = OverrideConfigHelper
        .convertToScalingFactors(overrideConfigs, timeSeriesTargetLevel);

    if (CollectionUtils.isNotEmpty(scalingFactors)) {
      LOG.info("Found {} scaling-factor rules for collection {}, metric {}, function {}",
          scalingFactors.size(), collection, metric, functionId);
    }

    return scalingFactors;
  }
}
