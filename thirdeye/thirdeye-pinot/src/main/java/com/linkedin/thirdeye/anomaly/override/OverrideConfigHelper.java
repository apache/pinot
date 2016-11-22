package com.linkedin.thirdeye.anomaly.override;

import com.linkedin.thirdeye.datalayer.dto.OverrideConfigDTO;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.joda.time.DateTime;

public class OverrideConfigHelper {
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
}
