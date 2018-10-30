package com.linkedin.thirdeye.detection.yaml;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.detection.annotation.DetectionRegistry;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.MapUtils;



/**
 * The translator converts the alert yaml config into a detection alert config
 * TODO Refactor this to support alert schemes
 */
public class YamlDetectionAlertConfigTranslator {
  private static final String PROP_NAME = "name";
  private static final String PROP_CRON = "cron";
  private static final String PROP_APPLICATION = "application";
  private static final String PROP_TYPE = "type";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_DETECTION_CONFIG_ID = "detectionConfigIds";
  private static final String CRON_SCHEDULE_DEFAULT = "0 21 * * * ? *";
  private static final DetectionRegistry DETECTION_REGISTRY = DetectionRegistry.getInstance();

  /**
   * generate detection alerter from YAML
   * @param alertYamlConfigs yaml configuration of the alerter
   * @param detectionConfigIds detection config ids that should be included in the detection alerter
   * @param existingVectorClocks vector clocks that should be kept in the new alerter
   * @return a detection alert config
   */
  public DetectionAlertConfigDTO generateDetectionAlertConfig(Map<String, Object> alertYamlConfigs,
      Collection<Long> detectionConfigIds, Map<Long, Long> existingVectorClocks) {
    DetectionAlertConfigDTO alertConfigDTO = new DetectionAlertConfigDTO();
    Preconditions.checkArgument(alertYamlConfigs.containsKey(PROP_NAME), "Alert property missing: " + PROP_NAME);

    if (existingVectorClocks == null) {
      existingVectorClocks = new HashMap<>();
    }
    for (long detectionConfigId : detectionConfigIds) {
      if (!existingVectorClocks.containsKey(detectionConfigId)){
        existingVectorClocks.put(detectionConfigId, 0L);
      }
    }
    alertConfigDTO.setVectorClocks(existingVectorClocks);

    alertConfigDTO.setName(MapUtils.getString(alertYamlConfigs, PROP_NAME));
    alertConfigDTO.setCronExpression(MapUtils.getString(alertYamlConfigs, PROP_CRON, CRON_SCHEDULE_DEFAULT));
    alertConfigDTO.setActive(true);
    alertConfigDTO.setApplication(MapUtils.getString(alertYamlConfigs, PROP_APPLICATION));
    alertConfigDTO.setProperties(buildAlerterProperties(alertYamlConfigs, detectionConfigIds));
    return alertConfigDTO;
  }

  private Map<String, Object> buildAlerterProperties(Map<String, Object> alertYamlConfigs, Collection<Long> detectionConfigIds) {
    Map<String, Object> properties = new HashMap<>();
    fillInProperties(properties, alertYamlConfigs);
    properties.put(PROP_DETECTION_CONFIG_ID, detectionConfigIds);
    return properties;
  }

  private void fillInProperties(Map<String, Object> properties, Map<String, Object> alertYamlConfigs) {
    // properties to ignore in the properties
    Set<String> exclusionProperties = new HashSet();
    exclusionProperties.addAll(Arrays.asList(PROP_NAME, PROP_CRON, PROP_APPLICATION));
    for (Map.Entry<String, Object> entry : alertYamlConfigs.entrySet()) {
      if (exclusionProperties.contains(entry.getKey())){
        continue;
      }
      if (entry.getKey().equals(PROP_TYPE)) {
        properties.put(PROP_CLASS_NAME, DETECTION_REGISTRY.lookup(MapUtils.getString(alertYamlConfigs, PROP_TYPE)));
      } else {
        properties.put(entry.getKey(), entry.getValue());
      }
    }
  }
}
