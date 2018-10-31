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

package com.linkedin.thirdeye.anomaly.alert.grouping.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.datalayer.dto.GroupedAnomalyResultsDTO;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This filter check if the given grouped anomaly has a size exceeds a certain threshold. The threshold could be
 * overridden for different groups; for example, users could specify that the default threshold 3 and it overridden to
 * 4 when group name (dimension name) is "country".
 */
public class SizeSeverityAlertGroupFilter extends BaseAlertGroupFilter {
  private static final Logger LOG = LoggerFactory.getLogger(SizeSeverityAlertGroupFilter.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String THRESHOLD_KEY = "threshold";
  // Override threshold to different dimension map
  public static final String OVERRIDE_THRESHOLD_KEY = "overrideThreshold";

  private static final int DEFAULT_THRESHOLD = 3;

  private int threshold = 3;
  private Map<Set<String>, Integer> overrideThreshold = new HashMap<>();

  // Getters is limited in package level for testing purpose
  int getThreshold() {
    return threshold;
  }

  Map<Set<String>, Integer> getOverrideThreshold() {
    return overrideThreshold;
  }

  @Override
  public void setParameters(Map<String, String> props) {
    super.setParameters(props);

    // Initialize threshold from users' setting
    threshold = DEFAULT_THRESHOLD;
    if (props.containsKey(THRESHOLD_KEY)) {
      threshold = Integer.parseInt(props.get(THRESHOLD_KEY));
    }

    // Initialize the lookup table for overriding thresholds
    if (props.containsKey(OVERRIDE_THRESHOLD_KEY)) {
      String overrideJsonPayLoad = props.get(OVERRIDE_THRESHOLD_KEY);
      try {
        Map<String, Integer> rawOverrideThresholdMap = OBJECT_MAPPER.readValue(overrideJsonPayLoad, HashMap.class);
        for (Map.Entry<String, Integer> overrideThresholdEntry : rawOverrideThresholdMap.entrySet()) {
          String[] dimensionNames = overrideThresholdEntry.getKey().split(",");
          Set<String> dimensionNameSet = new HashSet<>();
          for (String dimensionName : dimensionNames) {
            dimensionNameSet.add(dimensionName.trim());
          }
          Integer threshold = overrideThresholdEntry.getValue();
          overrideThreshold.put(dimensionNameSet, threshold);
        }
      } catch (IOException e) {
        LOG.error("Failed to reconstruct override threshold mappings from this json string: {}", overrideJsonPayLoad);
      }
    }
  }

  @Override
  public boolean isQualified(GroupedAnomalyResultsDTO groupedAnomaly) {
    Set<String> dimensionNames = new HashSet<>();
    dimensionNames.addAll(groupedAnomaly.getDimensions().keySet());
    int threshold = this.threshold;
    if (overrideThreshold.containsKey(dimensionNames)) {
      threshold = overrideThreshold.get(dimensionNames);
    }
    return CollectionUtils.size(groupedAnomaly.getAnomalyResults()) > threshold;
  }
}
