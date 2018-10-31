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

package com.linkedin.thirdeye.anomaly.alert.grouping;

import java.util.Collections;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

public class AlertGrouperFactory {
  public static final String GROUPER_TYPE_KEY = "type";
  private static final AlertGrouper DUMMY_ALERT_GROUPER = new DummyAlertGrouper();

  public enum GrouperType {
    DUMMY, DIMENSIONAL, HORIZONTAL_DIMENSIONAL
  }

  public static AlertGrouper fromSpec(Map<String, String> spec) {
    if (MapUtils.isEmpty(spec)) {
      spec = Collections.emptyMap();
    }
    AlertGrouper alertGrouper = fromStringType(spec.get(GROUPER_TYPE_KEY));
    alertGrouper.setParameters(spec);

    return alertGrouper;
  }

  private static AlertGrouper fromStringType(String type) {
    if (StringUtils.isBlank(type)) { // backward compatibility
      return DUMMY_ALERT_GROUPER;
    }

    GrouperType filterType = GrouperType.DUMMY;
    for (GrouperType enumFilterType : GrouperType.values()) {
      if (enumFilterType.name().compareToIgnoreCase(type) == 0) {
        filterType = enumFilterType;
        break;
      }
    }

    switch (filterType) {
    case DUMMY: // speed optimization for most use cases
      return DUMMY_ALERT_GROUPER;
    case DIMENSIONAL:
      return new DimensionalAlertGrouper();
    case HORIZONTAL_DIMENSIONAL:
      return new HorizontalDimensionalAlertGrouper();
    default:
      return DUMMY_ALERT_GROUPER;
    }
  }
}
