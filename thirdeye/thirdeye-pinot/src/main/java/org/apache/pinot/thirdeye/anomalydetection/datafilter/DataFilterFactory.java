/*
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

package org.apache.pinot.thirdeye.anomalydetection.datafilter;

import java.util.Collections;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

public class DataFilterFactory {
  public static final String FILTER_TYPE_KEY = "type";
  private static final DataFilter DUMMY_DATA_FILTER = new DummyDataFilter();

  public enum FilterType {
    DUMMY, AVERAGE_THRESHOLD
  }

  public static DataFilter fromSpec(Map<String, String> spec) {
    if (MapUtils.isEmpty(spec)) {
      spec = Collections.emptyMap();
    }
    DataFilter dataFilter = fromStringType(spec.get(FILTER_TYPE_KEY));
    dataFilter.setParameters(spec);

    return dataFilter;
  }

  private static DataFilter fromStringType(String type) {
    if (StringUtils.isBlank(type)) { // backward compatibility
      return DUMMY_DATA_FILTER;
    }

    FilterType filterType = FilterType.DUMMY;
    for (FilterType enumFilterType : FilterType.class.getEnumConstants()) {
      if (enumFilterType.name().compareToIgnoreCase(type) == 0) {
        filterType = enumFilterType;
        break;
      }
    }

    switch (filterType) {
      case DUMMY: // speed optimization for most use cases
        return DUMMY_DATA_FILTER;
      case AVERAGE_THRESHOLD:
        return new AverageThresholdDataFilter();
      default:
        return DUMMY_DATA_FILTER;
    }
  }
}
