package com.linkedin.thirdeye.anomalydetection.datafilter;

import java.util.Collections;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
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
