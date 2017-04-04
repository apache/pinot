package com.linkedin.thirdeye.anomalydetection.datafilter;

import java.util.Collections;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

public class DataFilterFactory {
  private static final String FILTER_TYPE_KEY = "type";

  public DataFilter fromSpec(Map<String, String> spec) {
    if (MapUtils.isEmpty(spec)) {
      spec = Collections.emptyMap();
    }
    DataFilter dataFilter = fromStringType(spec.get(FILTER_TYPE_KEY));
    dataFilter.setParameters(spec);

    return dataFilter;
  }

  private DataFilter fromStringType(String type) {
    String upperCaseType = "";
    if (StringUtils.isNotBlank(type)) {
      upperCaseType = type.toUpperCase();
    }
    switch (upperCaseType) {
    case "": // speed optimization for most use cases
      return new DummyDataFilter();
    case "AVERAGETHRESHOLDDATAFILTER":
      return new AverageThresholdDataFilter();
    default:
      return new DummyDataFilter();
    }
  }
}
