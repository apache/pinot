package com.linkedin.pinot.common.utils.time;

import com.linkedin.pinot.common.data.TimeGranularitySpec;


public class TimeConverterProvider {

  public static TimeConverter getTimeConverterFromGranularitySpecs(TimeGranularitySpec incoming, TimeGranularitySpec outgoing) {
    return new NoOpTimeConverter(incoming);
  }
}
