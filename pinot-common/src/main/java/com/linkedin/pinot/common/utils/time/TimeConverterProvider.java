package com.linkedin.pinot.common.utils.time;

import com.linkedin.pinot.common.data.GranularitySpec;


public class TimeConverterProvider {

  public static TimeConverter getTimeConverterFromGranularitySpecs(GranularitySpec incoming, GranularitySpec outgoing) {
    return new NoOpTimeConverter(incoming);
  }
}
