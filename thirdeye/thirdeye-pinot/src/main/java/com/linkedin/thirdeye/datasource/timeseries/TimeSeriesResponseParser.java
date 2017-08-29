package com.linkedin.thirdeye.datasource.timeseries;

import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import java.util.List;

public interface TimeSeriesResponseParser {
  List<TimeSeriesRow> parseResponse(ThirdEyeResponse response);
}
