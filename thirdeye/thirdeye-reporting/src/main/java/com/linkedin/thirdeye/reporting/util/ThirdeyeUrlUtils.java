package com.linkedin.thirdeye.reporting.util;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;
import com.linkedin.thirdeye.reporting.api.ScheduleSpec;
import com.linkedin.thirdeye.reporting.api.TableSpec;

public class ThirdeyeUrlUtils {

  public final static String DEFAULT_AGGREGATION_GRANULARITY = "1";
  public final static String DEFAULT_AGGREGATION_UNIT = "HOURS";


  public static URL getThirdeyeUri(String dashboardUri, String collection, ScheduleSpec scheduleSpec, TableSpec tableSpec, long startTime, long endTime) throws MalformedURLException {

    List<String> thirdeyeUri = new ArrayList<String>();
    thirdeyeUri.add(dashboardUri);
    thirdeyeUri.add("dashboard");
    thirdeyeUri.add(collection);
    String metricFunction = "AGGREGATE_"+DEFAULT_AGGREGATION_GRANULARITY+"_"+DEFAULT_AGGREGATION_UNIT;
    thirdeyeUri.add(metricFunction+"("+Joiner.on(',').join(tableSpec.getMetrics())+")");

    if (scheduleSpec.getAggregationSize() == 1 && scheduleSpec.getAggregationUnit() == TimeUnit.HOURS) {
      thirdeyeUri.add("INTRA_DAY");
    } else {
      thirdeyeUri.add("TIME_SERIES_FULL");
    }
    if (tableSpec.getFilter() != null && tableSpec.getFilter().getIncludeDimensions() != null) {
      thirdeyeUri.add("MULTI_TIME_SERIES");
    } else {
      thirdeyeUri.add("HEAT_MAP");
    }
    thirdeyeUri.add(String.valueOf(startTime));
    thirdeyeUri.add(String.valueOf(endTime));
    String uri = Joiner.on('/').join(thirdeyeUri);

    if (tableSpec.getFixedDimensions() != null) {
      List<String> fixedDimensions = new ArrayList<String>();
      for (Entry<String, String> entry : tableSpec.getFixedDimensions().entrySet()) {
        fixedDimensions.add(entry.getKey()+"="+entry.getValue());
      }
      uri = uri + "?" + Joiner.on('&').join(fixedDimensions);
    }

    return new URL(uri);
  }

}
