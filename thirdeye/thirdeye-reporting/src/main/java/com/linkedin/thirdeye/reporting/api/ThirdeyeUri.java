package com.linkedin.thirdeye.reporting.api;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;

public class ThirdeyeUri {

  private String dashboardUri;
  private String collection;
  private long aggregationSize;
  private TimeUnit aggregationUnit;
  private List<String> metrics;
  private long startTime;
  private long endTime;


  public ThirdeyeUri(String dashboardUri, String collection, long aggregationSize, TimeUnit aggregationUnit, List<String> metrics, long startTime, long endTime) {
    this.dashboardUri = dashboardUri;
    this.collection = collection;
    this.aggregationSize = aggregationSize;
    this.aggregationUnit = aggregationUnit;
    this.metrics = metrics;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  public URL getThirdeyeUri() throws MalformedURLException {

    List<String> thirdeyeUri = new ArrayList<String>();
    thirdeyeUri.add(dashboardUri);
    thirdeyeUri.add("dashboard");
    thirdeyeUri.add(collection);
    String metricFunction = "AGGREGATE_"+aggregationSize+"_"+aggregationUnit;
    thirdeyeUri.add(metricFunction+"("+Joiner.on(',').join(metrics)+")");
    thirdeyeUri.add("INTRA_DAY");
    thirdeyeUri.add("HEAT_MAP");
    thirdeyeUri.add(String.valueOf(startTime));
    thirdeyeUri.add(String.valueOf(endTime));
    return new URL(Joiner.on('/').join(thirdeyeUri));

  }

}
