package com.linkedin.thirdeye.views;

import io.dropwizard.views.View;

public class HeatMapView extends View
{
  private static final String TITLE = "ThirdEye";

  private final String collection;

  public HeatMapView(String collection)
  {
    super("heat-map-dashboard-view.ftl");
    this.collection = collection;
  }

  public String getTitle()
  {
    return TITLE + " (" + collection + ")";
  }

  public String getCollection()
  {
    return collection;
  }
}
