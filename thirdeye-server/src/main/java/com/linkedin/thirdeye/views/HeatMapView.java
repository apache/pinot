package com.linkedin.thirdeye.views;

import com.sun.jersey.api.NotFoundException;
import io.dropwizard.views.View;

import java.util.List;

public class HeatMapView extends View
{
  private static final String TITLE = "ThirdEye";

  private final List<String> collections;

  public HeatMapView(List<String> collections)
  {
    super("heat-map.ftl");
    this.collections = collections;

    if (collections == null || collections.isEmpty())
    {
      throw new NotFoundException("No collections loaded");
    }
  }

  public String getTitle()
  {
    return TITLE;
  }

  public List<String> getCollections()
  {
    return collections;
  }
}
