package com.linkedin.thirdeye.views;

import com.sun.jersey.api.NotFoundException;
import io.dropwizard.views.View;

import java.util.List;

public class SnapshotHeatMapView extends View
{
  private static final String TITLE = "Snapshot Heat Map";

  private final List<String> collections;

  public SnapshotHeatMapView(List<String> collections)
  {
    super("snapshot-heat-map.ftl");
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
