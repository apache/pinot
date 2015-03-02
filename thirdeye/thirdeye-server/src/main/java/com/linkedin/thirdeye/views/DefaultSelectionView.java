package com.linkedin.thirdeye.views;

import io.dropwizard.views.View;

import java.util.List;

public class DefaultSelectionView extends View
{
  private final List<String> collections;

  public DefaultSelectionView(List<String> collections)
  {
    super("default-selection-view.ftl");
    this.collections = collections;
  }

  public List<String> getCollections()
  {
    return collections;
  }
}
