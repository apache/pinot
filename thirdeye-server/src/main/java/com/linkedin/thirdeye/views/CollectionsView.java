package com.linkedin.thirdeye.views;

import io.dropwizard.views.View;

import java.util.List;

public class CollectionsView extends View
{
  private final List<String> collections;

  public CollectionsView(List<String> collections)
  {
    super("collections-view.ftl");
    this.collections = collections;
  }

  public List<String> getCollections()
  {
    return collections;
  }
}
