package com.linkedin.thirdeye;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeManager;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ThirdEyeCreateTask extends Task
{
  private final StarTreeManager manager;

  public ThirdEyeCreateTask(StarTreeManager manager)
  {
    super("create");
    this.manager = manager;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception
  {
    Collection<String> collectionParam = params.get("collection");
    if (collectionParam == null || collectionParam.isEmpty())
    {
      throw new IllegalArgumentException("Must provide collection");
    }

    Collection<String> dimensionParam = params.get("dimension");
    if (dimensionParam.isEmpty())
    {
      throw new IllegalArgumentException("Must provide dimension names");
    }

    Collection<String> metricParam = params.get("metric");
    if (metricParam.isEmpty())
    {
      throw new IllegalArgumentException("Must provide metric names");
    }

    Collection<String> timeColumnParam = params.get("timeColumn");
    if (timeColumnParam == null || timeColumnParam.isEmpty())
    {
      throw new IllegalArgumentException("Must provide timeColumn");
    }

    String collection = collectionParam.iterator().next();
    List<String> dimensionNames = new ArrayList<String>(dimensionParam);
    List<String> metricNames = new ArrayList<String>(metricParam);
    String timeColumnName = timeColumnParam.iterator().next();

    StarTreeConfig config = new StarTreeConfig.Builder()
            .setCollection(collection)
            .setDimensionNames(dimensionNames)
            .setMetricNames(metricNames)
            .setTimeColumnName(timeColumnName)
            .build();

    manager.registerConfig(collection, config);
    manager.create(collection);
  }
}
