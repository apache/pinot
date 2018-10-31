package com.linkedin.thirdeye.rootcause.callgraph;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import com.linkedin.thirdeye.rootcause.Entity;


public class CallGraphEntityFormatter extends RootCauseEntityFormatter {
  private static final String ATTR_INDEX = "index";
  private static final String ATTR_DIMENSIONS = "dimensions";

  @Override
  public boolean applies(Entity entity) {
    return entity instanceof CallGraphEntity;
  }

  @Override
  public RootCauseEntity format(Entity entity) {
    CallGraphEntity e = (CallGraphEntity) entity;

    Multimap<String, String> attributes = ArrayListMultimap.create();

    for (String seriesName : e.getEdge().getSeriesNames()) {
      attributes.put(seriesName, e.getEdge().getString(seriesName, 0));
    }
    attributes.putAll(ATTR_DIMENSIONS, e.getEdge().getSeriesNames());
    attributes.putAll(ATTR_INDEX, e.getEdge().getIndexNames());

    RootCauseEntity out = makeRootCauseEntity(entity, "callgraph", "(callgraph edge)", "");

    out.setAttributes(attributes);

    return out;
  }
}
