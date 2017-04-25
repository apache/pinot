package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class TopKPipeline extends Pipeline {
  public static final String PROP_K = "k";
  public static final String PROP_CLASS = "class";

  final int k;
  final Class<? extends Entity> clazz;

  public TopKPipeline(String name, Set<String> inputs, Class<? extends Entity> clazz, int k) {
    super(name, inputs);
    this.k = k;
    this.clazz = clazz;
  }

  public TopKPipeline(String name, Set<String> inputs, Map<String, String> properties) throws Exception {
    super(name, inputs);
    this.k = Integer.parseInt(properties.get(PROP_K));
    this.clazz = (Class<? extends Entity>)Class.forName(properties.get(PROP_CLASS));
  }

  @Override
  public PipelineResult run(PipelineContext input) {
    List<Entity> entities = new ArrayList<>(EntityUtils.filterContext(input, this.clazz));
    Collections.sort(entities, Entity.HIGHEST_SCORE_FIRST);

    return new PipelineResult(input, new HashSet<>(entities.subList(0, Math.min(entities.size(), this.k))));
  }
}
