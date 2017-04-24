package com.linkedin.thirdeye.rootcause;

import com.linkedin.thirdeye.rootcause.impl.EntityUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class TopKPipeline extends Pipeline {
  final int k;
  final Class<? extends Entity> clazz;

  public TopKPipeline(String name, Set<String> inputs, Class<? extends Entity> clazz, int k) {
    super(name, inputs);
    this.k = k;
    this.clazz = clazz;
  }

  @Override
  public PipelineResult run(PipelineContext input) {
    List<Entity> entities = new ArrayList<>(EntityUtils.filterContext(input, this.clazz));
    Collections.sort(entities, Entity.HIGHEST_SCORE_FIRST);

    return new PipelineResult(input, new HashSet<>(entities.subList(0, Math.min(entities.size(), this.k))));
  }
}
