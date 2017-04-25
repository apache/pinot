package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DimensionRewriter extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(DimensionRewriter.class);

  final Map<String, StringMapping> dimensionMappings;

  public DimensionRewriter(String name, Set<String> inputs, Collection<StringMapping> dimensionMappings) {
    super(name, inputs);

    this.dimensionMappings = new HashMap<>();
    for(StringMapping m : dimensionMappings) {
      this.dimensionMappings.put(m.getFrom(), m);
    }
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<DimensionEntity> entities = EntityUtils.filterContext(context, DimensionEntity.class);

    Set<DimensionEntity> output = new HashSet<>();
    for(DimensionEntity e : entities) {
      if(!this.dimensionMappings.containsKey(e.getName())) {
        output.add(e);
      } else {
        StringMapping m = this.dimensionMappings.get(e.getName());
        String newName = m.getTo();
        double newScore = e.getScore() * m.getWeight();
        DimensionEntity n = DimensionEntity.fromDimension(newScore, newName, e.getValue());
        LOG.debug("Rewriting '{}' to '{}'", e.getUrn(), n.getUrn());
        output.add(n);
      }
    }

    return new PipelineResult(context, output);
  }
}
