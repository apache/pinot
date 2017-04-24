package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DimensionRewriter extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(DimensionRewriter.class);

  final Map<String, String> nameMapping;

  public DimensionRewriter(String name, Set<String> inputs, Map<String, String> nameMapping) {
    super(name, inputs);
    this.nameMapping = nameMapping;
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<DimensionEntity> entities = EntityUtils.filterContext(context, DimensionEntity.class);

    Set<DimensionEntity> output = new HashSet<>();
    for(DimensionEntity e : entities) {
      if(!this.nameMapping.containsKey(e.getName())) {
        output.add(e);
      } else {
        String newName = this.nameMapping.get(e.getName());
        DimensionEntity n = DimensionEntity.fromDimension(e.getScore(), newName, e.getValue());
        LOG.debug("Rewriting '{}' to '{}'", e.getUrn(), n.getUrn());
        output.add(n);
      }
    }

    return new PipelineResult(context, output);
  }
}
