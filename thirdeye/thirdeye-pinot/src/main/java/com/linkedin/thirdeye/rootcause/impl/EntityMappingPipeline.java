package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
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


public class EntityMappingPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(EntityMappingPipeline.class);

  final Map<String, StringMapping> urnMappings;
  final Class<? extends Entity> fromClass;

  public EntityMappingPipeline(String name, Set<String> inputs, Collection<StringMapping> urnMappings, Class<? extends Entity> fromClass) {
    super(name, inputs);
    this.fromClass = fromClass;
    this.urnMappings = new HashMap<>();
    for(StringMapping m : urnMappings) {
      this.urnMappings.put(m.getFrom(), m);
    }
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<Entity> entities = EntityUtils.filterContext(context, this.fromClass);

    Set<Entity> output = new HashSet<>();
    for(Entity e : entities) {
      if(this.urnMappings.containsKey(e.getUrn())) {
        StringMapping m = this.urnMappings.get(e.getUrn());
        try {
          double score = e.getScore() * m.getWeight();
          Entity newEntity = EntityUtils.parseURN(m.getTo(), score);
          LOG.info("Mapping {} [{}] {} to {} [{}] {}", e.getScore(), e.getClass().getSimpleName(), e.getUrn(),
              newEntity.getScore(), newEntity.getClass().getSimpleName(), newEntity.getUrn());
          output.add(newEntity);
        } catch (Exception ex) {
          LOG.warn("Exception while mapping entity '{}' to '{}'. Skipping.", e.getUrn(), m.getTo());
        }
      }
    }

    return new PipelineResult(context, output);
  }
}
