package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The EntityMappingPipeline is a generic implementation for emitting Entities based on
 * association with incoming Entities. For example, it may be used to generate "similar" metrics
 * for each incoming MetricEntity based on a per-defined mapping of Entity URNs to other URNs.
 */
public class EntityMappingPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(EntityMappingPipeline.class);

  public static final String PROP_PATH = PipelineLoader.PROP_PATH;

  private final Map<String, StringMapping> urnMappings;

  /**
   * Constructor for dependency injection
   *
   * @param name pipeline name
   * @param inputs pipeline inputs
   * @param urnMappings string mappings from URNs to other URNs
   */
  public EntityMappingPipeline(String name, Set<String> inputs, Collection<StringMapping> urnMappings) {
    super(name, inputs);
    this.urnMappings = StringMapping.toMap(urnMappings);
  }

  /**
   * Alternate constructor for use by PipelineLoader
   *
   * @param name pipeline name
   * @param inputs pipeline inputs
   * @param properties configuration properties ({@code PROP_PARALLELISM})
   */
  public EntityMappingPipeline(String name, Set<String> inputs, Map<String, String> properties) throws IOException {
    super(name, inputs);
    File csv = new File(properties.get(PROP_PATH));
    this.urnMappings = StringMapping.toMap(StringMappingParser.fromCsv(new FileReader(csv), 1.0d));
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<Entity> entities = context.filter(Entity.class);

    Set<Entity> output = new HashSet<>();
    for(Entity e : entities) {
      if(this.urnMappings.containsKey(e.getUrn())) {
        StringMapping m = this.urnMappings.get(e.getUrn());
        try {
          double score = e.getScore() * m.getScore();
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
