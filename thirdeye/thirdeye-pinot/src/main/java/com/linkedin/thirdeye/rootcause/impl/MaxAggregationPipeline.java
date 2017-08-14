package com.linkedin.thirdeye.rootcause.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.StringSeries;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of an aggregator that handles the same entity being returned from multiple
 * pipelines by selecting the entity with the highest score. It optionally truncates the
 * number of returned entities to the top k by score.
 */
public class MaxAggregationPipeline extends Pipeline {
  private static Logger LOG = LoggerFactory.getLogger(MaxAggregationPipeline.class);

  private final static String PROP_K = "k";
  private final static int PROP_K_DEFAULT = -1;

  private final int k;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param k top k truncation before aggregation ({@code -1} for unbounded)
   */
  public MaxAggregationPipeline(String outputName, Set<String> inputNames, int k) {
    super(outputName, inputNames);
    this.k = k;
  }

  /**
   * Alternate constructor for use by RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_K})
   */
  public MaxAggregationPipeline(String outputName, Set<String> inputNames, Map<String, Object> properties) {
    super(outputName, inputNames);
    this.k = MapUtils.getIntValue(properties, PROP_K, PROP_K_DEFAULT);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Multimap<String, Entity> entities = ArrayListMultimap.create();
    for(Map.Entry<String, Set<Entity>> input : context.getInputs().entrySet()) {
      for(Entity e : input.getValue()) {
        entities.put(e.getUrn(), e);
      }
    }

    Set<Entity> output = new HashSet<>();
    for(String urn : entities.keySet()) {
      double maxScore = Double.MIN_VALUE;
      Entity maxEntity = null;

      Collection<Entity> values = entities.get(urn);
      List<Entity> related = new ArrayList<>();

      for(Entity e : values) {
        if(maxScore < e.getScore()) {
          maxScore = e.getScore();
          maxEntity = e;
        }
        related.addAll(e.getRelated());
      }

      if(maxEntity == null) {
        LOG.warn("Could not determine max for urn '{}'. Skipping.", urn);
        continue;
      }

      output.add(maxEntity.withScore(maxScore).withRelated(related));
    }

    return new PipelineResult(context, EntityUtils.topk(output, this.k));
  }
}
