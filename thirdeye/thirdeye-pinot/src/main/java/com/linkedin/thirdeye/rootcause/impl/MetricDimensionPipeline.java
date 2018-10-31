package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.MaxScoreSet;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The MetricDimensionPipeline extracts filters of input metrics as DimensionEntities.
 */
public class MetricDimensionPipeline extends Pipeline {

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   */
  public MetricDimensionPipeline(String outputName, Set<String> inputNames) {
    super(outputName, inputNames);
  }

  /**
   * Alternate constructor for RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param ignore configuration properties (ignore)
   */
  public MetricDimensionPipeline(String outputName, Set<String> inputNames, Map<String, Object> ignore) {
    super(outputName, inputNames);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<MetricEntity> metrics = context.filter(MetricEntity.class);
    Set<Entity> output = new MaxScoreSet<>();

    for (MetricEntity metric : metrics) {
      for (Map.Entry<String, String> entry : metric.getFilters().entries()) {
        output.add(DimensionEntity.fromDimension(metric.getScore(), entry.getKey(), entry.getValue(), DimensionEntity.TYPE_GENERATED));
      }
    }

    return new PipelineResult(context, output);
  }
}
