package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * NormalizationPipeline normalizes entity scores to a [0.0,1.0] interval based on observed
 * minimum and maximum scores.
 */
public class NormalizationPipeline extends Pipeline {
  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   */
  public NormalizationPipeline(String outputName, Set<String> inputNames) {
    super(outputName, inputNames);
  }

  /**
   * Alternate constructor for RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param ignore configuration properties (none)
   */
  public NormalizationPipeline(String outputName, Set<String> inputNames, Map<String, Object> ignore) {
    super(outputName, inputNames);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    return new PipelineResult(context, EntityUtils.normalizeScores(context.filter(Entity.class)));
  }
}
