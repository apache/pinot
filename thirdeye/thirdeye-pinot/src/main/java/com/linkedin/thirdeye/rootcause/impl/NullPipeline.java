package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.Collections;
import java.util.Map;
import java.util.Set;


/**
 * NullPipeline serves as a dummy implementation or sink that may receive inputs, but does not
 * emit any output. Can be used to construct an validate a DAG without a full implementation
 * of component pipelines.
 */
public class NullPipeline extends Pipeline {
  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   */
  public NullPipeline(String outputName, Set<String> inputNames) {
    super(outputName, inputNames);
  }

  /**
   * Alternate constructor for PipelineLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param ignore configuration properties (none)
   */
  public NullPipeline(String outputName, Set<String> inputNames, Map<String, String> ignore) {
    super(outputName, inputNames);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    return new PipelineResult(context, Collections.<Entity>emptySet());
  }
}
