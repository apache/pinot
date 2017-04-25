package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.Collections;
import java.util.Map;
import java.util.Set;


public class NullPipeline extends Pipeline {
  public NullPipeline(String name, Set<String> inputs) {
    super(name, inputs);
  }

  public NullPipeline(String name, Set<String> inputs, Map<String, String> ignore) {
    super(name, inputs);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    return new PipelineResult(context, Collections.<Entity>emptySet());
  }
}
