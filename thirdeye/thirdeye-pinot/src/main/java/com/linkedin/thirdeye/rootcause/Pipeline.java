package com.linkedin.thirdeye.rootcause;

public interface Pipeline {
  String getName();
  PipelineResult run(ExecutionContext context);
}
