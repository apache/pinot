package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.ExecutionContext;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineResult;

public class HolidayEventsPipeline implements Pipeline {


  public HolidayEventsPipeline() {

  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public PipelineResult run(ExecutionContext context) {
    // TODO Auto-generated method stub
    return null;
  }

}
