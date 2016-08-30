package com.linkedin.thirdeye.hadoop.transform;

import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTransformConfigUDF implements TransformConfigUDF {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTransformConfigUDF.class);

  @Override
  public void setTransformConfig(Job job) {

  }

}
