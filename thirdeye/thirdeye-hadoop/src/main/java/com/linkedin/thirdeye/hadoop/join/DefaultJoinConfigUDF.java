package com.linkedin.thirdeye.hadoop.join;

import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultJoinConfigUDF implements JoinConfigUDF {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultJoinConfigUDF.class);

  @Override
  public void setJoinConfig(Job job) {

  }

}
