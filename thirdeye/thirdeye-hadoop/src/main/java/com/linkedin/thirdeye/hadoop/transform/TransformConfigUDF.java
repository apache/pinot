package com.linkedin.thirdeye.hadoop.transform;

import org.apache.hadoop.mapreduce.Job;

/**
 * Simple interface to transform a Generic Record
 */
public interface TransformConfigUDF {

  /**
   * @param record
   * @return
   */
  void setTransformConfig(Job job);
}
