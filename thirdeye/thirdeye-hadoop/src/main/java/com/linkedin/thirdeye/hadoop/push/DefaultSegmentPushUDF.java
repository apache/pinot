package com.linkedin.thirdeye.hadoop.push;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSegmentPushUDF implements SegmentPushUDF {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultSegmentPushUDF.class);

  @Override
  public void emitCustomEvents(Properties properties) {
    // do nothing
    LOG.info("Default segment push udf");
  }

}
