package com.linkedin.thirdeye.hadoop.push;

import java.util.Properties;

public interface SegmentPushUDF {

  void emitCustomEvents(Properties properties);

}
