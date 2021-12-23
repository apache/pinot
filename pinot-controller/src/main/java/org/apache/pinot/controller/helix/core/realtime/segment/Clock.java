package org.apache.pinot.controller.helix.core.realtime.segment;

interface Clock {
  long currentTimeMillis();
}
