package org.apache.pinot.controller.helix.core.realtime.segment;

class SystemClock implements Clock {

  @Override
  public long currentTimeMillis() {
    return System.currentTimeMillis();
  }
}
