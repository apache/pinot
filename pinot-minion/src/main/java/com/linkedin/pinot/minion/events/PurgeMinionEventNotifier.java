package com.linkedin.pinot.minion.events;

import com.linkedin.pinot.minion.executor.SegmentConversionInfo;


public class PurgeMinionEventNotifier implements MinionEventNotifier {

  public void notifyMinionTaskStart(SegmentConversionInfo segmentConversionInfo) {
    // do nothing
  }

  public void notifyMinionTaskEnd(SegmentConversionInfo segmentConversionInfo) {
    // do nothing
  }
}
