package org.apache.pinot.spi.accounting;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;


@JsonSerialize
public interface ThreadResourceTracker {
  long getCPUTimeMS();

  long getMemoryAllocationBytes();

  String getQueryId();

  int getTaskId();
}
