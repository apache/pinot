package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.TimeRange;

import java.nio.ByteBuffer;

public interface MetricStoreListener
{
  void notifyDelete(TimeRange timeRange);
  void notifyCreate(TimeRange timeRange, ByteBuffer buffer);
}
