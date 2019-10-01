package org.apache.pinot.thirdeye.detection.cache;

import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;


public class LoadTimeseriesRequestContainer {
  private final MetricSlice slice;
  private final long configId;

  public LoadTimeseriesRequestContainer(MetricSlice slice, long configId) {
    this.slice = slice;
    this.configId = configId;
  }

  public long getConfigId() {
    return configId;
  }

  public MetricSlice getSlice() {
    return slice;
  }
}
