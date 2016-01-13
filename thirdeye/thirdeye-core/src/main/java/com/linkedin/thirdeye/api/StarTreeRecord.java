package com.linkedin.thirdeye.api;

import java.util.Collection;
import java.util.Map;

public interface StarTreeRecord {
  StarTreeConfig getConfig();

  DimensionKey getDimensionKey();

  MetricTimeSeries getMetricTimeSeries();

  /** @return a deep copy of this instance with dimension name == "*" */
  StarTreeRecord relax(String dimensionName);

  /** @return a deep copy of this instance with dimension names == "*" */
  StarTreeRecord relax(Collection<String> dimensionNames);

  StarTreeRecord aliasOther(String dimensionName);

  /**
   * @return a deep copy of this instance with all named dimensions unconditionally aliased to other
   */
  StarTreeRecord aliasOther(Collection<String> otherDimensionNames);
}
