package com.linkedin.thirdeye.api;

import java.util.Collection;
import java.util.Map;

public interface StarTreeRecord extends Comparable<StarTreeRecord>
{
  /** @return The dimension values of this record */
  Map<String, String> getDimensionValues();

  /** @return The aggregate metric values for this record */
  Map<String, Integer> getMetricValues();

  /** @return The time series information for this record */
  Long getTime();

  /** @return a deep copy of this instance */
  StarTreeRecord copy(boolean keepMetrics);

  /** @return a deep copy of this instance with dimension name == "*" */
  StarTreeRecord relax(String dimensionName);

  /** @return a deep copy of this instance with dimension names == "*" */
  StarTreeRecord relax(Collection<String> dimensionNames);

  StarTreeRecord aliasOther(String dimensionName);

  /** @return a deep copy of this instance with all named dimensions unconditionally aliased to other */
  StarTreeRecord aliasOther(Collection<String> otherDimensionNames);

  String getKey(boolean includeTime);
}
