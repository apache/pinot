package com.linkedin.thirdeye.anomaly;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.StarTreeConfig;

import java.io.IOException;
import java.util.Properties;

public interface AnomalyResultHandler
{
  /**
   * Initializes this function with the star tree config and arbitrary function config
   */
  void init(StarTreeConfig starTreeConfig, Properties handlerConfig);

  /**
   * Processes the result of an {@link AnomalyDetectionFunction}
   *
   * <p>
   *   This could be persisting to a database, calling an HTTP endpoint, etc.
   * </p>
   *
   * @throws IOException
   *  If there was an error in handing the result
   */
  void handle(DimensionKey dimensionKey, AnomalyResult result) throws IOException;
}
