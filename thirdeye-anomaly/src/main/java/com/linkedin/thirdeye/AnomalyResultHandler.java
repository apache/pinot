package com.linkedin.thirdeye;

import java.io.IOException;

public interface AnomalyResultHandler
{
  /**
   * Processes the result of an {@link com.linkedin.thirdeye.AnomalyDetectionFunction}
   *
   * <p>
   *   This could be persisting to a database, calling an HTTP endpoint, etc.
   * </p>
   *
   * @throws IOException
   *  If there was an error in handing the result
   */
  void handle(AnomalyResult result) throws IOException;
}
