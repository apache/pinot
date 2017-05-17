package com.linkedin.thirdeye.client;

import java.util.List;

public abstract class ThirdEyeClient {

  /**
   * Returns simple name of the sub class
   */
  public String getName() {
    return this.getClass().getSimpleName();
  }

  public abstract ThirdEyeResponse execute(ThirdEyeRequest request) throws Exception;

  public abstract List<String> getCollections() throws Exception;

  /** Clear any cached values. */
  public abstract void clear() throws Exception;

  public abstract void close() throws Exception;

  public abstract long getMaxDataTime(String collection) throws Exception;

}
