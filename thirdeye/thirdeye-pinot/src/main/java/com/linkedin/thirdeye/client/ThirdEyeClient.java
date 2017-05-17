package com.linkedin.thirdeye.client;

import java.util.List;

public interface ThirdEyeClient {

  /**
   * Returns simple name of the class
   */
  public String getName();

  public ThirdEyeResponse execute(ThirdEyeRequest request) throws Exception;

  public List<String> getCollections() throws Exception;

  /** Clear any cached values. */
  public void clear() throws Exception;

  public void close() throws Exception;

  public long getMaxDataTime(String collection) throws Exception;

}
