package com.linkedin.thirdeye.client;

import java.util.List;

public abstract class ThirdEyeClient {

  private String name;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }


  public ThirdEyeClient(String name) {
    this.name = name;
  }

  public abstract ThirdEyeResponse execute(ThirdEyeRequest request) throws Exception;

  public abstract List<String> getCollections() throws Exception;

  /** Clear any cached values. */
  public abstract void clear() throws Exception;

  public abstract void close() throws Exception;

  public abstract long getMaxDataTime(String collection) throws Exception;

}
