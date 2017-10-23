package com.linkedin.thirdeye.datasource.pinot;

import com.google.common.cache.CacheLoader;
import com.linkedin.pinot.client.ResultSetGroup;
import java.util.Map;

public abstract class PinotResponseCacheLoader extends CacheLoader<PinotQuery, ResultSetGroup> {
  /**
   * Initializes the cache loader using the given property map.
   *
   * @param properties the property map that provides the information to connect to the data source.
   *
   * @throws Exception when an error occurs connecting to the Pinot controller.
   */
  public abstract void init(Map<String, String> properties) throws Exception;
}
