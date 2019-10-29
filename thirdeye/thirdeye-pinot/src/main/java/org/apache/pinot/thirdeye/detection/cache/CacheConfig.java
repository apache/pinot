package org.apache.pinot.thirdeye.detection.cache;

public class CacheConfig {
  private static boolean useCentralizedCache = false;
  private static boolean useInMemoryCache = true;

  public static final String COUCHBASE_AUTH_USERNAME = "thirdeye";
  public static final String COUCHBASE_AUTH_PASSWORD = "thirdeye";
  public static final String COUCHBASE_BUCKET_NAME = "travel-sample";

  // time-to-live for documents in the cache.
  public static final int TTL = 3600;

  public static boolean useCentralizedCache() { return useCentralizedCache; }
  public static boolean useInMemoryCache() { return useInMemoryCache; }
}
