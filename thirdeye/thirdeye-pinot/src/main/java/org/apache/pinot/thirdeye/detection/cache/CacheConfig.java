package org.apache.pinot.thirdeye.detection.cache;

public class CacheConfig {
  private static boolean useCentralizedCache = false;
  private static boolean useInMemoryCache = true;

  private static final String COUCHBASE_AUTH_USERNAME = "thirdeye";
  private static final String COUCHBASE_AUTH_PASSWORD = "thirdeye";
  private static final String COUCHBASE_BUCKET_NAME = "travel-sample";

  public static boolean useCentralizedCache() { return useCentralizedCache; }
  public static boolean useInMemoryCache() { return useInMemoryCache; }
  public static String getCouchbaseAuthUsername() {
    return COUCHBASE_AUTH_USERNAME;
  }
  public static String getCouchbaseAuthPassword() {
    return COUCHBASE_AUTH_PASSWORD;
  }
  public static String getCouchbaseBucketName() {
    return COUCHBASE_BUCKET_NAME;
  }
}
