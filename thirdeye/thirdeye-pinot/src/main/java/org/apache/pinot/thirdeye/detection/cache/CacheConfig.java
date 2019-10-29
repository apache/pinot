package org.apache.pinot.thirdeye.detection.cache;

public class CacheConfig {
  private static boolean useCentralizedCache = true;

  private static final String AUTH_USERNAME = "thirdeye";
  private static final String AUTH_PASSWORD = "thirdeye";
  private static final String BUCKET_NAME = "travel-sample";

  public static boolean useCentralizedCache() {
    return useCentralizedCache;
  }

  public static String getAuthUsername() {
    return AUTH_USERNAME;
  }

  public static String getAuthPassword() {
    return AUTH_PASSWORD;
  }

  public static String getBucketName() {
    return BUCKET_NAME;
  }
}
