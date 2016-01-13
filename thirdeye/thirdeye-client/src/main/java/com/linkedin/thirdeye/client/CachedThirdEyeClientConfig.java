package com.linkedin.thirdeye.client;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Objects;

public class CachedThirdEyeClientConfig {
  private boolean expireAfterAccess = true;
  private long expirationTime = 60;
  private TimeUnit expirationUnit = TimeUnit.SECONDS;
  private boolean useCacheForExecuteMethod = true;

  public CachedThirdEyeClientConfig() {
  }

  public boolean isExpireAfterAccess() {
    return expireAfterAccess;
  }

  public CachedThirdEyeClientConfig setExpireAfterAccess(boolean expireAfterAccess) {
    this.expireAfterAccess = expireAfterAccess;
    return this;
  }

  public long getExpirationTime() {
    return expirationTime;
  }

  public CachedThirdEyeClientConfig setExpirationTime(long expirationTime) {
    this.expirationTime = expirationTime;
    return this;
  }

  public TimeUnit getExpirationUnit() {
    return expirationUnit;
  }

  public CachedThirdEyeClientConfig setExpirationUnit(TimeUnit expirationUnit) {
    this.expirationUnit = expirationUnit;
    return this;
  }

  /**
   * Indicates whether any {@link CachedThirdEyeClient} instances using this config should use a
   * built-in optimization for the {@link ThirdEyeClient#execute(ThirdEyeRequest)} method rather
   * than the underlying client implementation. The default value is true, indicating that the
   * built-in optimization will be used.
   */
  public boolean useCacheForExecuteMethod() {
    return useCacheForExecuteMethod;
  }

  /**
   * Determines whether any {@link CachedThirdEyeClient} instances using this config should use a
   * built-in optimization for the {@link ThirdEyeClient#execute(ThirdEyeRequest)} method rather
   * than the underlying client implementation.
   */

  public void setUseCacheForExecuteMethod(boolean useCache) {
    this.useCacheForExecuteMethod = useCache;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(CachedThirdEyeClientConfig.class)
        .add("expireAfterAccess", expireAfterAccess).add("expirationTime", expirationTime)
        .add("expirationUnit", expirationUnit)
        .add("useCacheForExecuteMethod", useCacheForExecuteMethod).toString();
  }
}
