package com.linkedin.thirdeye.client;

import java.util.concurrent.TimeUnit;

public class DefaultThirdEyeClientConfig {
  private boolean expireAfterAccess = true;
  private long expirationTime = 60;
  private TimeUnit expirationUnit = TimeUnit.SECONDS;

  public DefaultThirdEyeClientConfig() {}

  public boolean isExpireAfterAccess() {
    return expireAfterAccess;
  }

  public DefaultThirdEyeClientConfig setExpireAfterAccess(boolean expireAfterAccess) {
    this.expireAfterAccess = expireAfterAccess;
    return this;
  }

  public long getExpirationTime() {
    return expirationTime;
  }

  public DefaultThirdEyeClientConfig setExpirationTime(long expirationTime) {
    this.expirationTime = expirationTime;
    return this;
  }

  public TimeUnit getExpirationUnit() {
    return expirationUnit;
  }

  public DefaultThirdEyeClientConfig setExpirationUnit(TimeUnit expirationUnit) {
    this.expirationUnit = expirationUnit;
    return this;
  }
}
