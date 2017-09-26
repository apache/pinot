package com.linkedin.thirdeye.auth;

public class AuthCookie {
  String principal;
  String password; // TODO replace with sessionId or token in DB
  long timeCreated;

  public String getPrincipal() {
    return principal;
  }

  public void setPrincipal(String principal) {
    this.principal = principal;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public long getTimeCreated() {
    return timeCreated;
  }

  public void setTimeCreated(long timeCreated) {
    this.timeCreated = timeCreated;
  }
}
