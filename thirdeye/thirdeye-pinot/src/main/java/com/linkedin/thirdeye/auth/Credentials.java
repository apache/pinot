package com.linkedin.thirdeye.auth;

public class Credentials {
  String principal;
  String password;

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
}
