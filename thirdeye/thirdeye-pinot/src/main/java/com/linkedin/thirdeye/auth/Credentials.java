package com.linkedin.thirdeye.auth;

import java.util.Objects;


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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Credentials)) {
      return false;
    }
    Credentials that = (Credentials) o;
    return Objects.equals(principal, that.principal) && Objects.equals(password, that.password);
  }

  @Override
  public int hashCode() {
    return Objects.hash(principal, password);
  }
}
