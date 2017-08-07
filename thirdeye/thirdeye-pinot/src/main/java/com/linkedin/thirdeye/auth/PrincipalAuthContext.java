package com.linkedin.thirdeye.auth;

import java.security.Principal;
import java.util.Set;


public class PrincipalAuthContext implements Principal {
  String principal;
  Set<String> groups;

  @Override
  public String getName() {
    return principal;
  }

  public String getPrincipal() {
    return principal;
  }

  public void setPrincipal(String principal) {
    this.principal = principal;
  }

  public Set<String> getGroups() {
    return groups;
  }

  public void setGroups(Set<String> groups) {
    this.groups = groups;
  }
}
