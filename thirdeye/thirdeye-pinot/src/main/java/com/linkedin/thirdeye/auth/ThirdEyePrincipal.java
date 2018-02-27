package com.linkedin.thirdeye.auth;

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;


public class ThirdEyePrincipal implements Principal {
  String name;
  Set<String> groups = new HashSet<>();

  @Override
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Set<String> getGroups() {
    return groups;
  }

  public void setGroups(Set<String> groups) {
    this.groups = groups;
  }
}
