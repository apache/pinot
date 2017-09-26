package com.linkedin.thirdeye.dashboard.configs;

import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class AuthConfiguration {
  boolean authEnabled;

  String authKey;

  /**
   * ldap://exampleldap.com:639
   */
  String ldapUrl;

  /**
   * @xyz.com
   */
  String domainSuffix;

  /**
   * System admins
   */
  List<String> adminUsers;

  Set<String> allowedPaths = new HashSet<>();

  long cacheTTL; // in seconds

  long cookieTTL; // in seconds

  public Set<String> getAllowedPaths() {
    return allowedPaths;
  }

  public void setAllowedPaths(Set<String> allowedPaths) {
    this.allowedPaths = allowedPaths;
  }

  public boolean isAuthEnabled() {
    return authEnabled;
  }

  public void setAuthEnabled(boolean authEnabled) {
    this.authEnabled = authEnabled;
  }

  public String getAuthKey() {
    return authKey;
  }

  public void setAuthKey(String authKey) {
    this.authKey = authKey;
  }

  public String getLdapUrl() {
    return ldapUrl;
  }

  public void setLdapUrl(String ldapUrl) {
    this.ldapUrl = ldapUrl;
  }

  public String getDomainSuffix() {
    return domainSuffix;
  }

  public void setDomainSuffix(String domainSuffix) {
    this.domainSuffix = domainSuffix;
  }

  public List<String> getAdminUsers() {
    return adminUsers;
  }

  public void setAdminUsers(List<String> adminUsers) {
    this.adminUsers = adminUsers;
  }

  public long getCacheTTL() {
    return cacheTTL;
  }

  public void setCacheTTL(long cacheTTL) {
    this.cacheTTL = cacheTTL;
  }

  public long getCookieTTL() {
    return cookieTTL;
  }

  public void setCookieTTL(long cookieTTL) {
    this.cookieTTL = cookieTTL;
  }
}
