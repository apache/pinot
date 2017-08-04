package com.linkedin.thirdeye.dashboard.configs;

import java.util.List;


public class AuthConfiguration {

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
}
