/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.dashboard.configs;

import java.util.Collections;
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
  List<String> domainSuffix = Collections.emptyList();

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

  public List<String> getDomainSuffix() {
    return domainSuffix;
  }

  public void setDomainSuffix(List<String> domainSuffix) {
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
