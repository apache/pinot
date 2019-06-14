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

package org.apache.pinot.thirdeye.auth;

import java.util.Objects;


public class ThirdEyeCredentials {
  String principal;
  String password;
  String token;

  ThirdEyeCredentials(String principal, String password) {
    this.principal = principal;
    this.password = password;
  }

  public ThirdEyeCredentials() {
  }

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

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ThirdEyeCredentials)) {
      return false;
    }
    ThirdEyeCredentials that = (ThirdEyeCredentials) o;
    return Objects.equals(principal, that.principal) && Objects.equals(password, that.password);
  }

  @Override
  public int hashCode() {
    return Objects.hash(principal, password);
  }
}
