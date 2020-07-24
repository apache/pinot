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

package org.apache.pinot.thirdeye.common.restclient;

import java.util.Optional;


public class ThirdEyeRestClientConfiguration {
  private String adminUser;
  private String sessionKey;
  private static final String DEFAULT_THIRDEYE_SERVICE_NAME = "thirdeye";
  private static final String DEFAULT_THIRDEYE_SERVICE_KEY = "thirdeyeServiceKey";

  public String getAdminUser() {
    return Optional.ofNullable(adminUser).orElse(DEFAULT_THIRDEYE_SERVICE_NAME);
  }

  public void setAdminUser(String adminUser) {
    this.adminUser = adminUser;
  }

  public String getSessionKey() {
    return Optional.ofNullable(sessionKey).orElse(DEFAULT_THIRDEYE_SERVICE_KEY);
  }

  public void setSessionKey(String sessionKey) {
    this.sessionKey = sessionKey;
  }
}
