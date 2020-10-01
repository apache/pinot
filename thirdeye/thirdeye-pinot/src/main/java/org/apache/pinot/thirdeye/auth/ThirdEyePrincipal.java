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

import java.security.Principal;

public class ThirdEyePrincipal implements Principal {

  private final String name; // 'username@domainName'
  private final String sessionKey;

  public ThirdEyePrincipal(final String name) {
    this(name, null);
  }

  public ThirdEyePrincipal(final String name, final String sessionKey) {
    this.name = name;
    this.sessionKey = sessionKey;
  }

  @Override
  public String getName() {
    return name;
  }

  public String getSessionKey() {
    return sessionKey;
  }
}
