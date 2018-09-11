/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.datalayer.entity;

import com.linkedin.thirdeye.datalayer.pojo.SessionBean;


public class SessionIndex extends AbstractIndexEntity {
  String sessionKey;
  SessionBean.PrincipalType principalType;

  public String getSessionKey() {
    return sessionKey;
  }

  public void setSessionKey(String sessionKey) {
    this.sessionKey = sessionKey;
  }

  public SessionBean.PrincipalType getPrincipalType() {
    return principalType;
  }

  public void setPrincipalType(SessionBean.PrincipalType principalType) {
    this.principalType = principalType;
  }
}
