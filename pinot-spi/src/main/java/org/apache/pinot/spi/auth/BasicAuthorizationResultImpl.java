/**
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
package org.apache.pinot.spi.auth;

import org.apache.commons.lang3.StringUtils;


public class BasicAuthorizationResultImpl implements AuthorizationResult {
  private boolean _hasAccess;
  private String _failureMessage;

  public BasicAuthorizationResultImpl(boolean hasAccess, String failureMessage) {
    _hasAccess = hasAccess;
    _failureMessage = failureMessage;
  }

  public BasicAuthorizationResultImpl(boolean hasAccess) {
    _hasAccess = hasAccess;
    _failureMessage = StringUtils.EMPTY;
  }

  public static BasicAuthorizationResultImpl noFailureResult() {
    return new BasicAuthorizationResultImpl(true);
  }

  public static BasicAuthorizationResultImpl joinResults(AuthorizationResult result1, AuthorizationResult result2) {
    boolean hasAccess = result1.hasAccess() && result2.hasAccess();
    if (hasAccess) {
      return BasicAuthorizationResultImpl.noFailureResult();
    }
    String failureMessage = result1.getFailureMessage() + " ; " + result2.getFailureMessage();
    failureMessage = failureMessage.trim();
    return new BasicAuthorizationResultImpl(hasAccess, failureMessage);
  }

  public void setHasAccess(boolean hasAccess) {
    _hasAccess = hasAccess;
  }

  @Override
  public boolean hasAccess() {
    return _hasAccess;
  }

  @Override
  public String getFailureMessage() {
    return _failureMessage;
  }

  public void setFailureMessage(String failureMessage) {
    _failureMessage = failureMessage;
  }
}
