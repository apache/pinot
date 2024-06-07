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


/**
 * Implementation of the AuthorizationResult interface that provides basic
 * authorization results including access status and failure messages.
 */
public class BasicAuthorizationResultImpl implements AuthorizationResult {

  private static final BasicAuthorizationResultImpl SUCCESS = new BasicAuthorizationResultImpl(true);
  private final boolean _hasAccess;
  private final String _failureMessage;

  /**
   * Constructs a BasicAuthorizationResultImpl with the specified access status and failure message.
   *
   * @param hasAccess      true if access is granted, false otherwise.
   * @param failureMessage the failure message if access is denied.
   */
  public BasicAuthorizationResultImpl(boolean hasAccess, String failureMessage) {
    _hasAccess = hasAccess;
    _failureMessage = failureMessage;
  }

  /**
   * Constructs a BasicAuthorizationResultImpl with the specified access status and an empty failure message.
   *
   * @param hasAccess true if access is granted, false otherwise.
   */
  public BasicAuthorizationResultImpl(boolean hasAccess) {
    _hasAccess = hasAccess;
    _failureMessage = StringUtils.EMPTY;
  }

  /**
   * Creates a BasicAuthorizationResultImpl with access granted and no failure message.
   *
   * @return a BasicAuthorizationResultImpl with access granted and an empty failure message.
   */
  public static BasicAuthorizationResultImpl success() {
    return SUCCESS;
  }

  /**
   * Indicates whether access is granted.
   *
   * @return true if access is granted, false otherwise.
   */
  @Override
  public boolean hasAccess() {
    return _hasAccess;
  }

  /**
   * Provides the failure message if access is denied.
   *
   * @return the failure message if access is denied, otherwise an empty string.
   */
  @Override
  public String getFailureMessage() {
    return _failureMessage;
  }
}
