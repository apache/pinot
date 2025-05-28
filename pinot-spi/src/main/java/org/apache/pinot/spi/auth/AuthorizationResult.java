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

/**
 * The AuthorizationResult interface defines the contract for authorization results in the Pinot system.
 * Implementations of this interface provide the access status and an optional failure message indicating
 * the reason for denied access.
 */
public interface AuthorizationResult {

  /**
   * Indicates whether the access is granted.
   *
   * @return true if access is granted, false otherwise.
   */
  boolean hasAccess();

  /**
   * Provides the failure message if access is denied.
   *
   * @return A string containing the failure message if access is denied, otherwise an empty string or null.
   */
  String getFailureMessage();
}
