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
package org.apache.pinot.core.auth;

import javax.ws.rs.core.HttpHeaders;


/**
 * Interface for fine-grained access control.
 */
public interface FineGrainedAccessControl {
  /**
   * Checks whether the user has access to perform action on the particular resource
   *
   * @param httpHeaders HTTP headers
   * @param targetType type of resource being accessed
   * @param targetId id of the resource
   * @param action type to validate
   * @return true if user is allowed to perform the action
   */
  default boolean hasAccess(HttpHeaders httpHeaders, TargetType targetType, String targetId, String action) {
    return true;
  }

  /**
   * If an API is neither annotated with Authorize nor ManualAuthorization,
   * this method will be called to check the default authorization.
   * If the return is false, then API will be terminated by the filter.
   * @return true to allow
   */
  default boolean defaultAccess(HttpHeaders httpHeaders) {
    return true;
  }
}
