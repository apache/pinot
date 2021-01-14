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
package org.apache.pinot.controller.api.access;

import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;


@InterfaceAudience.Public
@InterfaceStability.Stable
public interface AccessControl {

  /**
   * Return whether the client has data access to the given table.
   *
   * Note: This method is only used for read access. It will be deprecated soon and its usage will be replaced by
   * `hasAccess` method with AccessType.READ.
   *
   * @param httpHeaders Http headers
   * @param tableName Name of the table to be accessed
   * @return Whether the client has data access to the table
   */
  boolean hasDataAccess(HttpHeaders httpHeaders, String tableName);

  /**
   * Return whether the client has permission to the given table
   *
   * @param accessType access type
   * @param httpHeaders HTTP headers
   * @param tableName Name of the table to be accessed
   * @return whether the client has permission
   */
  default boolean hasAccess(AccessType accessType, HttpHeaders httpHeaders, String tableName) {
    return true;
  }

  /**
   * Return whether the client has permission to access the endpoints which are not table level
   *
   * @param accessType access type
   * @param httpHeaders HTTP headers
   * @return whether the client has permission
   */
  default boolean hasAccess(AccessType accessType, HttpHeaders httpHeaders) {
    return true;
  }
}
