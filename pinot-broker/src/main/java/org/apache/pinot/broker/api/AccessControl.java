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
package org.apache.pinot.broker.api;

import java.util.Set;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;


@InterfaceAudience.Public
@InterfaceStability.Stable
public interface AccessControl {
  /**
   * First-step access control when processing broker requests. Decides whether request is allowed to acquire resources
   * for further processing. Request may still be rejected at table-level later on.
   *
   * @param requesterIdentity requester identity
   *
   * @return {@code true} if authorized, {@code false} otherwise
   */
  default boolean hasAccess(RequesterIdentity requesterIdentity) {
    return true;
  }

  /**
   * Fine-grained access control on parsed broker request. May check table, column, permissions, etc.
   *
   * @param requesterIdentity requester identity
   * @param brokerRequest broker request (incl query)
   *
   * @return {@code true} if authorized, {@code false} otherwise
   */
  boolean hasAccess(RequesterIdentity requesterIdentity, BrokerRequest brokerRequest);

  /**
   * Fine-grained access control on pinot tables.
   *
   * @param requesterIdentity requester identity
   * @param tables Set of pinot tables used in the query. Table name can be with or without tableType.
   *
   * @return {@code true} if authorized, {@code false} otherwise
   */
  boolean hasAccess(RequesterIdentity requesterIdentity, Set<String> tables);

  /**
   * Checks whether the user has access to perform action on the particular resource
   *
   * @param requesterIdentity requester identity
   * @param targetType type of resource being accessed
   * @param targetId id of the resource
   * @param action type to validate
   * @return true if requester is allowed to perform the action
   */
  default boolean hasAccess(RequesterIdentity requesterIdentity, TargetType targetType, String targetId,
      String action) {
    return true;
  }

  /**
   * If an API is neither annotated with Authorize nor ManualAuthorization,
   * this method will be called to check the default authorization.
   * If the return is false, then API will be terminated by the filter.
   * @return true to allow
   */
  default boolean defaultAccess(RequesterIdentity requesterIdentity) {
    return true;
  }
}
