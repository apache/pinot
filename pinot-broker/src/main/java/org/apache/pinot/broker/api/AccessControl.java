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
import org.apache.pinot.core.auth.FineGrainedAccessControl;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.apache.pinot.spi.auth.BasicAuthorizationResultImpl;
import org.apache.pinot.spi.auth.TableAuthorizationResult;


@InterfaceAudience.Public
@InterfaceStability.Stable
public interface AccessControl extends FineGrainedAccessControl {
  /**
   * First-step access control when processing broker requests. Decides whether request is allowed to acquire resources
   * for further processing. Request may still be rejected at table-level later on.
   * The default implementation is kept to have backward compatibility with the existing implementations
   * @param requesterIdentity requester identity
   *
   * @return {@code true} if authorized, {@code false} otherwise
   */
  @Deprecated
  default boolean hasAccess(RequesterIdentity requesterIdentity) {
    return true;
  }

  /**
   * First-step access control when processing broker requests. Decides whether request is allowed to acquire resources
   * for further processing. Request may still be rejected at table-level later on.
   * The default implementation returns a {@link BasicAuthorizationResultImpl} with the result of the hasAccess() of
   * the implementation
   *
   * @param requesterIdentity requester identity
   *
   * @return {@code AuthorizationResult} with the result of the access control check
   */
  default AuthorizationResult authorize(RequesterIdentity requesterIdentity) {
    return new BasicAuthorizationResultImpl(hasAccess(requesterIdentity));
  }

  /**
   * Fine-grained access control on parsed broker request. May check column, permissions, etc.
   * NOTE: Table Permissions are checked by authorize(RequesterIdentity, Set<String> tables)
   * The default implementation is kept to have backward compatibility with the existing implementations
   * @param requesterIdentity requester identity
   * @param brokerRequest broker request (incl query)
   *
   * @return {@code true} if authorized, {@code false} otherwise
   */
  @Deprecated
  default boolean hasAccess(RequesterIdentity requesterIdentity, BrokerRequest brokerRequest) {
    throw new UnsupportedOperationException(
        "Both hasAccess() and authorize() are not implemented . Do implement authorize() method for new "
            + "implementations.");
  }

  /**
   * Verify access control on parsed broker request.
   * May fine-grained details about the request such column, permissions, etc.
   * NOTE: Table Permissions are checked by authorize(RequesterIdentity, Set<String> tables)
   * The default implementation returns a {@link BasicAuthorizationResultImpl} with the result of the hasAccess() of
   * the implementation
   *
   * @param requesterIdentity requester identity
   * @param brokerRequest broker request (incl query)
   *
   * @return {@code AuthorizationResult} with the result of the access control check
   */
  default AuthorizationResult authorize(RequesterIdentity requesterIdentity, BrokerRequest brokerRequest) {
    return new BasicAuthorizationResultImpl(hasAccess(requesterIdentity, brokerRequest));
  }

  /**
   * Fine-grained access control on pinot tables.
   * The default implementation is kept to have backward compatibility with the existing implementations
   *
   * @param requesterIdentity requester identity
   * @param tables Set of pinot tables used in the query. Table name can be with or without tableType.
   *
   * @return {@code true} if authorized, {@code false} otherwise
   */
  @Deprecated
  default boolean hasAccess(RequesterIdentity requesterIdentity, Set<String> tables) {
    throw new UnsupportedOperationException(
        "Both hasAccess() and authorize() are not implemented . Do implement authorize() method for new "
            + "implementations.");
  }

  /**
   * Verify access control on pinot tables.
   * The default implementation returns a {@link TableAuthorizationResult} with the result of the hasAccess() of the
   * implementation
   *
   * @param requesterIdentity requester identity
   * @param tables Set of pinot tables used in the query. Table name can be with or without tableType.
   *
   * @return {@code TableAuthorizationResult} with the result of the access control check
   */
  default TableAuthorizationResult authorize(RequesterIdentity requesterIdentity, Set<String> tables) {
    // Taking all tables when hasAccess Failed , to not break existing implementations
    // It will say all tables names failed AuthZ even only some failed AuthZ - which is same as just boolean output
    return hasAccess(requesterIdentity, tables) ? TableAuthorizationResult.success()
        : new TableAuthorizationResult(tables);
  }
}
