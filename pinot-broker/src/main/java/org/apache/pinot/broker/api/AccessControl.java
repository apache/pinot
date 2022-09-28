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

import org.apache.pinot.common.request.BrokerRequest;
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
}
