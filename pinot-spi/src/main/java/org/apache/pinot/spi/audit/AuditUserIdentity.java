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
package org.apache.pinot.spi.audit;

import javax.annotation.Nullable;


/**
 * Represents a resolved user identity for audit logging purposes.
 * <p>
 * This interface allows {@link AuditTokenResolver} implementations to return
 * structured identity information that can be extended in the future
 * (e.g., roles, groups) without breaking the SPI contract.
 */
@FunctionalInterface
public interface AuditUserIdentity {

  /**
   * Returns the principal (user identifier) for this identity.
   *
   * @return the principal, or {@code null} if not available
   */
  @Nullable
  String getPrincipal();
}
