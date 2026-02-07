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
 * Service Provider Interface for resolving user identity from authentication tokens
 * for audit logging purposes.
 * <p>
 * Implementations can handle custom token formats (e.g., proprietary tokens, API keys)
 * that are not standard JWTs. The resolver receives the full Authorization header value
 * and returns the principal if it can handle the token format.
 * <p>
 * If the resolver cannot handle the token format, it should return null to allow
 * fallback to the default JWT-based resolution.
 */
public interface AuditTokenResolver {

  /**
   * Resolves the principal from an authorization header value.
   * <p>
   * The implementation should:
   * <ul>
   *   <li>Return the principal (user identifier) if it can handle the token format</li>
   *   <li>Return null if it cannot handle the token format (to allow fallback)</li>
   * </ul>
   *
   * @param authHeaderValue the full Authorization header value (e.g., "Bearer &lt;token&gt;")
   * @return the principal if resolved, null otherwise
   */
  @Nullable
  String resolve(String authHeaderValue);
}
