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
package org.apache.pinot.coordination.ratis.transport;


/// Structured metadata access audit, called once per admitted RPC completion. Outcome describes the response or
/// transport failure; an unknown write outcome is not proof of rollback. Implementations must be non-blocking and
/// must not throw. No configuration contents, certificates, credentials or operation payloads are supplied.
@FunctionalInterface
public interface MetadataAudit {
  void record(String authenticatedClientId, String operation, String outcome);
}
