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
package org.apache.pinot.common.request.context;


/// Authenticated native query admission independent of a query transport. The transport supplies the fingerprint
/// of its TLS-verified peer certificate, never an identity from request metadata. Implementations validate current
/// membership, table authority and serving deadlines; callers must pin exact physical content before execution.
@FunctionalInterface
public interface NativeQueryGate {
  Delivery admit(NativeQueryContext context, String peerCertificateSha256);

  /// Serializes each result callback with authority invalidation and checks the original serving deadline. A check
  /// followed by a separate callback is insufficient: implementations must hold their fence through the callback.
  @FunctionalInterface
  interface Delivery {
    void deliver(Runnable callback);
  }
}
