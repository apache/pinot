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
package org.apache.pinot.spi.ingestion.error;

import java.util.Locale;


/// Where the source gives it, the finer reason behind an [IngestionSourceErrorCategory#AUTH] verdict.
///
/// It is a hint for log lines only, never a metric label: on many paths the line between a rejected identity and a
/// denied permission is invisible (MSK IAM reports a policy denial as a SASL authentication failure, Pulsar's admin
/// client maps 401 and 403 to one class), and both route to the same owner.
public enum IngestionSourceErrorSubReason {
  /// The identity was rejected: a wrong, expired or revoked credential, a bad signature, the wrong auth mechanism.
  AUTHENTICATION,
  /// The identity was accepted but a policy denied the request: an ACL, IAM, a role grant, a trust policy, a network
  /// policy the source enforces.
  AUTHORIZATION,
  /// The source refused to say whether the object exists ("does not exist or not authorized"). Check both.
  MISSING_OR_DENIED,
  /// The source refused us but gave no finer reason, or its reason is known to be unreliable.
  UNSPECIFIED;

  /// The lower-case name used in log lines.
  public String label() {
    return name().toLowerCase(Locale.ROOT);
  }
}
