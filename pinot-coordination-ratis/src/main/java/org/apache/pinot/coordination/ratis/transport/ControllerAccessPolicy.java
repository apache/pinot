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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.HexFormat;
import javax.annotation.Nullable;


/// Current controller-only authorization policy. Returning null denies access. Implementations must be thread-safe,
/// non-blocking and consult current policy on every call; workers and public users have no access to this full image.
@FunctionalInterface
public interface ControllerAccessPolicy {
  @Nullable
  ControllerIdentity authorize(String certificateFingerprint);

  /// Stable authenticated client identity and authorization generation. Change accessScopeId whenever access changes;
  /// old snapshot handles and cursors then fail scope checks even if a certificate remains connected.
  record ControllerIdentity(String clientId, String accessScopeId) {
    public ControllerIdentity {
      if (clientId == null || clientId.isBlank() || clientId.length() > 128 || accessScopeId == null
          || accessScopeId.isBlank() || accessScopeId.length() > 128) {
        throw new IllegalArgumentException("Controller identity and access scope must contain 1 to 128 characters");
      }
    }
  }

  static String fingerprint(X509Certificate certificate)
      throws CertificateEncodingException {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded()));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required by the JVM", e);
    }
  }
}
