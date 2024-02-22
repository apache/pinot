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
package org.apache.pinot.common.utils.tls;

import java.security.KeyStore;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import nl.altindag.ssl.SSLFactory;
import org.apache.commons.lang.StringUtils;


public class JvmDefaultSslContext {
  private JvmDefaultSslContext() {
  }

  // TODO: need to support "javax.net.ssl.keyStoreProvider" and "javax.net.ssl.trustStoreProvider" system properties
  static {
    // When either key store "javax.net.ssl.keyStore" or trust store "javax.net.ssl.trustStore" is specified in
    // system property and they are files:
    // set the default SSL context to the default SSL context created by SSLFactory, and enable auto renewal of
    // SSLFactory when either key store or trust store file changes.
    String jvmKeyStorePath = System.getProperty("javax.net.ssl.keyStore");
    String jvmTrustStorePath = System.getProperty("javax.net.ssl.trustStore");
    // Enable auto renewal of SSLFactory when either key store or trust store file is specified.
    if (TlsUtils.isKeyOrTrustStorePathNullOrHasFileScheme(jvmKeyStorePath)
        && TlsUtils.isKeyOrTrustStorePathNullOrHasFileScheme(jvmTrustStorePath)
        && (StringUtils.isNotBlank(jvmKeyStorePath) || StringUtils.isNotBlank(jvmTrustStorePath))) {
      SSLFactory.Builder jvmSslFactoryBuilder =
          SSLFactory.builder().withSystemPropertyDerivedProtocols().withSystemPropertyDerivedCiphers();

      // If key store "javax.net.ssl.keyStore" is specified by system property, create a new SSLFactory with the
      // keyStore
      if (StringUtils.isNotBlank(jvmKeyStorePath)) {
        jvmSslFactoryBuilder.withSwappableIdentityMaterial().withSystemPropertyDerivedIdentityMaterial();
      }

      // If trust store "javax.net.ssl.trustStore" is specified by system property, create a new SSLFactory with the
      // trustStore; otherwise, use the default one.
      if (StringUtils.isNotBlank(jvmTrustStorePath)) {
        jvmSslFactoryBuilder.withSwappableTrustMaterial().withSystemPropertyDerivedTrustMaterial();
      } else {
        // Must use the default one when trust store is not specified since this is the default behavior
        jvmSslFactoryBuilder.withDefaultTrustMaterial();
      }
      SSLFactory jvmSslFactory = jvmSslFactoryBuilder.build();
      SSLContext.setDefault(jvmSslFactory.getSslContext());

      String jvmKeystoreType = Optional.ofNullable(System.getProperty("javax.net.ssl.trustStoreType")).map(String::trim)
          .filter(StringUtils::isNotBlank).orElseGet(KeyStore::getDefaultType);
      String jvmKeystorePassword =
          Optional.ofNullable(System.getProperty("javax.net.ssl.keyStorePassword"))
              .map(String::trim).filter(StringUtils::isNotBlank).orElse(null);
      String jvmTrustStoreType =
          Optional.ofNullable(System.getProperty("javax.net.ssl.trustStoreType"))
              .map(String::trim).filter(StringUtils::isNotBlank).orElseGet(KeyStore::getDefaultType);
      String jvmTrustStorePassword =
          Optional.ofNullable(System.getProperty("javax.net.ssl.trustStorePassword"))
              .map(String::trim).filter(StringUtils::isNotBlank).orElse(null);
      TlsUtils.enableAutoRenewalFromFileStoreForSSLFactory(jvmSslFactory, jvmKeystoreType, jvmKeyStorePath,
          jvmKeystorePassword, jvmTrustStoreType, jvmTrustStorePath, jvmTrustStorePassword, null, null, false);
    }
  }
}
