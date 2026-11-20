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

import io.netty.handler.ssl.CipherSuiteConverter;
import java.util.Properties;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.common.X509Exception.SSLContextException;
import org.apache.zookeeper.common.ZKConfig;


/// Preserves disabled ZooKeeper hostname verification with Netty 4.2 using ZooKeeper's JDK context supplier hook.
///
/// Pinot's Helix clients use process-wide ZooKeeper TLS bootstrap properties. Call [#initialize()] before creating
/// these clients; existing custom suppliers retain ownership of their context and provider. The vendor's no-argument
/// supplier cannot consume arbitrary per-client TLS overrides, so it is intended for these bootstrap-owned clients.
/// Each context uses ZooKeeper's configured stores, protocol, revocation and FIPS policy, without caching key material.
public class ZooKeeperSslContextSupplier implements Supplier<SSLContext> {
  /// Installs the JDK adapter only when bootstrap configuration explicitly disables hostname verification.
  /// Enabled verification and custom suppliers keep their existing construction paths. TLS bootstrap configuration
  /// must be supplied before client creation; it cannot be obtained from the cluster being connected to.
  public static void initialize() {
    Properties properties = System.getProperties();
    synchronized (properties) {
      try (ClientX509Util util = new ClientX509Util()) {
        ZKConfig config = new ZKConfig();
        if (util.isServerHostnameVerificationEnabled(config)
            || config.getProperty(util.getSslContextSupplierClassProperty()) != null) {
          return;
        }
        // The supplied JDK context needs JSSE names for ciphers previously configured for OpenSSL.
        String cipherSuites = config.getProperty(util.getSslCipherSuitesProperty());
        if (cipherSuites != null) {
          String[] ciphers = cipherSuites.split(",", -1);
          for (int i = 0; i < ciphers.length; i++) {
            String javaCipher = CipherSuiteConverter.toJava(ciphers[i], "TLS");
            if (javaCipher != null) {
              ciphers[i] = javaCipher;
            }
          }
          properties.setProperty(util.getSslCipherSuitesProperty(), String.join(",", ciphers));
        }
        // ZooKeeper 3.9.6 supports supplied SSLContext instances only with its JDK provider.
        properties.setProperty(util.getSslProviderProperty(), "JDK");
        properties.setProperty(util.getSslContextSupplierClassProperty(), ZooKeeperSslContextSupplier.class.getName());
      }
    }
  }

  @Override
  public SSLContext get() {
    try (ClientX509Util util = new ClientX509Util()) {
      // Bypass supplier loading without temporarily removing the process-wide supplier property.
      return util.createSSLContextAndOptionsFromConfig(new ZKConfig()).getSSLContext();
    } catch (SSLContextException e) {
      throw new IllegalStateException("Unable to create the ZooKeeper TLS context from bootstrap configuration", e);
    }
  }
}
