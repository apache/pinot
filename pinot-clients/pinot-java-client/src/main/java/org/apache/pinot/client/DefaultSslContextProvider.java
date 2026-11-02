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
package org.apache.pinot.client;

import java.util.List;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;


/// Default SSL context provider that uses the JDK/BCJSSE stack and disables OpenSSL.
///
/// When an [SSLContext] is supplied, every engine verifies that the broker certificate matches the host the client
/// dials (HTTPS endpoint identification), so certificates need SANs for broker hostnames or, when brokers register
/// by IP, their IP addresses. Verification is skipped only when the AsyncHttpClient config disables it, e.g. with the
/// JVM-wide system property `org.asynchttpclient.disableHttpsEndpointIdentificationAlgorithm=true`.
public class DefaultSslContextProvider implements SslContextProvider {

  @Override
  public DefaultAsyncHttpClientConfig.Builder configure(DefaultAsyncHttpClientConfig.Builder builder,
      @Nullable SSLContext sslContext, TlsProtocols tlsProtocols) {
    builder.setUseOpenSsl(false);

    List<String> enabledProtocolList = List.of();
    if (tlsProtocols != null) {
      List<String> configuredProtocols = tlsProtocols.getEnabledProtocols();
      if (configuredProtocols != null) {
        enabledProtocolList = configuredProtocols;
      }
    }
    String[] enabledProtocols = enabledProtocolList.toArray(new String[0]);
    if (sslContext != null) {
      builder.setSslEngineFactory((config, peerHost, peerPort) -> {
        SSLEngine engine = sslContext.createSSLEngine(peerHost, peerPort);
        engine.setUseClientMode(true);
        // A JDK SSLEngine verifies the peer hostname only when an endpoint identification algorithm is set. This
        // factory replaces AsyncHttpClient's own engine factory, so apply the same check it would, honoring
        // disableHttpsEndpointIdentificationAlgorithm.
        if (!config.isDisableHttpsEndpointIdentificationAlgorithm()) {
          SSLParameters sslParameters = engine.getSSLParameters();
          sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
          engine.setSSLParameters(sslParameters);
        }
        if (enabledProtocols.length > 0) {
          engine.setEnabledProtocols(enabledProtocols);
        }
        return engine;
      });
    }
    if (enabledProtocols.length > 0) {
      builder.setEnabledProtocols(enabledProtocols);
    }
    return builder;
  }
}
