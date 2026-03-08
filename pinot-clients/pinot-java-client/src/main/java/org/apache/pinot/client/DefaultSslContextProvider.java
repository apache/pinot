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

import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;


/**
 * Default SSL context provider that uses the JDK/BCJSSE stack and disables OpenSSL.
 */
public class DefaultSslContextProvider implements SslContextProvider {

  @Override
  public DefaultAsyncHttpClientConfig.Builder configure(DefaultAsyncHttpClientConfig.Builder builder,
      @Nullable SSLContext sslContext, TlsProtocols tlsProtocols) {
    builder.setUseOpenSsl(false);

    List<String> enabledProtocolList = Collections.emptyList();
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
