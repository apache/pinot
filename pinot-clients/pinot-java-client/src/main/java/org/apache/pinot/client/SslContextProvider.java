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

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;


/**
 * Pluggable provider for configuring AsyncHttpClient SSL/TLS.
 *
 * A custom provider can be supplied using the system property
 * {@code pinot.client.sslContextProvider} (fully qualified class name)
 * or via the Java service loader (META-INF/services).
 */
public interface SslContextProvider {

  /**
   * Configure the AsyncHttpClient builder with SSL/TLS settings.
   *
   * @param builder the client config builder to update
   * @param sslContext optional SSL context to use
   * @param tlsProtocols configured TLS protocol list
   * @return the same builder for chaining
   */
  DefaultAsyncHttpClientConfig.Builder configure(DefaultAsyncHttpClientConfig.Builder builder,
      @Nullable SSLContext sslContext, TlsProtocols tlsProtocols);
}
