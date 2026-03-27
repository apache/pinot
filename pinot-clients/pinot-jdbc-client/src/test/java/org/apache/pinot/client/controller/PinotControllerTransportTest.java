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
package org.apache.pinot.client.controller;

import java.lang.reflect.Field;
import javax.net.ssl.SSLContext;
import org.apache.pinot.client.ConnectionTimeouts;
import org.apache.pinot.client.TlsProtocols;
import org.asynchttpclient.AsyncHttpClient;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;


public class PinotControllerTransportTest {

  @Test
  public void testLegacyConstructorLeavesEndpointIdentificationAlgorithmUnset()
      throws Exception {
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, null, null);

    PinotControllerTransport transport = null;
    try {
      transport =
          new PinotControllerTransport(null, "https", sslContext, ConnectionTimeouts.create(1000, 1000, 1000),
              TlsProtocols.defaultProtocols(false), null);

      Field httpClientField = PinotControllerTransport.class.getDeclaredField("_httpClient");
      httpClientField.setAccessible(true);
      AsyncHttpClient httpClient = (AsyncHttpClient) httpClientField.get(transport);

      assertNull(httpClient.getConfig().getSslEngineFactory().newSslEngine(httpClient.getConfig(), "localhost", 443)
          .getSSLParameters().getEndpointIdentificationAlgorithm());
    } finally {
      if (transport != null) {
        transport.close();
      }
    }
  }
}
