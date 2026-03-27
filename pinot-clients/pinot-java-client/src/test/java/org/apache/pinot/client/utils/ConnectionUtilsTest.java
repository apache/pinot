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
package org.apache.pinot.client.utils;

import java.util.Properties;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;


public class ConnectionUtilsTest {

  @Test
  public void testCreateSslContextFromPropertiesDoesNotMutateGlobalTlsState() {
    SSLSocketFactory originalSocketFactory = HttpsURLConnection.getDefaultSSLSocketFactory();
    SSLContext originalSslContext = TlsUtils.getSslContext();

    SSLContext sslContext = ConnectionUtils.createSSLContextFromProperties(new Properties());

    assertNotNull(sslContext);
    assertSame(HttpsURLConnection.getDefaultSSLSocketFactory(), originalSocketFactory);
    assertSame(TlsUtils.getSslContext(), originalSslContext);
  }
}
