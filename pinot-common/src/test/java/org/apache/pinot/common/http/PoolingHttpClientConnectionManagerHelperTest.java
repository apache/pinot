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
package org.apache.pinot.common.http;

import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.config.Registry;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class PoolingHttpClientConnectionManagerHelperTest {

  @Test
  public void itBuildsCorrectRegistry() {
    Registry<ConnectionSocketFactory> socketFactoryRegistry =
        PoolingHttpClientConnectionManagerHelper.getSocketFactoryRegistry();

    assertNotNull(socketFactoryRegistry.lookup(CommonConstants.HTTP_PROTOCOL));
    assertNotNull(socketFactoryRegistry.lookup(CommonConstants.HTTPS_PROTOCOL));
    assertTrue(socketFactoryRegistry.lookup(CommonConstants.HTTP_PROTOCOL) instanceof PlainConnectionSocketFactory);
    assertTrue(socketFactoryRegistry.lookup(CommonConstants.HTTPS_PROTOCOL) instanceof SSLConnectionSocketFactory);
  }
}
