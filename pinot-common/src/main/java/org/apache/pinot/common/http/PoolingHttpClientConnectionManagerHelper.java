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

import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.spi.utils.CommonConstants;


public class PoolingHttpClientConnectionManagerHelper {

  private PoolingHttpClientConnectionManagerHelper() {
  }

  public static Registry<ConnectionSocketFactory> getSocketFactoryRegistry() {
    return RegistryBuilder.<ConnectionSocketFactory>create()
        .register(CommonConstants.HTTP_PROTOCOL, PlainConnectionSocketFactory.getSocketFactory())
        .register(CommonConstants.HTTPS_PROTOCOL, TlsUtils.buildConnectionSocketFactory()).build();
  }

  public static PoolingHttpClientConnectionManager createWithSocketFactory() {
    return new PoolingHttpClientConnectionManager(getSocketFactoryRegistry());
  }
}
