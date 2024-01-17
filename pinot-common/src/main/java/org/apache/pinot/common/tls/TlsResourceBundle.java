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
package org.apache.pinot.common.tls;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * A bundle of TLS resources. It contains the SSLContext, KeyManagerFactory and TrustManagerFactory.
 * Any of those can be null if not available.
 */
@AllArgsConstructor
@Getter
public class TlsResourceBundle {
  private KeyManagerFactory _keyManagerFactory;
  private TrustManagerFactory _trustManagerFactory;
  private SSLContext _sslContext;
}
