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

import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Pinot client transport factory for JSON encoded BrokerResults through HTTP.
 */
public class JsonAsyncHttpPinotClientTransportFactory implements PinotClientTransportFactory {
  private Map<String, String> _headers = new HashMap<>();
  private String _scheme = CommonConstants.HTTP_PROTOCOL;
  private SSLContext _sslContext = null;

  @Override
  public PinotClientTransport buildTransport() {
    return new JsonAsyncHttpPinotClientTransport(_headers, _scheme, _sslContext);
  }

  public Map<String, String> getHeaders() {
    return _headers;
  }

  public void setHeaders(Map<String, String> headers) {
    _headers = headers;
  }

  public String getScheme() {
    return _scheme;
  }

  public void setScheme(String scheme) {
    _scheme = scheme;
  }

  public SSLContext getSslContext() {
    return _sslContext;
  }

  public void setSslContext(SSLContext sslContext) {
    _sslContext = sslContext;
  }
}
