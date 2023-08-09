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

import java.net.URI;
import org.apache.http.client.methods.CloseableHttpResponse;


public class MultiHttpRequestResponse implements AutoCloseable {
  private final URI _uri;
  private final int _responseStatusCode;
  private final String _responseContent;
  private final CloseableHttpResponse _response;

  public MultiHttpRequestResponse(URI uri, int responseStatusCode,
      String responseContent, CloseableHttpResponse response) {
    _uri = uri;
    _response = response;
    _responseStatusCode = responseStatusCode;
    _responseContent = responseContent;
  }

  public URI getURI() {
    return _uri;
  }

  public int getResponseStatusCode() {
    return _responseStatusCode;
  }

  public String getResponseContent() {
    return _responseContent;
  }

  @Override
  public void close() throws Exception {
    _response.close();
  }
}
