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
import javax.annotation.Nullable;


/// Immutable, thread-safe HTTP response whose entity has been fully buffered and whose connection has been released.
public final class MultiHttpRequestBufferedResponse {
  private final URI _uri;
  private final int _statusCode;
  @Nullable
  private final String _reasonPhrase;
  @Nullable
  private final String _responseBody;

  /// Creates a response whose body has already been consumed and whose HTTP connection has been released.
  public MultiHttpRequestBufferedResponse(URI uri, int statusCode, @Nullable String reasonPhrase,
      @Nullable String responseBody) {
    _uri = uri;
    _statusCode = statusCode;
    _reasonPhrase = reasonPhrase;
    _responseBody = responseBody;
  }

  /// Returns the request URI.
  public URI getURI() {
    return _uri;
  }

  /// Returns the HTTP status code.
  public int getStatusCode() {
    return _statusCode;
  }

  /// Returns the optional HTTP reason phrase.
  @Nullable
  public String getReasonPhrase() {
    return _reasonPhrase;
  }

  /// Returns the buffered response body, or `null` when the response had no entity.
  @Nullable
  public String getResponseBody() {
    return _responseBody;
  }
}
