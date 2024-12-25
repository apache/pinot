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
package org.apache.pinot.common.utils.http;

import java.net.URI;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpVersion;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;


/**
 * Utility class for creating and managing HTTP related requests utility functions.
 * - This class provides various utility methods to facilitate HTTP request creation and manipulation.
 */
public class HttpUtils {
  private HttpUtils() {
  }

  /**
   * Creates a {@link ClassicRequestBuilder} with the given URI and method.
   *
   * @param uri the URI for the request
   * @param method the HTTP method for the request (e.g., GET, POST)
   * @return a {@link ClassicRequestBuilder} instance
   */
  public static ClassicRequestBuilder createRequestBuilder(URI uri, String method) {
    return ClassicRequestBuilder.create(method).setUri(uri).setVersion(HttpVersion.HTTP_2_0);
  }

  /**
   * Apply the provided headers and parameters to the given {@link ClassicRequestBuilder}.
   *
   * @param requestBuilder the {@link ClassicRequestBuilder} to which headers and parameters will be added
   * @param headers a list of headers to be added to the request, or null if no headers are to be added
   * @param parameters a list of parameters to be added to the request, or null if no parameters are to be added
   */
  public static void applyHeadersAndParameters(ClassicRequestBuilder requestBuilder, @Nullable List<Header> headers,
      @Nullable List<NameValuePair> parameters) {
    if (headers != null) {
      for (Header header : headers) {
        requestBuilder.addHeader(header);
      }
    }
    if (parameters != null) {
      for (NameValuePair parameter : parameters) {
        requestBuilder.addParameter(parameter);
      }
    }
  }
}
