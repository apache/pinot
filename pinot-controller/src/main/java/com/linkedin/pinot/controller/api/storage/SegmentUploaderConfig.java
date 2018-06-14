/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.api.storage;

import javax.ws.rs.core.HttpHeaders;
import org.glassfish.grizzly.http.server.Request;


/**
 * Wrapper class that encapsulates all parameters sent by clients during segment upload
 */
public class SegmentUploaderConfig {
  private final HttpHeaders _headers;
  private final Request _request;


  private SegmentUploaderConfig(HttpHeaders headers, Request request) {
    _headers = headers;
    _request = request;
  }

  public HttpHeaders getHeaders() {
    return _headers;
  }

  public Request getRequest() {
    return _request;
  }

  public static class Builder {
    private HttpHeaders _headers;
    private Request _request;

    public Builder() {
    }

    public Builder setHeaders(HttpHeaders headers) {
      _headers = headers;
      return this;
    }

    public Builder setRequest(Request request) {
      _request = request;
      return this;
    }

    public SegmentUploaderConfig build() {
      return new SegmentUploaderConfig(_headers, _request);
    }
  }
}
