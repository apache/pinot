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
package org.apache.pinot.client.controller.response;

import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.client.PinotClientException;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


abstract class ControllerResponseFuture<T> implements Future<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerResponseFuture.class);
  private final Future<Response> _response;
  private final String _url;

  public ControllerResponseFuture(Future<Response> response, String url) {
    _response = response;
    _url = url;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return _response.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled() {
    return _response.isCancelled();
  }

  @Override
  public boolean isDone() {
    return _response.isDone();
  }

  @Override
  public T get()
      throws ExecutionException {
    return get(1000L, TimeUnit.DAYS);
  }

  abstract public T get(long timeout, TimeUnit unit)
      throws ExecutionException;

  public InputStream getStreamResponse(long timeout, TimeUnit unit)
      throws ExecutionException {
    try {
      LOGGER.debug("Sending request to {}", _url);

      Response httpResponse = _response.get(timeout, unit);

      LOGGER.debug("Completed request, HTTP status is {}", httpResponse.getStatusCode());

      if (httpResponse.getStatusCode() != 200) {
        throw new PinotClientException("Pinot returned HTTP status " + httpResponse.getStatusCode() + ", expected 200");
      }

      return httpResponse.getResponseBodyAsStream();
    } catch (TimeoutException | InterruptedException e) {
      throw new ExecutionException(e);
    }
  }
}
