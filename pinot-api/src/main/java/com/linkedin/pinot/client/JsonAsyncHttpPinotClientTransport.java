/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.client;

import com.linkedin.pinot.common.response.BrokerResponse;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.json.JSONObject;


/**
 * JSON encoded Pinot client transport over AsyncHttpClient.
 *
 * @author jfim
 */
class JsonAsyncHttpPinotClientTransport implements PinotClientTransport {
  AsyncHttpClient _httpClient = new AsyncHttpClient();

  @Override
  public BrokerResponse executeQuery(String brokerAddress, String query) throws PinotClientException {
    try {
      return executeQueryAsync(brokerAddress, query).get();
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  @Override
  public Future<BrokerResponse> executeQueryAsync(String brokerAddress, String query) {
    try {
      final JSONObject json = new JSONObject();
      json.put("pql", query);

      final String url = "http://" + brokerAddress + "/query";
      final Future<Response> response = _httpClient.preparePost(url).setBody(json.toString()).execute();

      return new Future<BrokerResponse>() {
        public boolean cancel(boolean mayInterruptIfRunning) {
          return response.cancel(mayInterruptIfRunning);
        }

        public boolean isCancelled() {
          return response.isCancelled();
        }

        public boolean isDone() {
          return response.isDone();
        }

        @Override
        public BrokerResponse get()
            throws InterruptedException, ExecutionException {
          try {
            return get(1000L, TimeUnit.DAYS);
          } catch (TimeoutException e) {
            throw new InterruptedException();
          }
        }

        @Override
        public BrokerResponse get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
          try {
            Response httpResponse = response.get(timeout, unit);

            if (httpResponse.getStatusCode() != 200) {
              throw new PinotClientException("Pinot returned HTTP status " + httpResponse.getStatusCode() +
                  ", expected 200");
            }

            String responseBody = httpResponse.getResponseBody();
            return BrokerResponse.fromJson(new JSONObject(responseBody));
          } catch (Exception e) {
            throw new ExecutionException(e);
          }
        }
      };
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }
}
