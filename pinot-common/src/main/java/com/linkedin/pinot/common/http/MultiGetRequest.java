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
package com.linkedin.pinot.common.http;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import javax.annotation.Nonnull;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to support multiple http GET operations in parallel by using
 * the executor that is passed in.
 *
 * This is a wrapper around Apache common HTTP client.
 *
 * The execute method is re-usable but there is no real benefit to it. All
 * the connection management is handled by the input HttpConnectionManager.
 * For access through multiple-threads, use MultiThreadedHttpConnectionManager
 * as shown in the example below.
 *
 * Usage:
 * <pre>
 * {@code
 *    List<String> urls = Arrays.asList("http://www.linkedin.com", "http://www.google.com");
 *    MultiGetRequest mget = new MultiGetRequest(Executors.newCachedThreadPool(),
 *           new MultiThreadedHttpConnectionManager());
 *    CompletionService<GetMethod> completionService = mget.execute(urls);
 *    for (int i = 0; i < urls.size(); i++) {
 *      GetMethod getMethod = null;
 *      try {
 *        getMethod = completionService.take().get();
 *        if (getMethod.getStatusCode() >= 300) {
 *          System.out.println("error");
 *          continue;
 *        }
 *        System.out.println("Got data: " +  getMethod.getResponseBodyAsString());
 *      } catch (ExecutionException e) {
 *         if (Throwables.getRootcause(e) instanceof SocketTimeoutException) {
 *           System.out.println("Timeout");
 *         }
 *      } finally {
 *        if (getMethod != null) {
 *          getMethod.releaseConnection();
 *        }
 *      }
 *    }
 * }
 * </pre>
 */
public class MultiGetRequest {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiGetRequest.class);

  Executor executor;
  HttpConnectionManager connectionManager;

  /**
   * @param executor executor service to use for making parallel requests
   * @param connectionManager http connection manager to use.
   */
  public MultiGetRequest(@Nonnull Executor executor,
      @Nonnull HttpConnectionManager connectionManager) {
    Preconditions.checkNotNull(executor);
    Preconditions.checkNotNull(connectionManager);

    this.executor = executor;
    this.connectionManager = connectionManager;
  }

  /**
   * GET urls in parallel using the executor service.
   * @param urls absolute URLs to GET
   * @param timeoutMs timeout in milliseconds for each GET request
   * @return instance of CompletionService. Completion service will provide
   *   results as they arrive. The order is NOT same as the order of URLs
   */
  public CompletionService<GetMethod> execute(@Nonnull List<String> urls, final int timeoutMs) {
    Preconditions.checkNotNull(urls);
    Preconditions.checkArgument(timeoutMs > 0, "Timeout value for multi-get must be greater than 0");

    CompletionService<GetMethod> completionService = new ExecutorCompletionService<>(executor);
    for (final String url : urls) {
      completionService.submit(new Callable<GetMethod>() {
        @Override
        public GetMethod call()
            throws Exception {
          HttpClient client = new HttpClient(connectionManager);
          GetMethod getMethod = new GetMethod(url);
          getMethod.getParams().setSoTimeout(timeoutMs);
          // if all connections in the connection manager are busy this will wait to retrieve a connection
          // set time to wait to retrieve a connection from connection manager
          client.getParams().setConnectionManagerTimeout(timeoutMs);
          client.executeMethod(getMethod);
          return getMethod;
        }
      });
    }
    return completionService;
  }
}
