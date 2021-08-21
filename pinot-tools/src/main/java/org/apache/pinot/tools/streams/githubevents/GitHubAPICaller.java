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
package org.apache.pinot.tools.streams.githubevents;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.pinot.tools.Quickstart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.tools.Quickstart.printStatus;


/**
 * Helper class to make http calls to GitHub API
 */
public class GitHubAPICaller {

  private static final Logger LOGGER = LoggerFactory.getLogger(GitHubAPICaller.class);
  private static final String EVENTS_API_URL = "https://api.github.com/events";
  private static final String AUTHORIZATION_HEADER = "Authorization";
  private static final String IF_NONE_MATCH_HEADER = "If-None-Match";
  private static final String RATE_LIMIT_REMAINING_HEADER = "X-RateLimit-Remaining";
  private static final String RATE_LIMIT_RESET_HEADER = "X-RateLimit-Reset";
  private static final String ETAG_HEADER = "ETag";
  private static final String TOKEN_PREFIX = "token ";
  private static final String TIMEOUT_MESSAGE = "Timeout";

  private final CloseableHttpClient _closeableHttpClient;
  private String _personalAccessToken;

  GitHubAPICaller(String personalAccessToken) {
    printStatus(Quickstart.Color.CYAN, "***** Initializing GitHubAPICaller *****");
    _personalAccessToken = personalAccessToken;
    _closeableHttpClient = HttpClients.createDefault();
  }

  /**
   * Calls the events API
   */
  public GitHubAPIResponse callEventsAPI(String etag)
      throws IOException {
    HttpUriRequest request = buildRequest(EVENTS_API_URL, etag);
    return executeEventsRequest(request);
  }

  /**
   * Calls the given url
   */
  public GitHubAPIResponse callAPI(String url)
      throws IOException {
    HttpUriRequest request = buildRequest(url, null);
    return executeGet(request);
  }

  private void setTimeout(RequestBuilder requestBuilder) {
    RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(10_000).setSocketTimeout(10_000).build();
    requestBuilder.setConfig(requestConfig);
  }

  private HttpUriRequest buildRequest(String url, String etag) {
    RequestBuilder requestBuilder =
        RequestBuilder.get(url).setHeader(AUTHORIZATION_HEADER, TOKEN_PREFIX + _personalAccessToken);
    if (etag != null) {
      requestBuilder.setHeader(IF_NONE_MATCH_HEADER, etag);
    }
    setTimeout(requestBuilder);
    return requestBuilder.build();
  }

  /**
   * Make an Http GET call to the /events API
   */
  private GitHubAPIResponse executeEventsRequest(HttpUriRequest request)
      throws IOException {
    GitHubAPIResponse githubAPIResponse = new GitHubAPIResponse();

    try (CloseableHttpResponse httpResponse = _closeableHttpClient.execute(request)) {
      StatusLine statusLine = httpResponse.getStatusLine();
      githubAPIResponse.setStatusCode(statusLine.getStatusCode());
      githubAPIResponse.setStatusMessage(statusLine.getReasonPhrase());
      String remainingLimit = httpResponse.getFirstHeader(RATE_LIMIT_REMAINING_HEADER).getValue();
      String resetTimeSeconds = httpResponse.getFirstHeader(RATE_LIMIT_RESET_HEADER).getValue();
      try {
        if (remainingLimit != null) {
          githubAPIResponse.setRemainingLimit(Integer.parseInt(remainingLimit));
        }
        if (resetTimeSeconds != null) {
          githubAPIResponse.setResetTimeMs(Long.parseLong(resetTimeSeconds) * 1000);
        }
      } catch (NumberFormatException e) {
        LOGGER.warn("Could not parse remainingLimit: " + remainingLimit);
      }
      if (statusLine.getStatusCode() == 200) {
        githubAPIResponse.setEtag(httpResponse.getFirstHeader(ETAG_HEADER).getValue());
        githubAPIResponse.setResponseString(EntityUtils.toString(httpResponse.getEntity(), StandardCharsets.UTF_8));
      }
    } catch (SocketTimeoutException e) {
      githubAPIResponse.setStatusCode(408);
      githubAPIResponse.setStatusMessage(TIMEOUT_MESSAGE);
      LOGGER.error("Timeout in call to GitHub events API.", e);
    } catch (IOException e) {
      LOGGER.error("Exception in call to GitHub events API.", e);
      throw e;
    }
    return githubAPIResponse;
  }

  /**
   * Makes an Http GET call to the provided URL
   */
  private GitHubAPIResponse executeGet(HttpUriRequest request)
      throws IOException {
    GitHubAPIResponse githubAPIResponse = new GitHubAPIResponse();

    try (CloseableHttpResponse httpResponse = _closeableHttpClient.execute(request)) {
      StatusLine statusLine = httpResponse.getStatusLine();
      githubAPIResponse.setStatusCode(statusLine.getStatusCode());
      githubAPIResponse.setStatusMessage(statusLine.getReasonPhrase());

      if (statusLine.getStatusCode() == 200) {
        githubAPIResponse.setResponseString(EntityUtils.toString(httpResponse.getEntity(), StandardCharsets.UTF_8));
      } else {
        printStatus(Quickstart.Color.YELLOW,
            "Status code " + githubAPIResponse._statusCode + " status message " + githubAPIResponse._statusMessage
                + " uri " + request.getURI());
      }
    } catch (SocketTimeoutException e) {
      githubAPIResponse.setStatusCode(408);
      githubAPIResponse.setStatusMessage(TIMEOUT_MESSAGE);
      LOGGER.error("Timeout in call to GitHub API {}", request.getURI(), e);
    } catch (IOException e) {
      LOGGER.error("Exception in call to GitHub API {}", request.getURI(), e);
      throw e;
    }
    return githubAPIResponse;
  }

  public void shutdown()
      throws IOException {
    printStatus(Quickstart.Color.GREEN, "***** Shutting down GitHubAPICaller *****");
    _closeableHttpClient.close();
  }

  /**
   * Represents a response from the GitHub API
   */
  static class GitHubAPIResponse {
    String _responseString = null;
    int _statusCode = 0;
    String _statusMessage;
    String _etag;
    int _remainingLimit = 0;
    long _resetTimeMs = 0;

    public void setResponseString(String responseString) {
      _responseString = responseString;
    }

    public void setStatusCode(int statusCode) {
      _statusCode = statusCode;
    }

    public void setStatusMessage(String statusMessage) {
      _statusMessage = statusMessage;
    }

    public void setEtag(String etag) {
      _etag = etag;
    }

    public void setRemainingLimit(int remainingLimit) {
      _remainingLimit = remainingLimit;
    }

    public void setResetTimeMs(long resetTimeMs) {
      _resetTimeMs = resetTimeMs;
    }
  }
}
