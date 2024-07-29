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
import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.hc.core5.util.Timeout;
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
  private static final RequestConfig REQUEST_CONFIG = RequestConfig.custom()
      .setConnectTimeout(Timeout.of(10_000, TimeUnit.MILLISECONDS))
      .setResponseTimeout(Timeout.of(10_000, TimeUnit.MILLISECONDS))
      .build();

  private final CloseableHttpClient _closeableHttpClient;
  private String _personalAccessToken;

  GitHubAPICaller(String personalAccessToken) {
    printStatus(Quickstart.Color.CYAN, "***** Initializing GitHubAPICaller *****");
    _personalAccessToken = personalAccessToken;
    _closeableHttpClient = HttpClients.custom().setDefaultRequestConfig(REQUEST_CONFIG).build();
  }

  /**
   * Calls the events API
   */
  public GitHubAPIResponse callEventsAPI(String etag)
      throws IOException {
    ClassicHttpRequest request = buildRequest(EVENTS_API_URL, etag);
    return executeEventsRequest(request);
  }

  /**
   * Calls the given url
   */
  public GitHubAPIResponse callAPI(String url)
      throws IOException {
    ClassicHttpRequest request = buildRequest(url, null);
    return executeGet(request);
  }

  private ClassicHttpRequest buildRequest(String url, String etag) {
    ClassicRequestBuilder requestBuilder =
        ClassicRequestBuilder.get(url).setHeader(AUTHORIZATION_HEADER, TOKEN_PREFIX + _personalAccessToken);
    if (etag != null) {
      requestBuilder.setHeader(IF_NONE_MATCH_HEADER, etag);
    }
    return requestBuilder.build();
  }

  /**
   * Make an Http GET call to the /events API
   */
  private GitHubAPIResponse executeEventsRequest(ClassicHttpRequest request)
      throws IOException {
    GitHubAPIResponse githubAPIResponse = new GitHubAPIResponse();

    try (CloseableHttpResponse httpResponse = _closeableHttpClient.execute(request)) {
      githubAPIResponse.setStatusCode(httpResponse.getCode());
      githubAPIResponse.setStatusMessage(httpResponse.getReasonPhrase());
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
        LOGGER.warn("Could not parse remainingLimit: {}", remainingLimit);
      }
      if (httpResponse.getCode() == 200) {
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
    } catch (ParseException e) {
      LOGGER.error("Parse exception in GitHub events API response", e);
      throw new IOException(e);
    }
    return githubAPIResponse;
  }

  /**
   * Makes an Http GET call to the provided URL
   */
  private GitHubAPIResponse executeGet(ClassicHttpRequest request)
      throws IOException {
    GitHubAPIResponse githubAPIResponse = new GitHubAPIResponse();

    try (CloseableHttpResponse httpResponse = _closeableHttpClient.execute(request)) {
      githubAPIResponse.setStatusCode(httpResponse.getCode());
      githubAPIResponse.setStatusMessage(httpResponse.getReasonPhrase());

      if (httpResponse.getCode() == 200) {
        githubAPIResponse.setResponseString(EntityUtils.toString(httpResponse.getEntity(), StandardCharsets.UTF_8));
      } else {
        printStatus(Quickstart.Color.YELLOW,
            "Status code " + githubAPIResponse._statusCode + " status message " + githubAPIResponse._statusMessage
                + " uri " + request.getRequestUri());
      }
    } catch (SocketTimeoutException e) {
      githubAPIResponse.setStatusCode(408);
      githubAPIResponse.setStatusMessage(TIMEOUT_MESSAGE);
      LOGGER.error("Timeout in call to GitHub API {}", request.getRequestUri(), e);
    } catch (IOException e) {
      LOGGER.error("Exception in call to GitHub API {}", request.getRequestUri(), e);
      throw e;
    } catch (ParseException e) {
      LOGGER.error("Parse exception in GitHub API response {}", request.getRequestUri(), e);
      throw new IOException(e);
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
