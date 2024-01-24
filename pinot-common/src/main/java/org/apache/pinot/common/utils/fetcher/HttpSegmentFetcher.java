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
package org.apache.pinot.common.utils.fetcher;

import com.google.common.net.InetAddresses;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeader;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.RoundRobinURIProvider;
import org.apache.pinot.common.utils.http.HttpClientConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.apache.pinot.spi.utils.retry.RetryPolicies;


public class HttpSegmentFetcher extends BaseSegmentFetcher {
  protected FileUploadDownloadClient _httpClient;

  @Override
  protected void doInit(PinotConfiguration config) {
    _httpClient = new FileUploadDownloadClient(HttpClientConfig.newBuilder(config).build());
  }

  public HttpSegmentFetcher() {
  }

  public HttpSegmentFetcher(FileUploadDownloadClient httpClient, PinotConfiguration config) {
    _httpClient = httpClient;
    _retryCount = config.getProperty(RETRY_COUNT_CONFIG_KEY, DEFAULT_RETRY_COUNT);
    _retryWaitMs = config.getProperty(RETRY_WAIT_MS_CONFIG_KEY, DEFAULT_RETRY_WAIT_MS);
    _retryDelayScaleFactor = config.getProperty(RETRY_DELAY_SCALE_FACTOR_CONFIG_KEY, DEFAULT_RETRY_DELAY_SCALE_FACTOR);
    _logger
        .info("Initialized with retryCount: {}, retryWaitMs: {}, retryDelayScaleFactor: {}", _retryCount, _retryWaitMs,
            _retryDelayScaleFactor);
  }

  @Override
  public void fetchSegmentToLocal(URI downloadURI, File dest)
      throws Exception {
    // Create a RoundRobinURIProvider to round robin IP addresses when retry uploading. Otherwise may always try to
    // download from a same broken host as: 1) DNS may not RR the IP addresses 2) OS cache the DNS resolution result.
    RoundRobinURIProvider uriProvider = new RoundRobinURIProvider(downloadURI);

    int retryCount = getRetryCount(uriProvider);

    _logger.info("Retry downloading for {} times. retryCount from pinot server config: {}, number of IP addresses for "
        + "download URI: {}", retryCount, _retryCount, uriProvider.numAddresses());
    RetryPolicies.exponentialBackoffRetryPolicy(retryCount, _retryWaitMs, _retryDelayScaleFactor).attempt(() -> {
      URI uri = uriProvider.next();
      try {
        String hostName = downloadURI.getHost();
        int port = downloadURI.getPort();
        // If the original download address is specified as host name, need add a "HOST" HTTP header to the HTTP
        // request. Otherwise, if the download address is a LB address, when the LB be configured as "disallow direct
        // access by IP address", downloading will fail.
        List<Header> httpHeaders = new LinkedList<>();
        if (!InetAddresses.isInetAddress(hostName)) {
          httpHeaders.add(new BasicHeader(HttpHeaders.HOST, hostName + ":" + port));
        }
        int statusCode = _httpClient.downloadFile(uri, dest, _authProvider, httpHeaders);
        _logger
            .info("Downloaded segment from: {} to: {} of size: {}; Response status code: {}", uri, dest, dest.length(),
                statusCode);
        return true;
      } catch (HttpErrorStatusException e) {
        int statusCode = e.getStatusCode();
        if (statusCode == HttpStatus.SC_NOT_FOUND || statusCode >= 500) {
          // Temporary exception
          // 404 is treated as a temporary exception, as the downloadURI may be backed by multiple hosts,
          // if singe host is down, can retry with another host.
          _logger.warn("Got temporary error status code: {} while downloading segment from: {} to: {}", statusCode, uri,
              dest, e);
          return false;
        } else {
          // Permanent exception
          _logger.error("Got permanent error status code: {} while downloading segment from: {} to: {}, won't retry",
              statusCode, uri, dest, e);
          throw e;
        }
      } catch (Exception e) {
        _logger.warn("Caught exception while downloading segment from: {} to: {}", uri, dest, e);
        return false;
      }
    });
  }

  private int getRetryCount(RoundRobinURIProvider uriProvider) {
    // Use the minimal value of configured retry count and number of IP addresses.
    return Math.min(_retryCount, uriProvider.numAddresses());
  }

  @Override
  public File fetchUntarSegmentToLocalStreamed(URI downloadURI, File dest, long maxStreamRateInByte,
      AtomicInteger attempts)
      throws Exception {
    // Create a RoundRobinURIProvider to round robin IP addresses when retry uploading. Otherwise, may always try to
    // download from a same broken host as: 1) DNS may not RR the IP addresses 2) OS cache the DNS resolution result.
    RoundRobinURIProvider uriProvider = new RoundRobinURIProvider(downloadURI);

    int retryCount = getRetryCount(uriProvider);

    AtomicReference<File> ret = new AtomicReference<>(); // return the untared segment directory
    _logger.info("Retry downloading for {} times. retryCount from pinot server config: {}, number of IP addresses for "
        + "download URI: {}", retryCount, _retryCount, uriProvider.numAddresses());
    int tries;
    try {
      tries = RetryPolicies.exponentialBackoffRetryPolicy(retryCount, _retryWaitMs, _retryDelayScaleFactor).attempt(
          () -> {
        URI uri = uriProvider.next();
        try {
          String hostName = downloadURI.getHost();
          int port = downloadURI.getPort();
          // If the original download address is specified as host name, need add a "HOST" HTTP header to the HTTP
          // request. Otherwise, if the download address is a LB address, when the LB be configured as "disallow direct
          // access by IP address", downloading will fail.
          List<Header> httpHeaders = new LinkedList<>();
          if (!InetAddresses.isInetAddress(hostName)) {
            httpHeaders.add(new BasicHeader(HttpHeaders.HOST, hostName + ":" + port));
          }
          ret.set(_httpClient.downloadUntarFileStreamed(uri, dest, _authProvider, httpHeaders, maxStreamRateInByte));

          return true;
        } catch (HttpErrorStatusException e) {
          int statusCode = e.getStatusCode();
          if (statusCode == HttpStatus.SC_NOT_FOUND || statusCode >= 500) {
            // Temporary exception
            // 404 is treated as a temporary exception, as the downloadURI may be backed by multiple hosts,
            // if singe host is down, can retry with another host.
            _logger.warn("Got temporary error status code: {} while downloading segment from: {} to: {}", statusCode,
                uri, dest, e);
            return false;
          } else {
            // Permanent exception
            _logger.error("Got permanent error status code: {} while downloading segment from: {} to: {}, won't retry",
                statusCode, uri, dest, e);
            throw e;
          }
        } catch (IOException e) {
          _logger.warn("Caught IOException while stream download-untarring segment from: {} to: {}, retrying", uri,
              dest, e);
          return false;
        } catch (Exception e) {
          _logger.warn("Caught exception while downloading segment from: {} to: {}", uri, dest, e);
          return false;
        }
      });
    } catch (AttemptsExceededException e) {
      attempts.set(e.getAttempts());
      throw e;
    } catch (RetriableOperationException e) {
      attempts.set(e.getAttempts());
      throw e;
    }
    attempts.set(tries);
    return ret.get();
  }

  @Override
  public void fetchSegmentToLocalWithoutRetry(URI uri, File dest)
      throws Exception {
    try {
      int statusCode = _httpClient.downloadFile(uri, dest, _authProvider);
      _logger.info("Try to download the segment from: {} to: {} of size: {}; Response status code: {}", uri, dest,
          dest.length(), statusCode);
      // In case of download failure, throw exception.
      if (statusCode >= 300) {
        throw new HttpErrorStatusException("Failed to download segment", statusCode);
      }
    } catch (Exception e) {
      _logger.warn("Caught exception while downloading segment from: {} to: {}", uri, dest, e);
      throw e;
    }
  }
}
