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

import java.io.File;
import java.net.URI;
import org.apache.http.HttpStatus;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.RoundRobinURIProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.retry.RetryPolicies;


public class HttpSegmentFetcher extends BaseSegmentFetcher {
  protected FileUploadDownloadClient _httpClient;

  @Override
  protected void doInit(PinotConfiguration config) {
    _httpClient = new FileUploadDownloadClient();
  }

  @Override
  public void fetchSegmentToLocal(URI downloadURI, File dest)
      throws Exception {
    RoundRobinURIProvider uriProvider = new RoundRobinURIProvider(downloadURI);
    _retryCount = Math.max(_retryCount, uriProvider.numAddresses());
    _logger.info("set retryCount as: {}", _retryCount);
    RetryPolicies.exponentialBackoffRetryPolicy(_retryCount, _retryWaitMs, _retryDelayScaleFactor).attempt(() -> {
      URI uri = uriProvider.next();
      try {
        int statusCode = _httpClient.downloadFile(uri, dest, _authToken);
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

  @Override
  public void fetchSegmentToLocalWithoutRetry(URI uri, File dest)
      throws Exception {
    try {
      int statusCode = _httpClient.downloadFile(uri, dest, _authToken);
      _logger.info("Downloaded segment from: {} to: {} of size: {}; Response status code: {}", uri, dest, dest.length(),
          statusCode);
    } catch (Exception e) {
      _logger.warn("Caught exception while downloading segment from: {} to: {}", uri, dest, e);
      throw e;
    }
  }
}
