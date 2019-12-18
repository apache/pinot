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
package org.apache.pinot.common.segment.fetcher;

import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class HttpSegmentFetcher implements SegmentFetcher {
  protected final Logger _logger = LoggerFactory.getLogger(getClass().getSimpleName());

  protected FileUploadDownloadClient _httpClient;
  protected int _retryCount;
  protected int _retryWaitMs;

  @Override
  public void init(Configuration configs) {
    initHttpClient(configs);
    _retryCount = configs.getInt(PinotFS.Constants.RETRY, PinotFS.Constants.RETRY_DEFAULT);
    _retryWaitMs = configs.getInt(PinotFS.Constants.RETRY_WAITIME_MS, PinotFS.Constants.RETRY_WAITIME_MS_DEFAULT);
  }

  protected void initHttpClient(Configuration configs) {
    _httpClient = new FileUploadDownloadClient();
  }

  @Override
  public void fetchSegmentToLocal(final String uri, final File tempFile)
      throws Exception {
    RetryPolicies.exponentialBackoffRetryPolicy(_retryCount, _retryWaitMs, 5).attempt(() -> {
      try {
        int statusCode = _httpClient.downloadFile(new URI(uri), tempFile);
        _logger.info("Downloaded file from: {} to: {}; Length of downloaded file: {}; Response status code: {}", uri,
            tempFile, tempFile.length(), statusCode);
        return true;
      } catch (HttpErrorStatusException e) {
        int statusCode = e.getStatusCode();
        if (statusCode >= 500) {
          // Temporary exception
          _logger.warn("Caught temporary exception while downloading file from: {}, will retry", uri, e);
          return false;
        } else {
          // Permanent exception
          _logger.error("Caught permanent exception while downloading file from: {}, won't retry", uri, e);
          throw e;
        }
      } catch (Exception e) {
        _logger.warn("Caught temporary exception while downloading file from: {}, will retry", uri, e);
        return false;
      }
    });
  }

  @Override
  public Set<String> getProtectedConfigKeys() {
    return Collections.emptySet();
  }
}
