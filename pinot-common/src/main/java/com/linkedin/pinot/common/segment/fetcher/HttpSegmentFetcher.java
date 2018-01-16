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
package com.linkedin.pinot.common.segment.fetcher;

import com.linkedin.pinot.common.exception.HttpErrorStatusException;
import com.linkedin.pinot.common.exception.PermanentDownloadException;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.common.utils.retry.RetryPolicy;
import java.io.File;
import java.net.URI;
import java.util.concurrent.Callable;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.pinot.common.utils.CommonConstants.SegmentFetcher.*;


public class HttpSegmentFetcher implements SegmentFetcher {
  protected final Logger _logger = LoggerFactory.getLogger(getClass().getSimpleName());

  protected FileUploadDownloadClient _httpClient;
  protected int _retryCount;
  protected int _retryWaitMs;

  @Override
  public void init(Configuration configs) {
    initHttpClient(configs);
    _retryCount = configs.getInt(RETRY, RETRY_DEFAULT);
    _retryWaitMs = configs.getInt(RETRY_WAITIME_MS, RETRY_WAITIME_MS_DEFAULT);
  }

  protected void initHttpClient(Configuration configs) {
    _httpClient = new FileUploadDownloadClient();
  }

  @Override
  public void fetchSegmentToLocal(final String uri, final File tempFile) throws Exception {
    RetryPolicy policy = RetryPolicies.exponentialBackoffRetryPolicy(_retryCount, _retryWaitMs, 5);
    policy.attempt(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          int statusCode = _httpClient.downloadFile(new URI(uri), tempFile);
          _logger.info("Downloaded file from {} to {}; Length of downloaded file: {}; Response status code: {}", uri,
              tempFile, tempFile.length(), statusCode);
          return true;
        } catch (HttpErrorStatusException e) {
          int statusCode = e.getStatusCode();
          if (statusCode >= 500) {
            _logger.error("Failed to download file from {}, might retry", uri, e);
            return false;
          } else {
            _logger.error("Failed to download file from {}, won't retry", uri, e);
            throw new PermanentDownloadException(e.getMessage());
          }
        } catch (Exception e) {
          _logger.error("Failed to download file from {}, might retry", uri, e);
          return false;
        }
      }
    });
  }
}
