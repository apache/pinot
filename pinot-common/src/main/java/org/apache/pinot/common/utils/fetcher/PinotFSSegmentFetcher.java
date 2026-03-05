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
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.apache.pinot.spi.utils.retry.RetryPolicies;


public class PinotFSSegmentFetcher extends BaseSegmentFetcher {

  @Override
  protected void fetchSegmentToLocalWithoutRetry(URI uri, File dest)
      throws Exception {
    if (uri.getScheme() == null) {
      PinotFSFactory.create(PinotFSFactory.LOCAL_PINOT_FS_SCHEME).copyToLocalFile(uri, dest);
    } else {
      PinotFSFactory.create(uri.getScheme()).copyToLocalFile(uri, dest);
    }
  }

  @Override
  public File fetchUntarSegmentToLocalStreamed(URI uri, File dest, long rateLimit, AtomicInteger attempts)
      throws Exception {
    PinotFS pinotFS;
    if (uri.getScheme() == null) {
      pinotFS = PinotFSFactory.create(PinotFSFactory.LOCAL_PINOT_FS_SCHEME);
    } else {
      pinotFS = PinotFSFactory.create(uri.getScheme());
    }
    AtomicReference<File> untarredFileRef = new AtomicReference<>();

    try {
      int tries =
          RetryPolicies.exponentialBackoffRetryPolicy(_retryCount, _retryWaitMs, _retryDelayScaleFactor).attempt(() -> {
            try (InputStream inputStream = pinotFS.open(uri)) {
              List<File> untarredFiles = TarCompressionUtils.untarWithRateLimiter(inputStream, dest, rateLimit);
              untarredFileRef.set(untarredFiles.get(0));
              return true;
            } catch (Exception e) {
              _logger.warn("Caught exception while downloading segment from: {} to: {}", uri, dest, e);
              return false;
            }
          });
      attempts.set(tries);
    } catch (AttemptsExceededException | RetriableOperationException e) {
      attempts.set(e.getAttempts());
      throw e;
    }
    return untarredFileRef.get();
  }
}
