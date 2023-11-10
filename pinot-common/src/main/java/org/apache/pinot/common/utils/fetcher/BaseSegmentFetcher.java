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
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.core.util.PeerServerSegmentFinder;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base implementation of segment fetcher with the retry logic embedded.
 */
public abstract class BaseSegmentFetcher implements SegmentFetcher {
  public static final String RETRY_COUNT_CONFIG_KEY = "retry.count";
  public static final String RETRY_WAIT_MS_CONFIG_KEY = "retry.wait.ms";
  public static final String RETRY_DELAY_SCALE_FACTOR_CONFIG_KEY = "retry.delay.scale.factor";
  public static final int DEFAULT_RETRY_COUNT = 3;
  public static final int DEFAULT_RETRY_WAIT_MS = 100;
  public static final int DEFAULT_RETRY_DELAY_SCALE_FACTOR = 5;

  protected final Logger _logger = LoggerFactory.getLogger(getClass().getSimpleName());

  protected int _retryCount;
  protected int _retryWaitMs;
  protected int _retryDelayScaleFactor;
  protected AuthProvider _authProvider;

  @Override
  public void init(PinotConfiguration config) {
    _retryCount = config.getProperty(RETRY_COUNT_CONFIG_KEY, DEFAULT_RETRY_COUNT);
    _retryWaitMs = config.getProperty(RETRY_WAIT_MS_CONFIG_KEY, DEFAULT_RETRY_WAIT_MS);
    _retryDelayScaleFactor = config.getProperty(RETRY_DELAY_SCALE_FACTOR_CONFIG_KEY, DEFAULT_RETRY_DELAY_SCALE_FACTOR);
    _authProvider = AuthProviderUtils.extractAuthProvider(config, CommonConstants.KEY_OF_AUTH);
    doInit(config);
    _logger
        .info("Initialized with retryCount: {}, retryWaitMs: {}, retryDelayScaleFactor: {}", _retryCount, _retryWaitMs,
            _retryDelayScaleFactor);
  }

  /**
   * Override this for custom initialization.
   */
  protected void doInit(PinotConfiguration config) {
  }

  @Override
  public void fetchSegmentToLocal(URI uri, File dest)
      throws Exception {
    RetryPolicies.exponentialBackoffRetryPolicy(_retryCount, _retryWaitMs, _retryDelayScaleFactor).attempt(() -> {
      try {
        fetchSegmentToLocalWithoutRetry(uri, dest);
        _logger.info("Fetched segment from: {} to: {} of size: {}", uri, dest, dest.length());
        return true;
      } catch (Exception e) {
        _logger.warn("Caught exception while fetching segment from: {} to: {}", uri, dest, e);
        return false;
      }
    });
  }

  @Override
  public void fetchSegmentToLocal(List<URI> uris, File dest)
      throws Exception {
    if (uris == null || uris.isEmpty()) {
      throw new IllegalArgumentException("The input uri list is null or empty");
    }
    Random r = new Random();
    RetryPolicies.exponentialBackoffRetryPolicy(_retryCount, _retryWaitMs, _retryDelayScaleFactor).attempt(() -> {
      URI uri = uris.get(r.nextInt(uris.size()));
      try {
        fetchSegmentToLocalWithoutRetry(uri, dest);
        _logger.info("Fetched segment from: {} to: {} of size: {}", uri, dest, dest.length());
        return true;
      } catch (Exception e) {
        _logger.warn("Caught exception while fetching segment from: {} to: {}", uri, dest, e);
        return false;
      }
    });
  }

  public File fetchUntarSegmentToLocalStreamed(URI uri, File dest, long rateLimit,
      AtomicInteger attempts)
      throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fetchSegmentToLocal(String segmentName, File dest, HelixManager helixManager, String downloadScheme)
      throws Exception {
    Random r = new Random();
    RetryPolicies.exponentialBackoffRetryPolicy(_retryCount, _retryWaitMs, _retryDelayScaleFactor).attempt(() -> {
      // First find servers hosting the segment in ONLINE state.
      List<URI> peerSegmentURIs = PeerServerSegmentFinder.getPeerServerURIs(segmentName, downloadScheme, helixManager);
      // Next fetch the segment.
      fetchSegmentToLocalWithoutRetry(peerSegmentURIs.get(r.nextInt(peerSegmentURIs.size())), dest);
      return true;
    });
  }

  /**
   * Fetches a segment from URI location to local without retry. Sub-class should override this or
   * {@link #fetchSegmentToLocal(URI, File)}.
   */
  protected void fetchSegmentToLocalWithoutRetry(URI uri, File dest)
      throws Exception {
    throw new UnsupportedOperationException();
  }
}
