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

import com.google.common.base.Preconditions;
import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.auth.AuthConfig;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.spi.crypt.PinotCrypter;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentFetcherFactory {
  private final static SegmentFetcherFactory INSTANCE = new SegmentFetcherFactory();

  static final String SEGMENT_FETCHER_CLASS_KEY_SUFFIX = ".class";
  private static final String PROTOCOLS_KEY = "protocols";
  private static final String ENCODED_SUFFIX = ".enc";
  private static final String AUTH_KEY = CommonConstants.KEY_OF_AUTH;

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentFetcherFactory.class);
  private static final Random RANDOM = new Random();

  private final Map<String, SegmentFetcher> _segmentFetcherMap = new HashMap<>();
  private final SegmentFetcher _httpSegmentFetcher = new HttpSegmentFetcher();
  private final SegmentFetcher _pinotFSSegmentFetcher = new PinotFSSegmentFetcher();

  private SegmentFetcherFactory() {
    // left blank
  }

  public static SegmentFetcherFactory getInstance() {
    return INSTANCE;
  }

  /**
   * Initializes the segment fetcher factory. This method should only be called once.
   */
  public static void init(PinotConfiguration config)
      throws Exception {
    getInstance().initInternal(config);
  }

  private void initInternal(PinotConfiguration config)
      throws Exception {
    _httpSegmentFetcher.init(config); // directly, without sub-namespace
    _pinotFSSegmentFetcher.init(config); // directly, without sub-namespace

    List<String> protocols = config.getProperty(PROTOCOLS_KEY, Collections.emptyList());
    for (String protocol : protocols) {
      String segmentFetcherClassName = config.getProperty(protocol + SEGMENT_FETCHER_CLASS_KEY_SUFFIX);
      SegmentFetcher segmentFetcher;
      if (segmentFetcherClassName == null) {
        LOGGER.info("Segment fetcher class is not configured for protocol: {}, using default", protocol);
        switch (protocol) {
          case CommonConstants.HTTP_PROTOCOL:
            segmentFetcher = new HttpSegmentFetcher();
            break;
          case CommonConstants.HTTPS_PROTOCOL:
            segmentFetcher = new HttpsSegmentFetcher();
            break;
          default:
            segmentFetcher = new PinotFSSegmentFetcher();
            break;
        }
      } else {
        LOGGER.info("Creating segment fetcher for protocol: {} with class: {}", protocol, segmentFetcherClassName);
        segmentFetcher = (SegmentFetcher) Class.forName(segmentFetcherClassName).newInstance();
      }

      AuthConfig authConfig = AuthProviderUtils.extractAuthConfig(config, AUTH_KEY);

      PinotConfiguration subConfig = config.subset(protocol);
      AuthConfig subAuthConfig = AuthProviderUtils.extractAuthConfig(subConfig, AUTH_KEY);

      Map<String, Object> subConfigMap = config.subset(protocol).toMap();
      if (subAuthConfig.getProperties().isEmpty() && !authConfig.getProperties().isEmpty()) {
        authConfig.getProperties().forEach((key, value) -> subConfigMap.put(AUTH_KEY + "." + key, value));
      }

      segmentFetcher.init(new PinotConfiguration(subConfigMap));

      _segmentFetcherMap.put(protocol, segmentFetcher);
    }
  }

  /**
   * Returns the segment fetcher associated with the given protocol, or the default segment fetcher
   * ({@link HttpSegmentFetcher} for "http" and "https", {@link PinotFSSegmentFetcher} for other protocols).
   */
  public static SegmentFetcher getSegmentFetcher(String protocol) {
    return getInstance().getSegmentFetcherInternal(protocol);
  }

  private SegmentFetcher getSegmentFetcherInternal(String protocol) {
    SegmentFetcher segmentFetcher = _segmentFetcherMap.get(protocol);
    if (segmentFetcher != null) {
      return segmentFetcher;
    } else {
      LOGGER.info("Segment fetcher is not configured for protocol: {}, using default", protocol);
      switch (protocol) {
        case CommonConstants.HTTP_PROTOCOL:
        case CommonConstants.HTTPS_PROTOCOL:
          return _httpSegmentFetcher;
        default:
          return _pinotFSSegmentFetcher;
      }
    }
  }

  /**
   * Fetches a segment from URI location to local.
   */
  public static void fetchSegmentToLocal(URI uri, File dest)
      throws Exception {
    getInstance().fetchSegmentToLocalInternal(uri, dest);
  }

  /**
   * Fetches a segment from URI location to local.
   */
  public static void fetchSegmentToLocal(String uri, File dest)
      throws Exception {
    getInstance().fetchSegmentToLocalInternal(new URI(uri), dest);
  }

  private void fetchSegmentToLocalInternal(URI uri, File dest)
      throws Exception {
    // caller untars
    getSegmentFetcher(uri.getScheme()).fetchSegmentToLocal(uri, dest);
  }

  /**
   * Fetches a segment from URI location to local and untar it in a streamed manner.
   * @param uri URI
   * @param tempRootDir Tmp dir to download
   * @param maxStreamRateInByte limit the rate to write download-untar stream to disk, in bytes
   *                  -1 for no disk write limit, 0 for limit the writing to min(untar, download) rate
   * @return the untared directory
   * @throws Exception
   */
  public static File fetchAndStreamUntarToLocal(String uri, File tempRootDir,
      long maxStreamRateInByte, AtomicInteger attempts)
      throws Exception {
    return getInstance().fetchAndStreamUntarToLocalInternal(new URI(uri), tempRootDir, maxStreamRateInByte, attempts);
  }

  private File fetchAndStreamUntarToLocalInternal(URI uri, File tempRootDir,
      long maxStreamRateInByte, AtomicInteger attempts)
      throws Exception {
    return getSegmentFetcher(uri.getScheme()).fetchUntarSegmentToLocalStreamed(uri, tempRootDir, maxStreamRateInByte,
        attempts);
  }

  /**
   * Fetches a segment from a URI location to a local file and decrypts it if needed
   * @param uri remote segment location
   * @param dest local file
   */
  public static void fetchAndDecryptSegmentToLocal(String uri, File dest, String crypterName)
      throws Exception {
    getInstance().fetchAndDecryptSegmentToLocalInternal(uri, dest, crypterName);
  }

  // uris have equal weight to be selected for segment download
  public static void fetchAndDecryptSegmentToLocal(List<URI> uris, File dest, String crypterName)
          throws Exception {
    getInstance().fetchAndDecryptSegmentToLocalInternal(uris, dest, crypterName);
  }

  private void fetchAndDecryptSegmentToLocalInternal(String uri, File dest, String crypterName)
      throws Exception {
    if (crypterName == null) {
      fetchSegmentToLocal(uri, dest);
    } else {
      // download
      File tempDownloadedFile = new File(dest.getPath() + ENCODED_SUFFIX);
      fetchSegmentToLocal(uri, tempDownloadedFile);

      // decrypt
      PinotCrypter crypter = PinotCrypterFactory.create(crypterName);
      crypter.decrypt(tempDownloadedFile, dest);
    }
  }

  private void fetchAndDecryptSegmentToLocalInternal(@NonNull List<URI> uris, File dest, String crypterName)
          throws Exception {
    Preconditions.checkArgument(!uris.isEmpty(), "empty uris passed into the fetchAndDecryptSegmentToLocalInternal");
    URI uri = uris.get(RANDOM.nextInt(uris.size()));
    if (crypterName == null) {
      fetchSegmentToLocal(uri, dest);
    } else {
      // download
      File tempDownloadedFile = new File(dest.getPath() + ENCODED_SUFFIX);
      fetchSegmentToLocal(uri, tempDownloadedFile);

      // decrypt
      PinotCrypter crypter = PinotCrypterFactory.create(crypterName);
      crypter.decrypt(tempDownloadedFile, dest);
    }
  }
}
