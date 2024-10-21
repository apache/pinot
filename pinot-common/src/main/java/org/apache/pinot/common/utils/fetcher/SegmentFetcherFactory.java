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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.pinot.common.auth.AuthConfig;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.spi.crypt.PinotCrypter;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentFetcherFactory {
  private SegmentFetcherFactory() {
  }

  public static final String SEGMENT_FETCHER_CLASS_KEY_SUFFIX = ".class";
  public static final String PROTOCOLS_KEY = "protocols";
  public static final String ENCODED_SUFFIX = ".enc";

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentFetcherFactory.class);
  private static final Map<String, SegmentFetcher> SEGMENT_FETCHER_MAP = new HashMap<>();
  private static final SegmentFetcher HTTP_SEGMENT_FETCHER = new HttpSegmentFetcher();
  private static final SegmentFetcher PINOT_FS_SEGMENT_FETCHER = new PinotFSSegmentFetcher();

  /**
   * Initializes the segment fetcher factory. This method should only be called once.
   */
  public static void init(PinotConfiguration config)
      throws Exception {
    HTTP_SEGMENT_FETCHER.init(config); // directly, without sub-namespace
    PINOT_FS_SEGMENT_FETCHER.init(config); // directly, without sub-namespace

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
        segmentFetcher = (SegmentFetcher) Class.forName(segmentFetcherClassName).getConstructor().newInstance();
      }

      PinotConfiguration subConfig = config.subset(protocol);
      Map<String, Object> subConfigMap = subConfig.toMap();

      // Put global auth properties into sub-config if sub-config does not have auth properties
      AuthConfig authConfig = AuthProviderUtils.extractAuthConfig(config, CommonConstants.KEY_OF_AUTH);
      AuthConfig subAuthConfig = AuthProviderUtils.extractAuthConfig(subConfig, CommonConstants.KEY_OF_AUTH);
      if (subAuthConfig.getProperties().isEmpty() && !authConfig.getProperties().isEmpty()) {
        authConfig.getProperties()
            .forEach((key, value) -> subConfigMap.put(CommonConstants.KEY_OF_AUTH + "." + key, value));
      }

      segmentFetcher.init(new PinotConfiguration(subConfigMap));
      SEGMENT_FETCHER_MAP.put(protocol, segmentFetcher);
    }
  }

  /**
   * Returns the segment fetcher associated with the given protocol, or the default segment fetcher
   * ({@link HttpSegmentFetcher} for "http" and "https", {@link PinotFSSegmentFetcher} for other protocols).
   */
  public static SegmentFetcher getSegmentFetcher(String protocol) {
    SegmentFetcher segmentFetcher = SEGMENT_FETCHER_MAP.get(protocol);
    if (segmentFetcher != null) {
      return segmentFetcher;
    } else {
      LOGGER.info("Segment fetcher is not configured for protocol: {}, using default", protocol);
      if (protocol == null) {
        return PINOT_FS_SEGMENT_FETCHER;
      }
      switch (protocol) {
        case CommonConstants.HTTP_PROTOCOL:
        case CommonConstants.HTTPS_PROTOCOL:
          return HTTP_SEGMENT_FETCHER;
        default:
          return PINOT_FS_SEGMENT_FETCHER;
      }
    }
  }

  /**
   * Fetches a segment from URI location to local.
   */
  public static void fetchSegmentToLocal(URI uri, File dest)
      throws Exception {
    getSegmentFetcher(uri.getScheme()).fetchSegmentToLocal(uri, dest);
  }

  /**
   * Fetches a segment from URI location to local.
   */
  public static void fetchSegmentToLocal(String uri, File dest)
      throws Exception {
    fetchSegmentToLocal(new URI(uri), dest);
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
  public static File fetchAndStreamUntarToLocal(URI uri, File tempRootDir, long maxStreamRateInByte,
      AtomicInteger attempts)
      throws Exception {
    return getSegmentFetcher(uri.getScheme()).fetchUntarSegmentToLocalStreamed(uri, tempRootDir, maxStreamRateInByte,
        attempts);
  }

  public static File fetchAndStreamUntarToLocal(String uri, File tempRootDir, long maxStreamRateInByte,
      AtomicInteger attempts)
      throws Exception {
    return fetchAndStreamUntarToLocal(new URI(uri), tempRootDir, maxStreamRateInByte, attempts);
  }

  /**
   * Fetches a segment from a URI location to a local file and decrypts it if needed
   * @param uri remote segment location
   * @param dest local file
   */
  public static void fetchAndDecryptSegmentToLocal(String uri, File dest, @Nullable String crypterName)
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

  public static void fetchAndDecryptSegmentToLocal(String segmentName, String scheme, Supplier<List<URI>> uriSupplier,
      File dest, @Nullable String crypterName)
      throws Exception {
    SegmentFetcher segmentFetcher = getSegmentFetcher(scheme);
    if (crypterName == null) {
      segmentFetcher.fetchSegmentToLocal(segmentName, uriSupplier, dest);
    } else {
      // download
      File tempDownloadedFile = new File(dest.getPath() + ENCODED_SUFFIX);
      segmentFetcher.fetchSegmentToLocal(segmentName, uriSupplier, tempDownloadedFile);

      // decrypt
      PinotCrypter crypter = PinotCrypterFactory.create(crypterName);
      crypter.decrypt(tempDownloadedFile, dest);
    }
  }
}
