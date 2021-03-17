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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.spi.crypt.PinotCrypter;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentFetcherFactory {
  private final static SegmentFetcherFactory instance = new SegmentFetcherFactory();

  static final String SEGMENT_FETCHER_CLASS_KEY_SUFFIX = ".class";
  private static final String PROTOCOLS_KEY = "protocols";
  private static final String AUTH_TOKEN_KEY = CommonConstants.KEY_OF_AUTH_TOKEN;
  private static final String ENCODED_SUFFIX = ".enc";

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentFetcherFactory.class);

  private final Map<String, SegmentFetcher> _segmentFetcherMap = new HashMap<>();
  private final SegmentFetcher _httpSegmentFetcher = new HttpSegmentFetcher();
  private final SegmentFetcher _pinotFSSegmentFetcher = new PinotFSSegmentFetcher();

  private SegmentFetcherFactory() {
    // left blank
  }

  public static SegmentFetcherFactory getInstance() {
    return instance;
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

    List<String> protocols = config.getProperty(PROTOCOLS_KEY, Arrays.asList());
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
        }
      } else {
        LOGGER.info("Creating segment fetcher for protocol: {} with class: {}", protocol, segmentFetcherClassName);
        segmentFetcher = (SegmentFetcher) Class.forName(segmentFetcherClassName).newInstance();
      }

      String authToken = config.getProperty(AUTH_TOKEN_KEY);
      Map<String, Object> subConfigMap = config.subset(protocol).toMap();
      if (!subConfigMap.containsKey(AUTH_TOKEN_KEY) && StringUtils.isNotBlank(authToken)) {
        subConfigMap.put(AUTH_TOKEN_KEY, authToken);
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
    getSegmentFetcher(uri.getScheme()).fetchSegmentToLocal(uri, dest);
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
}
