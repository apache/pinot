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

import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.spi.crypt.PinotCrypter;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentFetcherFactory {
  private SegmentFetcherFactory() {
  }

  static final String SEGMENT_FETCHER_CLASS_KEY_SUFFIX = ".class";
  private static final String PROTOCOLS_KEY = "protocols";
  private static final String ENCODED_SUFFIX = ".enc";

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentFetcherFactory.class);
  private static final Map<String, SegmentFetcher> SEGMENT_FETCHER_MAP = new HashMap<>();
  private static final SegmentFetcher DEFAULT_HTTP_SEGMENT_FETCHER = new HttpSegmentFetcher();
  private static final SegmentFetcher DEFAULT_PINOT_FS_SEGMENT_FETCHER = new PinotFSSegmentFetcher();

  static {
    PinotConfiguration emptyConfig = new PinotConfiguration();
    DEFAULT_HTTP_SEGMENT_FETCHER.init(emptyConfig);
    DEFAULT_PINOT_FS_SEGMENT_FETCHER.init(emptyConfig);
  }

  /**
   * Initializes the segment fetcher factory. This method should only be called once.
   */
  public static void init(PinotConfiguration config)
      throws Exception {
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
      segmentFetcher.init(config.subset(protocol));
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
      switch (protocol) {
        case CommonConstants.HTTP_PROTOCOL:
        case CommonConstants.HTTPS_PROTOCOL:
          return DEFAULT_HTTP_SEGMENT_FETCHER;
        default:
          return DEFAULT_PINOT_FS_SEGMENT_FETCHER;
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
   * Fetches a segment from a URI location to a local file and decrypts it if needed
   * @param uri remote segment location
   * @param dest local file
   */
  public static void fetchAndDecryptSegmentToLocal(String uri, File dest, String crypterName)
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
