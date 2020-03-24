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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentFetcherFactory {
  private SegmentFetcherFactory() {
  }

  public static final String PROTOCOLS_KEY = "protocols";
  public static final String SEGMENT_FETCHER_CLASS_KEY_SUFFIX = ".class";

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentFetcherFactory.class);
  private static final Map<String, SegmentFetcher> SEGMENT_FETCHER_MAP = new HashMap<>();
  private static final String HTTP_PROTOCOL = "http";
  private static final String HTTPS_PROTOCOL = "https";
  private static final String PEER_2_PEER_PROTOCOL = "server";
  private static final SegmentFetcher DEFAULT_HTTP_SEGMENT_FETCHER = new HttpSegmentFetcher();
  private static final SegmentFetcher DEFAULT_PINOT_FS_SEGMENT_FETCHER = new PinotFSSegmentFetcher();

  static {
    Configuration emptyConfig = new BaseConfiguration();
    DEFAULT_HTTP_SEGMENT_FETCHER.init(emptyConfig);
    DEFAULT_PINOT_FS_SEGMENT_FETCHER.init(emptyConfig);
  }

  /**
   * Initializes the segment fetcher factory. This method should only be called once.
   */
  public static void init(Configuration config)
      throws Exception {
    initSegmentFetcherFactory(config, null, null);
  }

  public static void init(Configuration config, HelixManager helixManager, String helixClusterName)
      throws Exception {
    initSegmentFetcherFactory(config, helixManager, helixClusterName);
  }

  private static void initSegmentFetcherFactory(Configuration config, HelixManager helixManager, String helixClusterName)
      throws Exception {
    @SuppressWarnings("unchecked")
    List<String> protocols = config.getList(PROTOCOLS_KEY);
    for (String protocol : protocols) {
      String segmentFetcherClassName = config.getString(protocol + SEGMENT_FETCHER_CLASS_KEY_SUFFIX);
      SegmentFetcher segmentFetcher;
      if (segmentFetcherClassName == null) {
        LOGGER.info("Segment fetcher class is not configured for protocol: {}, using default", protocol);
        switch (protocol) {
          case HTTP_PROTOCOL:
            segmentFetcher = new HttpSegmentFetcher();
            break;
          case HTTPS_PROTOCOL:
            segmentFetcher = new HttpsSegmentFetcher();
            break;
          case PEER_2_PEER_PROTOCOL:
            if (helixManager != null && helixClusterName != null) {
              segmentFetcher = new PeerServerSegmentFetcher(helixManager, helixClusterName);
              break;
            }
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
        case HTTP_PROTOCOL:
        case HTTPS_PROTOCOL:
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
}
