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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.linkedin.pinot.common.utils.CommonConstants;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SegmentFetcherFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentFetcherFactory.class);

  public static String SEGMENT_FETCHER_CLASS_KEY = "class";

  private static Map<String, SegmentFetcher> SEGMENT_FETCHER_MAP = new ConcurrentHashMap<>();

  static {
    instantiateSegmentFetcher("file", LocalFileSegmentFetcher.class);
    instantiateSegmentFetcher("http", HttpSegmentFetcher.class);
    instantiateSegmentFetcher("https", HttpSegmentFetcher.class);
    instantiateSegmentFetcher("hdfs", "com.linkedin.pinot.common.segment.fetcher.HdfsSegmentFetcher");
  }

  private static <T extends SegmentFetcher> void instantiateSegmentFetcher(String protocol, Class<T> clazz) {
    try {
      SegmentFetcher fetcher = clazz.newInstance();
      SEGMENT_FETCHER_MAP.put(protocol, fetcher);
    } catch (Exception | LinkageError e) {
      LOGGER.warn(
          "Caught exception while instantiating segment fetcher for protocol {} and class name {}, this protocol will not be available.",
          protocol, clazz.getName(), e);
    }
  }

  private static void instantiateSegmentFetcher(String protocol, String className) {
    try {
      instantiateSegmentFetcher(protocol, (Class<SegmentFetcher>) Class.forName(className));
    } catch (Exception | LinkageError e) {
      LOGGER.warn(
          "Caught exception while instantiating segment fetcher for protocol {} and class name {}, this protocol will not be available.",
          protocol, className, e);
    }
  }

  public static void initSegmentFetcherFactory(Configuration pinotHelixProperties) {
    Configuration segmentFetcherFactoryConfig =
        pinotHelixProperties.subset(CommonConstants.Server.PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY);

    // initialize predefined fetcher
    for (String protocol: SEGMENT_FETCHER_MAP.keySet()) {
      LOGGER.info("initializing segment fetcher for protocol [{}]", protocol);
      Configuration conf = segmentFetcherFactoryConfig.subset(protocol);
      logFetcherInitConfig(protocol, conf);
      initSegmentFetcher(protocol, conf);
    }

    // initialize dynamic loaded fetcher
    Iterator segmentFetcherFactoryConfigIterator = segmentFetcherFactoryConfig.getKeys();
    while (segmentFetcherFactoryConfigIterator.hasNext()) {
      Object configKeyObject = segmentFetcherFactoryConfigIterator.next();
      try {
        String segmentFetcherConfigKey = configKeyObject.toString();
        String protocol = segmentFetcherConfigKey.split("\\.", 2)[0];
        if (!SegmentFetcherFactory.containsProtocol(protocol)) {
          LOGGER.info("initializing segment fetcher for protocol [{}]", protocol);
          Configuration conf = segmentFetcherFactoryConfig.subset(protocol);
          logFetcherInitConfig(protocol, conf);
          SegmentFetcherFactory.initSegmentFetcher(protocol, conf);
        }
      } catch (Exception e) {
        LOGGER.error("Got exception to process the key: " + configKeyObject);
      }
    }
  }

  @VisibleForTesting
  protected static Map<String, SegmentFetcher> getPreloadSegmentFetchers() {
    return SEGMENT_FETCHER_MAP;
  }

  private static void initSegmentFetcher(String protocol, Configuration configs) {
    try {
      final SegmentFetcher fetcher;
      if (SEGMENT_FETCHER_MAP.containsKey(protocol)) {
        fetcher = SEGMENT_FETCHER_MAP.get(protocol);
      } else {
        String segmentFetcherKlass = configs.getString(SEGMENT_FETCHER_CLASS_KEY);
        if (Strings.isNullOrEmpty(segmentFetcherKlass)) {
          throw new RuntimeException("No class def for provided segment fetcher " + protocol);
        }
        fetcher = (SegmentFetcher) Class.forName(segmentFetcherKlass).newInstance();
        SEGMENT_FETCHER_MAP.put(protocol, fetcher);
      }
      fetcher.init(configs);
    } catch (Exception | LinkageError e) {
      LOGGER.error("Failed to init SegmentFetcher: " + protocol, e);
      // If initialization fails, remove the protocol from the fetcher map.
      SEGMENT_FETCHER_MAP.remove(protocol);
    }
  }

  public static boolean containsProtocol(String protocol) {
    return SEGMENT_FETCHER_MAP.containsKey(protocol);
  }

  public static SegmentFetcher getSegmentFetcherBasedOnURI(String uri) {
    String protocol = getProtocolFromUri(uri);
    return SEGMENT_FETCHER_MAP.get(protocol);
  }

  private static String getProtocolFromUri(String uri) {
    String[] splitedUri = uri.split(":", 2);
    if (splitedUri.length > 1) {
      return splitedUri[0];
    }
    throw new UnsupportedOperationException("Not supported uri: " + uri);
  }

  private static void logFetcherInitConfig(String protocol, Configuration conf) {
    LOGGER.info("Initializing protocol [{}] with the following configs:", protocol);
    Iterator iter = conf.getKeys();
    while (iter.hasNext()) {
      String key = (String) iter.next();
      LOGGER.info("{}: {}", key, conf.getString(key));
    }
    LOGGER.info("");
  }
}
