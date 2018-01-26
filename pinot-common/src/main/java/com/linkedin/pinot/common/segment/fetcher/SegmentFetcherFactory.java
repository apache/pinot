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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentFetcherFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentFetcherFactory.class);

  public static String SEGMENT_FETCHER_CLASS_KEY = "class";

  private static Map<String, SegmentFetcher> SEGMENT_FETCHER_MAP = new ConcurrentHashMap<>();
  private static Map<String, String> DEFAULT_SEGMENT_FETCHER_CLASSES = new HashMap<>();

  static {
    // If a class is not configured for a particular protocol, the following classes will be instantiated.
    DEFAULT_SEGMENT_FETCHER_CLASSES.put("file", LocalFileSegmentFetcher.class.getName());
    DEFAULT_SEGMENT_FETCHER_CLASSES.put("http", HttpSegmentFetcher.class.getName());
    DEFAULT_SEGMENT_FETCHER_CLASSES.put("https", HttpsSegmentFetcher.class.getName());
    DEFAULT_SEGMENT_FETCHER_CLASSES.put("hdfs", HdfsSegmentFetcher.class.getName());
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

  // Requirements:
  // We should instantiate a class only once (in case there is some static initialization with the class that is not idempotent).
  // We should call init only once.
  // We should be able to override an existing class from conf (i.e. instantiate and call init for the new class).
  // A config for a protocol may not specify the class name, but specify other configs params (used in init())
  public static void initSegmentFetcherFactory(Configuration segmentFetcherFactoryConfig) {

    // Iterate through the configs to instantiate any configured classes first.
    Iterator segmentFetcherFactoryConfigIterator = segmentFetcherFactoryConfig.getKeys();
    while (segmentFetcherFactoryConfigIterator.hasNext()) {
      Object configKeyObject = segmentFetcherFactoryConfigIterator.next();
      if (!configKeyObject.toString().endsWith(SEGMENT_FETCHER_CLASS_KEY)) {
        continue;
      }
      String segmentFetcherConfigKey = configKeyObject.toString();
      String protocol = segmentFetcherConfigKey.split("\\.", 2)[0];
      Configuration protocolConfig = segmentFetcherFactoryConfig.subset(protocol);
      String configuredClassName = protocolConfig.getString(SEGMENT_FETCHER_CLASS_KEY);
      if (configuredClassName == null || configuredClassName.isEmpty()) {
        if (DEFAULT_SEGMENT_FETCHER_CLASSES.containsKey(protocol)) {
          LOGGER.warn("No class name provided for {}. Using built-in class {}", protocol,
              DEFAULT_SEGMENT_FETCHER_CLASSES.get(protocol));
        } else {
          LOGGER.error("No class name provided for {}. Ignored");
        }
      } else {
        instantiateSegmentFetcher(protocol, configuredClassName);
      }
    }

    // Make sure the default classes are instantiated if there is none configured for the supported protocols.
    for (String protocol : DEFAULT_SEGMENT_FETCHER_CLASSES.keySet()) {
      if (!SEGMENT_FETCHER_MAP.containsKey(protocol)) {
        instantiateSegmentFetcher(protocol, DEFAULT_SEGMENT_FETCHER_CLASSES.get(protocol));
      }
    }

    // Call init on all the segment fetchers in the map.
    for (Map.Entry<String, SegmentFetcher> entry: SEGMENT_FETCHER_MAP.entrySet()) {
      final String protocol = entry.getKey();
      final SegmentFetcher fetcher = entry.getValue();
      try {
        LOGGER.info("Initializing segment fetcher for protocol {}, class {}", protocol, fetcher.getClass().getName());
        Configuration conf = segmentFetcherFactoryConfig.subset(protocol);
        logFetcherInitConfig(fetcher, protocol, conf);
        fetcher.init(conf);
      } catch (Exception | LinkageError e) {
        LOGGER.error("Failed to initialize SegmentFetcher for protocol {}. This protocol will not be availalble ", protocol, e);
        // If initialization fails, remove the protocol from the fetcher map.
        SEGMENT_FETCHER_MAP.remove(protocol);
      }
    }
  }

  @VisibleForTesting
  protected static Map<String, SegmentFetcher> getPreloadSegmentFetchers() {
    return SEGMENT_FETCHER_MAP;
  }

  public static boolean containsProtocol(String protocol) {
    return SEGMENT_FETCHER_MAP.containsKey(protocol);
  }

  @Nonnull
  public static SegmentFetcher getSegmentFetcherBasedOnURI(String uri) {
    String protocol = getProtocolFromUri(uri);
    SegmentFetcher segmentFetcher = SEGMENT_FETCHER_MAP.get(protocol);
    if (segmentFetcher == null) {
      throw new IllegalStateException("No segment fetcher registered for protocol: " + protocol);
    }
    return segmentFetcher;
  }

  private static String getProtocolFromUri(String uri) {
    String[] splitedUri = uri.split(":", 2);
    if (splitedUri.length > 1) {
      return splitedUri[0];
    }
    throw new UnsupportedOperationException("Not supported uri: " + uri);
  }

  private static void logFetcherInitConfig(SegmentFetcher fetcher, String protocol, Configuration conf) {
    LOGGER.info("Initializing protocol [{}] with the following configs:", protocol);
    Iterator iter = conf.getKeys();
    Set<String> secretKeys = fetcher.getProtectedConfigKeys();
    while (iter.hasNext()) {
      String key = (String) iter.next();
      if (secretKeys.contains(key)) {
        LOGGER.info("{}: {}", key, "********");
      } else {
        LOGGER.info("{}: {}", key, conf.getString(key));
      }
    }
    LOGGER.info("");
  }
}
