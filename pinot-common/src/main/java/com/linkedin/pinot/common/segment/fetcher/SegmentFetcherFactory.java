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
import com.linkedin.pinot.common.utils.CommonConstants;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

  // Requirements:
  // We should instantiate a class only once (in case there is some static initialization with the class that is not idempotent).
  // We should call init only once.
  // We should be able to override an existing class from conf (i.e. instantiate and call init for the new class).
  // A config for a protocol may not specify the class name, but specify other configs params (used in init())
  public static void initSegmentFetcherFactory(Configuration pinotHelixProperties) {
    Configuration segmentFetcherFactoryConfig =
        pinotHelixProperties.subset(CommonConstants.Server.PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY);

    // Iterate through the configs to fill the map with any new configured classes or Overridden classes.
    Iterator segmentFetcherFactoryConfigIterator = segmentFetcherFactoryConfig.getKeys();
    while (segmentFetcherFactoryConfigIterator.hasNext()) {
      Object configKeyObject = segmentFetcherFactoryConfigIterator.next();
      String segmentFetcherConfigKey = configKeyObject.toString();
      String protocol = segmentFetcherConfigKey.split("\\.", 2)[0];
      Configuration protocolConfig = segmentFetcherFactoryConfig.subset(protocol);
      String configuredClassName = protocolConfig.getString(SEGMENT_FETCHER_CLASS_KEY);
      if (configuredClassName == null || configuredClassName.isEmpty()) {
        // No class name configured for this protocol
        if (SEGMENT_FETCHER_MAP.containsKey(protocol)) {
          // If the class is in the map, then it should already be instantiated, we can
          // still init it with the config provided.
          String builtinClassName = SEGMENT_FETCHER_MAP.get(protocol).getClass().getName();
          LOGGER.info("Using built-in class {} for protocol {}", builtinClassName, protocol);
        } else {
          // flag a warning since we only have a protocol name and no class name to instantiate.
          LOGGER.warn("No class name given for protocol {}. Configuration ignored", protocol);
        }
      } else {
        // A class name has been configured for this protocol. If it is same as built-in class name,
        // just use the one built-in, otherwise if it is different, then instantiate it and override it in the map.
        if (SEGMENT_FETCHER_MAP.containsKey(protocol)) {
          String builtinClassName = SEGMENT_FETCHER_MAP.get(protocol).getClass().getName();
          if (builtinClassName.equals(configuredClassName)) {
            LOGGER.info("Configured class {} same as built-in class name for protocol {}", configuredClassName, protocol);
          } else {
            LOGGER.info("Overriding class {} for protoocol {} with {}", builtinClassName, protocol, configuredClassName);
            instantiateSegmentFetcher(protocol, configuredClassName);
          }
          // If any exception happens during instantiation, the original class will be retained.
        } else {
          LOGGER.info("Instantiating new configured class {} for protocol {}", configuredClassName, protocol);
          instantiateSegmentFetcher(protocol, configuredClassName);
          // If any exception happened while instantiating this class, it will not be in the map.
        }
      }
    }

    // Call init on all the segment fetchers in the map.
    for (Map.Entry<String, SegmentFetcher> entry: SEGMENT_FETCHER_MAP.entrySet()) {
      final String protocol = entry.getKey();
      final SegmentFetcher fetcher = entry.getValue();
      try {
        LOGGER.info("Initializing segment fetcher for protocol {}, class {}", protocol, fetcher.getClass().getName());
        Configuration conf = segmentFetcherFactoryConfig.subset(protocol);
        logFetcherInitConfig(protocol, conf);
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
