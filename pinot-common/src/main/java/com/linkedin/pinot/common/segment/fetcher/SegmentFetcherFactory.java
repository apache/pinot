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

import com.linkedin.pinot.common.utils.CommonConstants;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SegmentFetcherFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentFetcherFactory.class);

  public static String SEGMENT_FETCHER_CLASS_KEY = "class";
  public static String SEGMENT_FETCHER_PROTOCOL_KEY = "protocol";

  private static Map<String, SegmentFetcher> SEGMENT_FETCHER_MAP = new ConcurrentHashMap<String, SegmentFetcher>();

  static {
    SEGMENT_FETCHER_MAP.put("file", new LocalFileSegmentFetcher());
    SEGMENT_FETCHER_MAP.put("http", new HttpSegmentFetcher());
    SEGMENT_FETCHER_MAP.put("https", new HttpSegmentFetcher());
    SEGMENT_FETCHER_MAP.put("hdfs", new HdfsSegmentFetcher());
  }

  public static void initSegmentFetcherFactory(Configuration pinotHelixProperties) {
    Configuration segmentFetcherFactoryConfig =
        pinotHelixProperties.subset(CommonConstants.Server.PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY);

    Iterator segmentFetcherFactoryConfigIterator = segmentFetcherFactoryConfig.getKeys();
    while (segmentFetcherFactoryConfigIterator.hasNext()) {
      Object configKeyObject = segmentFetcherFactoryConfigIterator.next();
      try {
        String segmentFetcherConfigKey = configKeyObject.toString();
        String protocol = segmentFetcherConfigKey.split(".", 2)[0];
        if (!SegmentFetcherFactory.containsProtocol(protocol)) {
          SegmentFetcherFactory.initSegmentFetcher(
              new ConfigurationMap(segmentFetcherFactoryConfig.subset(protocol)));
        }
      } catch (Exception e) {
        LOGGER.error("Got exception to process the key: " + configKeyObject);
      }
    }
  }

  public static void initSegmentFetcher(Map<String, String> configs) {
    try {
      String segmentFetcherKlass = configs.get(SEGMENT_FETCHER_CLASS_KEY);
      SegmentFetcher segmentFetcher = (SegmentFetcher) Class.forName(segmentFetcherKlass).newInstance();
      segmentFetcher.init(configs);
      String segmentFetcherProtocol = configs.get(SEGMENT_FETCHER_PROTOCOL_KEY);
      SEGMENT_FETCHER_MAP.put(segmentFetcherProtocol, segmentFetcher);
    } catch (Exception e) {
      LOGGER.error("Failed to init SegmentFetcher: {}", Arrays.toString(configs.entrySet().toArray()));
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
}
