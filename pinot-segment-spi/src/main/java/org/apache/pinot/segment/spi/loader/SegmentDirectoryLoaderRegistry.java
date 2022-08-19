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
package org.apache.pinot.segment.spi.loader;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to dynamically register all {@link SegmentDirectoryLoader} annotated with {@link SegmentLoader}
 */
public class SegmentDirectoryLoaderRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDirectoryLoaderRegistry.class);
  public static final String DEFAULT_SEGMENT_DIRECTORY_LOADER_NAME = "default";
  private static final Map<String, SegmentDirectoryLoader> SEGMENT_DIRECTORY_LOADER_MAP = new ConcurrentHashMap<>();

  private SegmentDirectoryLoaderRegistry() {
  }

  static {
    long startTime = System.currentTimeMillis();

    Set<Class<?>> loaderClasses =
        PinotReflectionUtils.getClassesThroughReflection("org.apache.pinot.segment", ".*\\.loader\\..*",
            SegmentLoader.class);
    for (Class<?> loaderClass : loaderClasses) {
      SegmentLoader segmentLoaderAnnotation = loaderClass.getAnnotation(SegmentLoader.class);
      if (segmentLoaderAnnotation.enabled()) {
        if (segmentLoaderAnnotation.name().isEmpty()) {
          LOGGER.error("Cannot register an unnamed SegmentDirectoryLoader for annotation {} ", segmentLoaderAnnotation);
        } else {
          String segmentLoaderName = segmentLoaderAnnotation.name();
          SegmentDirectoryLoader segmentDirectoryLoader;
          try {
            segmentDirectoryLoader = (SegmentDirectoryLoader) loaderClass.newInstance();
            SEGMENT_DIRECTORY_LOADER_MAP.putIfAbsent(segmentLoaderName, segmentDirectoryLoader);
          } catch (Exception e) {
            LOGGER.error(
                String.format("Unable to register SegmentDirectoryLoader %s . Cannot instantiate.", segmentLoaderName),
                e);
          }
        }
      }
    }

    LOGGER.info("Initialized SegmentDirectoryLoaderRegistry with {} segmentDirectoryLoaders: {} in {} ms",
        SEGMENT_DIRECTORY_LOADER_MAP.size(), SEGMENT_DIRECTORY_LOADER_MAP.keySet(),
        (System.currentTimeMillis() - startTime));
  }

  /**
   * Returns the segment directory loader instance from instantiated map, for the given segmentDirectoryLoader name
   */
  public static SegmentDirectoryLoader getSegmentDirectoryLoader(String segmentDirectoryLoader) {
    return SEGMENT_DIRECTORY_LOADER_MAP.get(segmentDirectoryLoader);
  }

  /**
   * Explicitly adds a {@link SegmentDirectoryLoader} to the map
   */
  public static void setSegmentDirectoryLoader(String segmentDirectoryLoaderName, SegmentDirectoryLoader loader) {
    SEGMENT_DIRECTORY_LOADER_MAP.put(segmentDirectoryLoaderName, loader);
  }

  /**
   * Returns the 'default' {@link SegmentDirectoryLoader}
   */
  public static SegmentDirectoryLoader getDefaultSegmentDirectoryLoader() {
    return SEGMENT_DIRECTORY_LOADER_MAP.get(DEFAULT_SEGMENT_DIRECTORY_LOADER_NAME);
  }
}
