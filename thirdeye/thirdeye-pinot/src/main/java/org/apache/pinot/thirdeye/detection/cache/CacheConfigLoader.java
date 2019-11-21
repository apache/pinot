/*
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

package org.apache.pinot.thirdeye.detection.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.net.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper methods to load cache config
 */
public class CacheConfigLoader {
  private static final Logger LOG = LoggerFactory.getLogger(CacheConfigLoader.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  /**
   * Returns cache config from yml file
   * @param cacheConfigUrl URL to cache config
   * @return cacheConfig
   */
  public static CacheConfig fromCacheConfigUrl(URL cacheConfigUrl) {
    CacheConfig cacheConfig = null;
    try {
      cacheConfig = OBJECT_MAPPER.readValue(cacheConfigUrl, CacheConfig.class);
    } catch (IOException e) {
      LOG.error("Exception in reading cache config {}", cacheConfigUrl, e);
    }
    return cacheConfig;
  }
}
