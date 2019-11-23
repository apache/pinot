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

/**
 * Config file for cache-related stuff.
 * Mapped from cache-config.yml
 */
public class CacheConfig {

  public static CacheConfig INSTANCE = new CacheConfig();

  /**
   * flags for which cache(s) to use
   */
  private static boolean useInMemoryCache;
  private static boolean useCentralizedCache;

  /**
   * settings for centralized cache.
   */
  private static CentralizedCacheConfig centralizedCacheSettings;

  // left blank
  public CacheConfig() {}

  public static CacheConfig getInstance() { return INSTANCE; }

  public boolean useCentralizedCache() { return useCentralizedCache; }
  public boolean useInMemoryCache() { return useInMemoryCache; }
  public CentralizedCacheConfig getCentralizedCacheSettings() { return centralizedCacheSettings; }

  public void setUseCentralizedCache(boolean toggle) { useCentralizedCache = toggle; }
  public void setUseInMemoryCache(boolean toggle) { useInMemoryCache = toggle; }
  public void setCentralizedCacheSettings(CentralizedCacheConfig centralizedCacheConfig) { centralizedCacheSettings = centralizedCacheConfig; }
}
