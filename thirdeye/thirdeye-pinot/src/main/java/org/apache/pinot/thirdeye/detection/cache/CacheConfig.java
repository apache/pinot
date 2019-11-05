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

  /**
   * flags for which cache to use; recommended to only use one at a time
   */
  private static boolean useInMemoryCache;
  private static boolean useCentralizedCache;

  /**
   * config values for accessing the centralized cache. should be set from CentralizedCacheConfig values.
   * these are added here for ease-of-access and making code a little more readable.
   */
  private static String authUsername;
  private static String authPassword;
  private static String bucketName;

  /**
   * settings for centralized cache.
   */
  private static CentralizedCacheConfig centralizedCacheSettings;

  // left blank
  public CacheConfig() {}

  public static boolean useCentralizedCache() { return useCentralizedCache; }
  public static boolean useInMemoryCache() { return useInMemoryCache; }
  public static CentralizedCacheConfig getCentralizedCacheSettings() { return centralizedCacheSettings; }

  public static String getAuthUsername() { return authUsername; }
  public static String getAuthPassword() { return authPassword; }
  public static String getBucketName() { return bucketName; }

  public void setUseCentralizedCache(boolean toggle) { useCentralizedCache = toggle; }
  public void setUseInMemoryCache(boolean toggle) { useInMemoryCache = toggle; }
  public void setCentralizedCacheSettings(CentralizedCacheConfig centralizedCacheConfig) { centralizedCacheSettings = centralizedCacheConfig; }

  public void setAuthUsername(String authUsername) { CacheConfig.authUsername = authUsername; }
  public void setAuthPassword(String authPassword) { CacheConfig.authPassword = authPassword; }
  public void setBucketName(String bucketName) { CacheConfig.bucketName = bucketName; }

}
