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

public class CacheConfig {
  private static boolean useCentralizedCache = true;
  private static boolean useInMemoryCache = false;

  public static final String COUCHBASE_AUTH_USERNAME = "thirdeye";
  public static final String COUCHBASE_AUTH_PASSWORD = "thirdeye";
  public static final String COUCHBASE_BUCKET_NAME = "travel-sample";

  // time-to-live for documents in the cache.
  public static final int TTL = 3600;

  public static boolean useCentralizedCache() { return useCentralizedCache; }
  public static boolean useInMemoryCache() { return useInMemoryCache; }

  public static void setUseCentralizedCache(boolean toggle) { useCentralizedCache = toggle; }
  public static void setUseInMemoryCache(boolean toggle) { useInMemoryCache = toggle; }
}
