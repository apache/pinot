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

import java.util.HashMap;
import java.util.Map;


/**
 * settings for centralized caches
 */
public class CentralizedCacheConfig {

  /**
   * time-to-live for documents in the cache before they naturally expire
   */
  private int ttl;

  /**
   * if inserting documents in parallel, the number of threads to spawn for inserting them
   */
  private int maxParallelInserts;

  /**
   * name of the single data store we are choosing to use from the data sources provided
   */
  private String cacheDataStoreName;

  /**
   * Map of data sources available for us to choose from
   */
  private Map<String, CacheDataSource> cacheDataSources = new HashMap<>();

  // left blank
  public CentralizedCacheConfig() {}

  public int getTTL() { return ttl; }
  public int getMaxParallelInserts() { return maxParallelInserts; }
  public String getCacheDataStoreName() { return cacheDataStoreName; }
  public Map<String, CacheDataSource> getCacheDataSources() { return cacheDataSources; }

  /**
   * shorthand to get the config for the single data source specified in the config file
   * @return CacheDataSource with auth info
   */
  public CacheDataSource getDataSourceConfig() { return cacheDataSources.get(cacheDataStoreName);  }

  public void setTTL(int ttl) { this.ttl = ttl; }
  public void setMaxParallelInserts(int maxParallelInserts) { this.maxParallelInserts = maxParallelInserts; }
  public void setCacheDataStoreName(String cacheDataStoreName) { this.cacheDataStoreName = cacheDataStoreName;  }
  public void setCacheDataSources(Map<String, CacheDataSource> cacheDataSources) { this.cacheDataSources = cacheDataSources; }
}
