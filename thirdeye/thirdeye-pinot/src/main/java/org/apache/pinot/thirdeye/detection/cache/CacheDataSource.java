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

import java.util.Map;


/**
 * Config for a single centralized cache data source.
 * For example, this class could be for Couchbase, or Redis, or Cassandra, etc.
 */
public class CacheDataSource {

  /**
   * class name, e.g. org.apache.pinot.thirdeye.detection.cache.CouchbaseCacheDAO
   */
  private String className;

  /**
   * settings/config for the specific data source. generic since different
   * data stores may have different authentication methods.
   */
  private Map<String, Object> config;

  // left blank
  public CacheDataSource() {}

  public String getClassName() { return className; }
  public Map<String, Object> getConfig() { return config; }

  public void setClassName(String className) { this.className = className; }
  public void setConfig(Map<String, Object> config) { this.config = config; }
}
