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
package org.apache.pinot.core.routing;

import java.util.Map;
import org.apache.pinot.common.config.provider.TableCache;


/**
 * A generic class which provides the dependencies for federation routing.
 */
public class FederationProvider {
  // Maps clusterName to TableCache.
  Map<String, TableCache> _tableCacheMap;

  public FederationProvider(Map<String, TableCache> tableCacheMap) {
    _tableCacheMap = tableCacheMap;
  }

  public Map<String, TableCache> getTableCacheMap() {
    return _tableCacheMap;
  }

  public TableCache getTableCache(String clusterName) {
    return _tableCacheMap.get(clusterName);
  }
}
