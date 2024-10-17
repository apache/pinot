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
package org.apache.pinot.spi.cursors;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;


@ThreadSafe
public class ResponseStoreService {
  private static volatile ResponseStoreService _instance = fromServiceLoader();

  private final Set<ResponseStore> _allResponseStores;
  private final Map<String, ResponseStore> _resultStoreByType;

  private ResponseStoreService(Set<ResponseStore> storeSet) {
    _allResponseStores = storeSet;
    _resultStoreByType = new HashMap<>();

    for (ResponseStore resultStore : storeSet) {
      _resultStoreByType.put(resultStore.getType(), resultStore);
    }
  }

  public static ResponseStoreService getInstance() {
    return _instance;
  }

  public static void setInstance(ResponseStoreService service) {
    _instance = service;
  }

  public static ResponseStoreService fromServiceLoader() {
    Set<ResponseStore> storeSet = new HashSet<>();
    for (ResponseStore resultStore : ServiceLoader.load(ResponseStore.class)) {
      storeSet.add(resultStore);
    }

    return new ResponseStoreService(storeSet);
  }

  public Set<ResponseStore> getAllResultStores() {
    return _allResponseStores;
  }

  public Map<String, ResponseStore> getResultStoresByType() {
    return _resultStoreByType;
  }

  public ResponseStore getResultStore(String type) {
    ResponseStore responseStore = _resultStoreByType.get(type);

    if (responseStore == null) {
      throw new IllegalArgumentException("Unknown ResultStore type: " + type);
    }

    return responseStore;
  }
}
