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
public class ResultStoreService {
  private static volatile ResultStoreService _instance = fromServiceLoader();

  private final Set<ResultStoreFactory> _allResultStoreFactories;
  private final Map<String, ResultStoreFactory> _resultStoreFactoryByType;

  private ResultStoreService(Set<ResultStoreFactory> storeSet) {
    _allResultStoreFactories = storeSet;
    _resultStoreFactoryByType = new HashMap<>();

    for (ResultStoreFactory resultStoreFactory : storeSet) {
      _resultStoreFactoryByType.put(resultStoreFactory.getType(), resultStoreFactory);
    }
  }

  public static ResultStoreService getInstance() {
    return _instance;
  }

  public static void setInstance(ResultStoreService service) {
    _instance = service;
  }

  public static ResultStoreService fromServiceLoader() {
    Set<ResultStoreFactory> storeSet = new HashSet<>();
    for (ResultStoreFactory resultStoreFactory : ServiceLoader.load(ResultStoreFactory.class)) {
      storeSet.add(resultStoreFactory);
    }

    return new ResultStoreService(storeSet);
  }

  public Set<ResultStoreFactory> getAllResultStoreFactories() {
    return _allResultStoreFactories;
  }

  public Map<String, ResultStoreFactory> getResultStoreFactoriesByType() {
    return _resultStoreFactoryByType;
  }

  public ResultStoreFactory getResultStoreFactory(String type) {
    ResultStoreFactory resultStoreFactory = _resultStoreFactoryByType.get(type);

    if (resultStoreFactory == null) {
      throw new IllegalArgumentException("Unknown ResultStoreFactory type: " + type);
    }

    return resultStoreFactory;
  }
}
