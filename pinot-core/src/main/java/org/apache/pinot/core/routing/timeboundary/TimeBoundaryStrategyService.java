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
package org.apache.pinot.core.routing.timeboundary;

import java.util.Map;
import java.util.ServiceLoader;


public class TimeBoundaryStrategyService {

  private static volatile TimeBoundaryStrategyService _instance = fromServiceLoader();
  private final Map<String, TimeBoundaryStrategy> _strategyMap;

  private TimeBoundaryStrategyService(Map<String, TimeBoundaryStrategy> strategyMap) {
    _strategyMap = strategyMap;
  }

  public static TimeBoundaryStrategyService fromServiceLoader() {
    Map<String, TimeBoundaryStrategy> strategyMap = new java.util.HashMap<>();
    for (TimeBoundaryStrategy strategy : ServiceLoader.load(TimeBoundaryStrategy.class)) {
      String strategyName = strategy.getName();
      if (strategyMap.containsKey(strategyName)) {
        throw new IllegalStateException("Duplicate TimeBoundaryStrategy found: " + strategyName);
      }
      strategyMap.put(strategyName, strategy);
    }
    return new TimeBoundaryStrategyService(strategyMap);
  }

  /**
   * Returns the singleton instance of the TimeBoundaryStrategyService.
   *
   * @return The singleton instance of the TimeBoundaryStrategyService.
   */
  public static TimeBoundaryStrategyService getInstance() {
    return _instance;
  }

  /**
   * Sets the singleton instance of the TimeBoundaryStrategyService.
   *
   * @param service The new instance to set.
   */
  public static void setInstance(TimeBoundaryStrategyService service) {
    _instance = service;
  }

  public TimeBoundaryStrategy getTimeBoundaryStrategy(String name) {
    TimeBoundaryStrategy strategy = _instance._strategyMap.get(name);
    if (strategy == null) {
      throw new IllegalArgumentException("No TimeBoundaryStrategy found for name: " + name);
    }
    return strategy;
  }
}
