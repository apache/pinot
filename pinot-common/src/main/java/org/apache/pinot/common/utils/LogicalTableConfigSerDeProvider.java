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
package org.apache.pinot.common.utils;

import com.google.common.annotations.VisibleForTesting;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Singleton holder for {@link LogicalTableConfigSerDe}, loaded via {@link ServiceLoader}.
 *
 * <p>When multiple implementations are discovered, the one with the highest
 * {@link LogicalTableConfigSerDe#getPriority()} is selected</p>
 */
public class LogicalTableConfigSerDeProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogicalTableConfigSerDeProvider.class);

  private static volatile LogicalTableConfigSerDe _instance = fromServiceLoader();

  private LogicalTableConfigSerDeProvider() {
  }

  public static LogicalTableConfigSerDe getInstance() {
    return _instance;
  }

  /**
   * Overrides the singleton instance. Useful for testing.
   */
  @VisibleForTesting
  public static void setInstance(LogicalTableConfigSerDe serDe) {
    _instance = serDe;
  }

  private static LogicalTableConfigSerDe fromServiceLoader() {
    LogicalTableConfigSerDe best = null;
    int bestPriority = Integer.MIN_VALUE;

    for (LogicalTableConfigSerDe serDe : ServiceLoader.load(LogicalTableConfigSerDe.class)) {
      LOGGER.info("Discovered LogicalTableConfigSerDe: {} with priority {}",
          serDe.getClass().getName(), serDe.getPriority());
      if (serDe.getPriority() > bestPriority) {
        best = serDe;
        bestPriority = serDe.getPriority();
      }
    }

    if (best == null) {
      throw new RuntimeException("No implementation of LogicalTableConfigSerDe found");
    }

    LOGGER.info("Selected LogicalTableConfigSerDe: {} with priority {}",
        best.getClass().getName(), bestPriority);

    return best;
  }
}
