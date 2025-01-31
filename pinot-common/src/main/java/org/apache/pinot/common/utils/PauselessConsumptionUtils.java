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

import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;


public class PauselessConsumptionUtils {

  private PauselessConsumptionUtils() {
    // Private constructor to prevent instantiation of utility class
  }

  /**
   * Checks if pauseless consumption is enabled for the given table configuration.
   * Returns false if any configuration component is missing or if the flag is not set to true.
   *
   * @param tableConfig The table configuration to check. Must not be null.
   * @return true if pauseless consumption is explicitly enabled, false otherwise
   * @throws NullPointerException if tableConfig is null
   */
  public static boolean isPauselessEnabled(@Nullable TableConfig tableConfig) {
    return Optional.ofNullable(tableConfig)
        .map(TableConfig::getIngestionConfig)
        .map(IngestionConfig::getStreamIngestionConfig)
        .map(StreamIngestionConfig::isPauselessConsumptionEnabled)
        .orElse(false);
  }
}
