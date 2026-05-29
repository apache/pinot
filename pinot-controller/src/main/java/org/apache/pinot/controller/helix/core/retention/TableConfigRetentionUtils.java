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
package org.apache.pinot.controller.helix.core.retention;

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.controller.helix.core.retention.strategy.RetentionStrategy;
import org.apache.pinot.controller.helix.core.retention.strategy.TimeRetentionStrategy;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Utility methods for deriving retention-related objects from a {@link TableConfig}.
public class TableConfigRetentionUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableConfigRetentionUtils.class);

  private TableConfigRetentionUtils() {
  }

  /// Builds a {@link RetentionStrategy} from {@code tableConfig}, or returns {@code null} when the
  /// retention config is absent, empty, or malformed. A null return means no retention is configured
  /// and no segment should be treated as purgeable.
  ///
  /// @param useCreationTimeFallback when true, the strategy falls back to segment creation time
  ///                                when segment end time is unavailable
  @Nullable
  public static RetentionStrategy buildRetentionStrategy(TableConfig tableConfig,
      boolean useCreationTimeFallback) {
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    String unit = validationConfig.getRetentionTimeUnit();
    String value = validationConfig.getRetentionTimeValue();
    if (unit == null || unit.isEmpty() || value == null || value.isEmpty()) {
      LOGGER.warn("Invalid retention time: {} {} for table: {}, skip", unit, value, tableConfig.getTableName());
      return null;
    }
    try {
      return new TimeRetentionStrategy(TimeUnit.valueOf(unit.toUpperCase(Locale.ROOT)), Long.parseLong(value),
          useCreationTimeFallback);
    } catch (Exception e) {
      LOGGER.warn("Invalid retention time: {} {} for table: {}, skip", unit, value, tableConfig.getTableName());
      return null;
    }
  }
}
