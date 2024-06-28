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
package org.apache.pinot.controller.helix.core.retention.strategy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>CompositeRetentionStrategy</code> class uses multiple strategies based on table-type to
 * manage the retention for segments.
 */
public class CompositeRetentionStrategy implements RetentionStrategy {

  private static final Logger LOGGER = LoggerFactory.getLogger(CompositeRetentionStrategy.class);
  private final List<RetentionStrategy> _retentionStrategies = new ArrayList<>();

  public CompositeRetentionStrategy(TableConfig tableConfig) {
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    String retentionTimeUnit = validationConfig.getRetentionTimeUnit();
    String retentionTimeValue = validationConfig.getRetentionTimeValue();
    RetentionStrategy retentionStrategy;
    try {
      retentionStrategy = new TimeRetentionStrategy(TimeUnit.valueOf(retentionTimeUnit.toUpperCase()),
          Long.parseLong(retentionTimeValue));
    } catch (Exception e) {
      LOGGER.warn("Invalid retention time: {} {} for table: {}, skip", retentionTimeUnit, retentionTimeValue,
          tableConfig.getTableName());
      return;
    }
    _retentionStrategies.add(retentionStrategy);
    if (TableNameBuilder.isRealtimeTableResource(tableConfig.getTableName())) {
      _retentionStrategies.add(new EmptySegmentRetentionStrategy());
    }
  }

  @Override
  public boolean isPurgeable(String tableNameWithType, SegmentZKMetadata segmentZKMetadata) {
    for (RetentionStrategy retentionStrategy : _retentionStrategies) {
      if (retentionStrategy.isPurgeable(tableNameWithType, segmentZKMetadata)) {
        return true;
      }
    }
    return false;
  }
}
