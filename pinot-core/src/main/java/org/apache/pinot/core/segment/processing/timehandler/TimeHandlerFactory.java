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
package org.apache.pinot.core.segment.processing.timehandler;

import com.google.common.base.Preconditions;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.Schema;


/**
 * Factory for TimeHandler.
 */
public class TimeHandlerFactory {
  private TimeHandlerFactory() {
  }

  public static TimeHandler getTimeHandler(SegmentProcessorConfig processorConfig) {
    TimeHandlerConfig timeHandlerConfig = processorConfig.getTimeHandlerConfig();
    TimeHandler.Type type = timeHandlerConfig.getType();
    switch (type) {
      case NO_OP:
        return new NoOpTimeHandler();
      case EPOCH:
        TableConfig tableConfig = processorConfig.getTableConfig();
        String timeColumn = tableConfig.getValidationConfig().getTimeColumnName();
        Preconditions
            .checkState(timeColumn != null, "Time column is not configured for table: %s", tableConfig.getTableName());
        Schema schema = processorConfig.getSchema();
        DateTimeFieldSpec dateTimeFieldSpec = schema.getSpecForTimeColumn(timeColumn);
        Preconditions.checkState(dateTimeFieldSpec != null,
            "Time column: %s is not configured as DateTimeField within the schema", timeColumn);
        return new EpochTimeHandler(dateTimeFieldSpec, timeHandlerConfig.getStartTimeMs(),
            timeHandlerConfig.getEndTimeMs(), timeHandlerConfig.getRoundBucketMs(),
            timeHandlerConfig.getPartitionBucketMs());
      default:
        throw new IllegalStateException("Unsupported time handler type: " + type);
    }
  }
}
