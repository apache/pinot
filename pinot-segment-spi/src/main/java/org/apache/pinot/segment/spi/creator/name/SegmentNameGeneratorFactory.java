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
package org.apache.pinot.segment.spi.creator.name;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.IngestionConfigUtils;


public class SegmentNameGeneratorFactory {
  public static final String SIMPLE_SEGMENT_NAME_GENERATOR = "simple";
  public static final String NORMALIZED_DATE_SEGMENT_NAME_GENERATOR = "normalizeddate";

  private SegmentNameGeneratorFactory() {
  }

  /**
   * Create the segment name generator given input configurations
   */
  public static SegmentNameGenerator createSegmentNameGenerator(TableConfig tableConfig, Schema schema,
      @Nullable String prefix, @Nullable String postfix, boolean excludeSequenceId) {
    String segmentNameGeneratorType = tableConfig.getIndexingConfig().getSegmentNameGeneratorType();
    if (segmentNameGeneratorType == null || segmentNameGeneratorType.isEmpty()) {
      segmentNameGeneratorType = SIMPLE_SEGMENT_NAME_GENERATOR;
    }

    String tableName = tableConfig.getTableName();
    switch (segmentNameGeneratorType.toLowerCase()) {
      case SIMPLE_SEGMENT_NAME_GENERATOR:
        if (prefix != null) {
          return new SimpleSegmentNameGenerator(prefix, postfix);
        }
        return new SimpleSegmentNameGenerator(tableName, postfix);
      case NORMALIZED_DATE_SEGMENT_NAME_GENERATOR:
        SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
        DateTimeFormatSpec dateTimeFormatSpec = null;
        String timeColumnName = validationConfig.getTimeColumnName();
        if (timeColumnName != null) {
          DateTimeFieldSpec dateTimeFieldSpec = schema.getSpecForTimeColumn(timeColumnName);
          Preconditions.checkNotNull(dateTimeFieldSpec,
              "Schema does not contain the time column specified in the table config.");
          dateTimeFormatSpec = new DateTimeFormatSpec(dateTimeFieldSpec.getFormat());
        }
        return new NormalizedDateSegmentNameGenerator(tableName, prefix, excludeSequenceId,
            IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig),
            IngestionConfigUtils.getBatchSegmentIngestionFrequency(tableConfig), dateTimeFormatSpec, postfix);
      default:
        throw new UnsupportedOperationException("Unsupported segment name generator type: " + segmentNameGeneratorType);
    }
  }
}
