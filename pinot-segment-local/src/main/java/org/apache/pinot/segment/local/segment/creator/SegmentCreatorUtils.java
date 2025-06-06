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
package org.apache.pinot.segment.local.segment.creator;

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for segment creation.
 */
public class SegmentCreatorUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentCreatorUtils.class);

  private SegmentCreatorUtils() {
  }

  /**
   * Checks if the row should be skipped based on the not-null columns.
   * @param notNullColumns the set of not-null columns
   * @param row the row to check
   * @return true if the row should be skipped, false otherwise
   */
  public static boolean shouldSkipRowForNotNull(Set<String> notNullColumns, GenericRow row) {
    for (String columnName : notNullColumns) {
      if (row.getValue(columnName) == null) {
        LOGGER.debug("Skipping Row due to null value for column {}, the whole row is {}", columnName, row);
        return true;
      }
    }
    return false;
  }

  /**
   * Extracts the set of not-null columns from the schema.
   * @param schema the schema to extract not-null columns from
   * @return a set of not-null column names, when column-based null handling is enabled; otherwise, an empty set
   */
  public static Set<String> extractNotNullColumns(Schema schema) {
    if (schema == null || !schema.isEnableColumnBasedNullHandling()) {
      return Set.of();
    }
    return schema.getAllFieldSpecs().stream()
        .filter(FieldSpec::isNotNull)
        .map(FieldSpec::getName)
        .collect(Collectors.toSet());
  }
}
