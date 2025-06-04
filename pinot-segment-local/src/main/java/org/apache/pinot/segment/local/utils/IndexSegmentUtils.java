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
package org.apache.pinot.segment.local.utils;

import org.apache.pinot.segment.local.segment.index.column.DefaultNullValueVirtualColumnProvider;
import org.apache.pinot.segment.local.segment.index.datasource.ImmutableDataSource;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProvider;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexSegmentUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexSegmentUtils.class);

  private IndexSegmentUtils() {
    // Utility class, no instantiation
  }

  /**
   * Get a virtual data source for the given column in the table.
   * First check if the column is present in the segment schema, if not then check in the table schema.
   * Since virtual columns like $segmentName, $docId, etc. are not present in the table schema.
   *
   * @param tableSchema Schema of the table, this the latest schema of the table.
   * @param segmentSchema Schema of the segment, this contains virtual columns like $segmentName, $docId, etc.
   * @param column Column name for which the data source is needed
   * @param totalDocCount Total document count for the column
   * @return DataSource for the column
   */
  public static DataSource getVirtualDataSource(Schema tableSchema, Schema segmentSchema,
                                                String column, int totalDocCount) {
    FieldSpec fieldSpec = segmentSchema.getFieldSpecFor(column);
    if (fieldSpec == null) {
      fieldSpec = tableSchema.getFieldSpecFor(column);
      if (fieldSpec == null) {
        // If the column is not present in the table schema. There could be two possibilities:
        // 1. Table Schema does not have the latest column. We rely on helix-refresh to update the table schema.
        //    so there could be a delay in the refresh message reaching the server causing this issue.
        // 2. The column could be invalid, this is highly unlikely as invalid columns are pruned at broker and server.
        LOGGER.warn("Column: {} is not present in the table schema: {}", column, tableSchema.getSchemaName());
        return null;
      }
    }
    String virtualColumnProviderName = fieldSpec.getVirtualColumnProvider();
    if (virtualColumnProviderName == null) {
      // Set the default virtual column provider if it is not set
      virtualColumnProviderName = DefaultNullValueVirtualColumnProvider.class.getName();
    }
    VirtualColumnContext virtualColumnContext = new VirtualColumnContext(fieldSpec, totalDocCount);
    VirtualColumnProvider virtualColumnProvider = VirtualColumnProviderFactory.buildProvider(virtualColumnProviderName);
    return new ImmutableDataSource(virtualColumnProvider.buildMetadata(virtualColumnContext),
        virtualColumnProvider.buildColumnIndexContainer(virtualColumnContext));
  }
}
