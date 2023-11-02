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
package org.apache.pinot.segment.spi.datasource;

import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * This enum specify the null semantics that should be used, usually in the context of a query.
 */
public enum NullMode {
  /**
   * In this mode columns are considered not nullable. Null vectors stored in the segments are ignored and V1 query
   * engine does not expect to see null values in the blocks.
   */
  NONE_NULLABLE,
  /**
   * In this mode all columns are considerable nullable (even if {@link FieldSpec#getNullable()}) is false.
   *
   * Columns will have a null vector if and only if the segment contains a null vector for the column.
   * This is not always true, given that depends on the value of {@link IndexingConfig#isNullHandlingEnabled()} at the
   * time the segment was created.
   *
   * In this mode, V1 engine must expect to receive null blocks.
   */
  ALL_NULLABLE,
  /**
   * In this mode a column is considered nullable if and only if {@link FieldSpec#getNullable()}) is true.
   *
   * Columns will have a null vector if and only if the column is nullable and the segment contains a null vector for
   * the column.
   * This is not always true, given that depends on the value of {@link IndexingConfig#isNullHandlingEnabled()} at the
   * time the segment was created.
   *
   * In this mode, V1 engine must expect to receive null blocks.
   */
  COLUMN_BASED;

  /**
   * Returns true if and only if V1 query engine must expect nulls in their values.
   */
  public boolean nullAtQueryTime() {
    return this != NullMode.NONE_NULLABLE;
  }
}
