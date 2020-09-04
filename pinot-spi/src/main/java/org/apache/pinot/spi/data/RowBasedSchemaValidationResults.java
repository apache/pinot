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
package org.apache.pinot.spi.data;

import java.util.HashSet;
import java.util.Set;


/**
 * This is the extension class on top of {@code SchemaValidationResults} class, since row based schema validation will
 * be called much more frequently than the file base schema validation. We collect all the mismatch columns into the hash
 * set and then generate the mismatch message all at once.
 */
public class RowBasedSchemaValidationResults extends SchemaValidationResults {

  private Set<String> _dataTypeMismatchColumns = new HashSet<>();
  private Set<String> _singleValueMultiValueFieldMismatchColumns = new HashSet<>();
  private Set<String> _multiValueStructureMismatchColumns = new HashSet<>();

  public void collectDataTypeMismatchColumns(Set<String> columns) {
    _dataTypeMismatchColumns.addAll(columns);
  }

  public void collectSingleValueMultiValueFieldMismatchColumns(Set<String> columns) {
    _singleValueMultiValueFieldMismatchColumns.addAll(columns);
  }

  public void collectMultiValueStructureMismatchColumns(Set<String> columns) {
    _multiValueStructureMismatchColumns.addAll(columns);
  }

  public void gatherRowBasedSchemaValidationResults() {
    if (!_dataTypeMismatchColumns.isEmpty()) {
      _dataTypeMismatch.addMismatchReason(String.format("Found data type mismatch from the following Pinot columns: %s",
          _dataTypeMismatchColumns.toString()));
    }
    if (!_singleValueMultiValueFieldMismatchColumns.isEmpty()) {
      _singleValueMultiValueFieldMismatch.addMismatchReason(String
          .format("Found single-value multi-value field mismatch from the following Pinot columns: %s",
              _singleValueMultiValueFieldMismatchColumns.toString()));
    }
    if (!_multiValueStructureMismatchColumns.isEmpty()) {
      _multiValueStructureMismatch.addMismatchReason(String
          .format("Found multi-value structure mismatch from the following Pinot columns: %s",
              _multiValueStructureMismatchColumns.toString()));
    }
  }
}
