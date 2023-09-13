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
package org.apache.pinot.plugin.inputformat.csv;

import java.util.Set;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/**
 * Config for {@link CSVRecordExtractor}
 */
public class CSVRecordExtractorConfig implements RecordExtractorConfig {

  private Character _multiValueDelimiter;
  private Set<String> _columnNames;

  /**
   * Returns the CSV file's multi-value delimiter
   */
  public Character getMultiValueDelimiter() {
    return _multiValueDelimiter;
  }

  /**
   * Sets the CSV file's multi-value delimiter
   */
  public void setMultiValueDelimiter(Character multiValueDelimiter) {
    _multiValueDelimiter = multiValueDelimiter;
  }

  /**
   * Sets the CSV file's column names
   */
  public Set<String> getColumnNames() {
    if (_columnNames == null) {
      throw new IllegalStateException("CSV column names must be set in " + this.getClass().getSimpleName()
          + " if the fields to extract are not explicitly provided.");
    }
    return _columnNames;
  }

  /**
   * Returns the CSV file's column names
   */
  public void setColumnNames(Set<String> columnNames) {
    _columnNames = columnNames;
  }
}
