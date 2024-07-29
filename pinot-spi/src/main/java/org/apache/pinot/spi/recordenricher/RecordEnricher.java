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
package org.apache.pinot.spi.recordenricher;

import java.util.List;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Interface for enriching records.
 * If a column with the same name as the input column already exists in the record, it will be overwritten.
 */
public interface RecordEnricher {
  /**
   * Returns the list of input columns required for enriching the record.
   * This is used to make sure the required input fields are extracted.
   */
  List<String> getInputColumns();

  /**
   * Enriches the given record, by adding new columns to the same record.
   */
  void enrich(GenericRow record);
}
