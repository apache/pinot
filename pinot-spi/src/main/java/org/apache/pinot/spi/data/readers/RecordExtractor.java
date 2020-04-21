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
package org.apache.pinot.spi.data.readers;

import java.util.Set;


/**
 * Extracts fields from input records
 * @param <T> The format of the input record
 */
public interface RecordExtractor<T> {

  /**
   * Initialize the record extractor with its config
   *
   * @param fields List of field names to extract from the provided input record
   * @param recordReaderConfig The record reader config
   */
  void init(Set<String> fields, RecordReaderConfig recordReaderConfig);

  /**
   * Extracts fields as listed in the sourceFieldNames from the given input record and sets them into the GenericRow
   *
   * @param from The input record
   * @param to The output GenericRow
   * @return The output GenericRow
   */
  GenericRow extract(T from, GenericRow to);
}
