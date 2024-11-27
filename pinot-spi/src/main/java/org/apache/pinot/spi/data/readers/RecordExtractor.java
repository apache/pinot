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

import java.io.Serializable;
import java.util.Set;
import javax.annotation.Nullable;


/**
 * Extracts fields from input records
 * 1) Number/String/ByteBuffer/byte[] become single-value column
 * 2) Collections/Arrays (except for byte[]) become Object[] i.e. multi-value column
 * 3) Nested/Complex fields (e.g. json maps, avro maps, avro records) become Map<Object, Object>
 * @param <T> The format of the input record
 */
public interface RecordExtractor<T> extends Serializable {

  /**
   * Initialize the record extractor with its config
   *
   * @param fields List of field names to extract from the provided input record. If null or empty, extracts all fields.
   * @param recordExtractorConfig The record extractor config
   */
  void init(@Nullable Set<String> fields, RecordExtractorConfig recordExtractorConfig);

  /**
   * Extracts fields as listed in the sourceFieldNames from the given input record and sets them into the GenericRow
   *
   * @param from The input record
   * @param to The output GenericRow
   * @return The output GenericRow
   */
  GenericRow extract(T from, GenericRow to);

  /**
   * Converts a field of the given input record. The field value will be converted to either a single value
   * (string, number, byte[]), multi value (Object[]) or a Map.
   *
   * The converted values can be used in ingestion transforms, so it should preserve the original values as much as
   * possible (e.g. empty Object[], empty Map, Map entries with null value, etc.). Data type transformer (applied in the
   * transform pipeline) is able to transform these values into proper value according to the data type.
   *
   * @param value the field value to be converted
   * @return The converted field value
   */
  Object convert(Object value);
}
