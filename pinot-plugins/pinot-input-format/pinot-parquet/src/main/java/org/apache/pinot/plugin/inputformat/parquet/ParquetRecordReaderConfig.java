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
package org.apache.pinot.plugin.inputformat.parquet;

import org.apache.pinot.spi.data.readers.RecordReaderConfig;


/// Config for [ParquetRecordReader] (and the underlying [ParquetAvroRecordReader] / [ParquetNativeRecordReader]).
/// Three settings, all default `false`:
/// - `useParquetAvroRecordReader` — force the parquet-avro reader.
/// - `useParquetNativeRecordReader` — force the native parquet reader. When neither flag is set, the dispatcher
///   auto-detects via the file's `avro.schema` metadata (Avro reader if present, native otherwise).
/// - `extractRawTimeValues` — opt out of TIMESTAMP / DATE / TIME conversion at the extractor boundary,
///   surfacing the raw underlying integer in the column's declared unit instead of the contract Java type.
///   DECIMAL and UUID always convert. See [ParquetAvroRecordExtractor] / [ParquetNativeRecordExtractor] for
///   the per-type matrix.
public class ParquetRecordReaderConfig implements RecordReaderConfig {
  private boolean _useParquetAvroRecordReader;
  private boolean _useParquetNativeRecordReader;
  private boolean _extractRawTimeValues;

  public boolean useParquetAvroRecordReader() {
    return _useParquetAvroRecordReader;
  }

  public void setUseParquetAvroRecordReader(boolean useParquetAvroRecordReader) {
    _useParquetAvroRecordReader = useParquetAvroRecordReader;
  }

  public boolean useParquetNativeRecordReader() {
    return _useParquetNativeRecordReader;
  }

  public void setUseParquetNativeRecordReader(boolean useParquetNativeRecordReader) {
    _useParquetNativeRecordReader = useParquetNativeRecordReader;
  }

  public boolean isExtractRawTimeValues() {
    return _extractRawTimeValues;
  }

  public void setExtractRawTimeValues(boolean extractRawTimeValues) {
    _extractRawTimeValues = extractRawTimeValues;
  }
}
