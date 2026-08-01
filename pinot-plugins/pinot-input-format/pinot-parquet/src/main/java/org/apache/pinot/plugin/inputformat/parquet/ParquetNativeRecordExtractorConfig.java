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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.parquet.schema.GroupType;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/// Config for [ParquetNativeRecordExtractor]. See [ParquetRecordReaderConfig] for the meaning of
/// `extractRawTimeValues`.
public class ParquetNativeRecordExtractorConfig implements RecordExtractorConfig {
  public static final String EXTRACT_RAW_TIME_VALUES = "extractRawTimeValues";

  private boolean _extractRawTimeValues;
  @Nullable
  private GroupType _parquetSchema;

  @Override
  public void init(Map<String, String> props) {
    _extractRawTimeValues = Boolean.parseBoolean(props.get(EXTRACT_RAW_TIME_VALUES));
  }

  public boolean isExtractRawTimeValues() {
    return _extractRawTimeValues;
  }

  public void setExtractRawTimeValues(boolean extractRawTimeValues) {
    _extractRawTimeValues = extractRawTimeValues;
  }

  /// Supplies the immutable Parquet record schema used to initialize schema-bound logical-type converters.
  ///
  /// <p>The native record reader sets this before initializing the extractor. Direct extractor users should do the
  /// same when the schema contains VARIANT columns; otherwise the extractor initializes those converters from the
  /// first record as a compatibility fallback.
  public void setParquetSchema(GroupType parquetSchema) {
    _parquetSchema = parquetSchema;
  }

  @Nullable
  GroupType getParquetSchema() {
    return _parquetSchema;
  }
}
