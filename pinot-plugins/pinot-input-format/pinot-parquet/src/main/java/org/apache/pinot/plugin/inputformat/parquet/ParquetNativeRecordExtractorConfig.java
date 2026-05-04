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
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/// Config for [ParquetNativeRecordExtractor]. See [ParquetRecordReaderConfig] for the meaning of
/// `extractRawTimeValues`.
public class ParquetNativeRecordExtractorConfig implements RecordExtractorConfig {
  public static final String EXTRACT_RAW_TIME_VALUES = "extractRawTimeValues";

  private boolean _extractRawTimeValues;

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
}
