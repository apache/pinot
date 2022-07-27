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

import org.apache.commons.configuration.Configuration;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


/**
 * Config for ParquetRecordReader
 */
public class ParquetRecordReaderConfig implements RecordReaderConfig {
  private static final String USE_PARQUET_AVRO_RECORDER_READER = "useParquetAvroRecordReader";
  private static final String TREAT_BINARY_AS_STRING = "treatBinaryAsString";
  private boolean _useParquetAvroRecordReader = true;
  private boolean _treatBinaryAsString = false;
  private Configuration _conf;

  public ParquetRecordReaderConfig() {
  }

  public ParquetRecordReaderConfig(Configuration conf) {
    _conf = conf;
    _useParquetAvroRecordReader = conf.getBoolean(USE_PARQUET_AVRO_RECORDER_READER, true);
    _treatBinaryAsString = conf.getBoolean(TREAT_BINARY_AS_STRING, false);
  }

  public boolean useParquetAvroRecordReader() {
    return _useParquetAvroRecordReader;
  }

  public void setUseParquetAvroRecordReader(boolean useParquetAvroRecordReader) {
    _useParquetAvroRecordReader = useParquetAvroRecordReader;
  }

  public boolean isTreatBinaryAsString() {
    return _treatBinaryAsString;
  }

  public void setTreatBinaryAsString(boolean treatBinaryAsString) {
    _treatBinaryAsString = treatBinaryAsString;
  }

  public Configuration getConfig() {
    return _conf;
  }
}
