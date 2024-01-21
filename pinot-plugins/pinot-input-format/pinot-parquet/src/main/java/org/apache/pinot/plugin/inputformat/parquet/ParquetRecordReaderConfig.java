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

import org.apache.commons.configuration2.Configuration;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


/**
 * Config for ParquetRecordReader
 */
public class ParquetRecordReaderConfig implements RecordReaderConfig {
  private static final String USE_PARQUET_AVRO_RECORDER_READER = "useParquetAvroRecordReader";
  private static final String USE_PARQUET_NATIVE_RECORDER_READER = "useParquetNativeRecordReader";

  private boolean _useParquetAvroRecordReader;
  private boolean _useParquetNativeRecordReader;
  private Configuration _conf;

  public ParquetRecordReaderConfig() {
  }

  public ParquetRecordReaderConfig(Configuration conf) {
    _conf = conf;
    _useParquetAvroRecordReader = conf.getBoolean(USE_PARQUET_AVRO_RECORDER_READER, false);
    _useParquetNativeRecordReader = conf.getBoolean(USE_PARQUET_NATIVE_RECORDER_READER, false);
  }

  public boolean useParquetAvroRecordReader() {
    return _useParquetAvroRecordReader;
  }

  public boolean useParquetNativeRecordReader() {
    return _useParquetNativeRecordReader;
  }

  public void setUseParquetNativeRecordReader(boolean useParquetNativeRecordReader) {
    _useParquetNativeRecordReader = useParquetNativeRecordReader;
  }

  public void setUseParquetAvroRecordReader(boolean useParquetAvroRecordReader) {
    _useParquetAvroRecordReader = useParquetAvroRecordReader;
  }

  public Configuration getConfig() {
    return _conf;
  }
}
