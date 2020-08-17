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
package org.apache.pinot.spi.ingestion.batch.spec;

import java.io.Serializable;
import java.util.Map;


/**
 * RecordReaderSpec defines how to initialize a RecordReader.
 */
public class RecordReaderSpec implements Serializable {

  /**
   * Record data format, e.g. 'avro', 'parquet', 'orc', 'csv', 'json', 'thrift' etc.
   */
  private String _dataFormat;

  /**
   * Corresponding RecordReader class name.
   * E.g.
   *    org.apache.pinot.plugin.inputformat.avro.AvroRecordReader
   *    org.apache.pinot.plugin.inputformat.csv.CSVRecordReader
   *    org.apache.pinot.plugin.inputformat.parquet.ParquetRecordReader
   *    org.apache.pinot.plugin.inputformat.json.JSONRecordReader
   *    org.apache.pinot.plugin.inputformat.orc.ORCRecordReader
   *    org.apache.pinot.plugin.inputformat.thrift.ThriftRecordReader
   */
  private String _className;

  /**
   * Corresponding RecordReaderConfig class name, it's mandatory for CSV and Thrift file format.
   * E.g.
   *    org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig
   *    org.apache.pinot.plugin.inputformat.thrift.ThriftRecordReaderConfig
   */
  private String _configClassName;

  /**
   * Used to init RecordReaderConfig class name, this config is required for CSV and Thrift data format.
   */
  private Map<String, String> _configs;

  public String getDataFormat() {
    return _dataFormat;
  }

  /**
   * Record data format, e.g. 'avro', 'parquet', 'orc', 'csv', 'json', 'thrift' etc.
   * @param dataFormat
   */
  public void setDataFormat(String dataFormat) {
    _dataFormat = dataFormat;
  }

  public String getClassName() {
    return _className;
  }

  /**
   * Corresponding RecordReader class name.
   * E.g.
   *    org.apache.pinot.plugin.inputformat.avro.AvroRecordReader
   *    org.apache.pinot.plugin.inputformat.csv.CSVRecordReader
   *    org.apache.pinot.plugin.inputformat.parquet.ParquetRecordReader
   *    org.apache.pinot.plugin.inputformat.json.JSONRecordReader
   *    org.apache.pinot.plugin.inputformat.orc.ORCRecordReader
   *    org.apache.pinot.plugin.inputformat.thrift.ThriftRecordReader
   *
   * @param className
   */
  public void setClassName(String className) {
    _className = className;
  }

  public Map<String, String> getConfigs() {
    return _configs;
  }

  /**
   * Used to init RecordReaderConfig class name, this config is required for CSV and Thrift data format.
   *
   * @param configs
   */
  public void setConfigs(Map<String, String> configs) {
    _configs = configs;
  }

  public String getConfigClassName() {
    return _configClassName;
  }

  /**
   * Corresponding RecordReaderConfig class name, it's mandatory for CSV and Thrift file format.
   * E.g.
   *    org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig
   *    org.apache.pinot.plugin.inputformat.thrift.ThriftRecordReaderConfig
   *
   * @param configClassName
   */
  public void setConfigClassName(String configClassName) {
    _configClassName = configClassName;
  }
}
