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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.JsonUtils;


public class RecordReaderFactory {
  private RecordReaderFactory() {
  }

  private static final Map<String, String> DEFAULT_RECORD_READER_CLASS_MAP = new HashMap<>();
  private static final Map<String, String> DEFAULT_RECORD_READER_CONFIG_CLASS_MAP = new HashMap<>();

  // TODO: This could be removed once we have dynamic loading plugins supports.
  private static final String DEFAULT_AVRO_RECORD_READER_CLASS =
      "org.apache.pinot.plugin.inputformat.avro.AvroRecordReader";
  private static final String DEFAULT_CSV_RECORD_READER_CLASS =
      "org.apache.pinot.plugin.inputformat.csv.CSVRecordReader";
  private static final String DEFAULT_CSV_RECORD_READER_CONFIG_CLASS =
      "org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig";
  private static final String DEFAULT_JSON_RECORD_READER_CLASS =
      "org.apache.pinot.plugin.inputformat.json.JSONRecordReader";
  private static final String DEFAULT_THRIFT_RECORD_READER_CLASS =
      "org.apache.pinot.plugin.inputformat.thrift.ThriftRecordReader";
  private static final String DEFAULT_THRIFT_RECORD_READER_CONFIG_CLASS =
      "org.apache.pinot.plugin.inputformat.thrift.ThriftRecordReaderConfig";
  private static final String DEFAULT_ORC_RECORD_READER_CLASS =
      "org.apache.pinot.plugin.inputformat.orc.ORCRecordReader";
  private static final String DEFAULT_PARQUET_RECORD_READER_CLASS =
      "org.apache.pinot.plugin.inputformat.parquet.ParquetRecordReader";

  public static void register(String fileFormat, String recordReaderClassName, String recordReaderConfigClassName) {
    DEFAULT_RECORD_READER_CLASS_MAP.put(fileFormat.toUpperCase(), recordReaderClassName);
    DEFAULT_RECORD_READER_CONFIG_CLASS_MAP.put(fileFormat.toUpperCase(), recordReaderConfigClassName);
  }

  public static void register(FileFormat fileFormat, String recordReaderClassName, String recordReaderConfigClassName) {
    register(fileFormat.name(), recordReaderClassName, recordReaderConfigClassName);
  }

  static {
    register(FileFormat.AVRO, DEFAULT_AVRO_RECORD_READER_CLASS, null);
    register(FileFormat.GZIPPED_AVRO, DEFAULT_AVRO_RECORD_READER_CLASS, null);
    register(FileFormat.CSV, DEFAULT_CSV_RECORD_READER_CLASS, DEFAULT_CSV_RECORD_READER_CONFIG_CLASS);
    register(FileFormat.JSON, DEFAULT_JSON_RECORD_READER_CLASS, null);
    register(FileFormat.THRIFT, DEFAULT_THRIFT_RECORD_READER_CLASS, DEFAULT_THRIFT_RECORD_READER_CONFIG_CLASS);
    register(FileFormat.ORC, DEFAULT_ORC_RECORD_READER_CLASS, null);
    register(FileFormat.PARQUET, DEFAULT_PARQUET_RECORD_READER_CLASS, null);
  }

  /**
   * Construct a RecordReaderConfig instance from a given file path.
   *
   * @param recordReaderConfigClassName
   * @param readerConfigFile
   * @return
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public static RecordReaderConfig getRecordReaderConfigByClassName(String recordReaderConfigClassName,
      String readerConfigFile) throws IOException, ClassNotFoundException {
    return getRecordReaderConfigByClassName(recordReaderConfigClassName, new File(readerConfigFile));
  }

  /**
   * Construct a RecordReaderConfig instance from a given file.
   *
   * @param recordReaderConfigClassName
   * @param readerConfigFile
   * @return
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public static RecordReaderConfig getRecordReaderConfigByClassName(String recordReaderConfigClassName,
      File readerConfigFile) throws IOException, ClassNotFoundException {
    Class recordReaderConfigClass = PluginManager.get().loadClass(recordReaderConfigClassName);
    RecordReaderConfig recordReaderConfig =
        (RecordReaderConfig) JsonUtils.fileToObject(readerConfigFile, recordReaderConfigClass);
    return recordReaderConfig;
  }

  /**
   * Construct a RecordReaderConfig instance from a given file.
   *
   * @param fileFormat
   * @param readerConfigFile
   * @return a RecordReaderConfig instance
   * @throws Exception
   */
  public static RecordReaderConfig getRecordReaderConfig(FileFormat fileFormat, String readerConfigFile)
      throws Exception {
    String fileFormatKey = fileFormat.name().toUpperCase();
    if (DEFAULT_RECORD_READER_CONFIG_CLASS_MAP.containsKey(fileFormatKey)) {
      return getRecordReaderConfigByClassName(DEFAULT_RECORD_READER_CONFIG_CLASS_MAP.get(fileFormatKey),
          readerConfigFile);
    }
    throw new UnsupportedOperationException("No supported RecordReader found for file format - '" + fileFormat + "'");
  }

  /**
   * Creates a {@link RecordReaderConfig} instance using file format and reader config properties
   */
  public static RecordReaderConfig getRecordReaderConfig(FileFormat fileFormat, Map<String, String> configs)
      throws ClassNotFoundException, IOException {
    String readerConfigClassName = getRecordReaderConfigClassName(fileFormat.toString());
    if (readerConfigClassName != null) {
      JsonNode jsonNode = new ObjectMapper().valueToTree(configs);
      Class<?> clazz = PluginManager.get().loadClass(readerConfigClassName);
      return (RecordReaderConfig) JsonUtils.jsonNodeToObject(jsonNode, clazz);
    }
    return null;
  }

  /**
   * Constructs and initializes a RecordReader based on the given RecordReader class name and config.
   */
  public static RecordReader getRecordReaderByClass(String recordReaderClassName, File dataFile,
      Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig) throws Exception {
    RecordReader recordReader = PluginManager.get().createInstance(recordReaderClassName);
    recordReader.init(dataFile, fieldsToRead, recordReaderConfig);
    return recordReader;
  }

  /**
   * Constructs and initializes a RecordReader based on the given file format and RecordReader config.
   */
  public static RecordReader getRecordReader(FileFormat fileFormat, File dataFile, Set<String> fieldsToRead,
      @Nullable RecordReaderConfig recordReaderConfig) throws Exception {
    return getRecordReader(fileFormat.name(), dataFile, fieldsToRead, recordReaderConfig);
  }

  /**
   * Constructs and initializes a RecordReader based on the given file format and RecordReader config.
   */
  public static RecordReader getRecordReader(String fileFormat, File dataFile, Set<String> fieldsToRead,
      @Nullable RecordReaderConfig recordReaderConfig) throws Exception {
    String recordReaderClassName = DEFAULT_RECORD_READER_CLASS_MAP.get(fileFormat.toUpperCase());
    if (recordReaderClassName == null) {
      throw new UnsupportedOperationException("No supported RecordReader found for file format - '" + fileFormat + "'");
    }
    return getRecordReaderByClass(recordReaderClassName, dataFile, fieldsToRead, recordReaderConfig);
  }

  /**
   * Get registered RecordReader class name given a file format.
   *
   * @param fileFormatStr
   * @return recordReaderClassName
   */
  public static String getRecordReaderClassName(String fileFormatStr) {
    return DEFAULT_RECORD_READER_CLASS_MAP.get(fileFormatStr.toUpperCase());
  }

  /**
   * Get registered RecordReaderConfig class name given a file format.
   *
   * @param fileFormatStr
   * @return recordReaderConfigClassName
   */
  public static String getRecordReaderConfigClassName(String fileFormatStr) {
    return DEFAULT_RECORD_READER_CONFIG_CLASS_MAP.get(fileFormatStr.toUpperCase());
  }
}
