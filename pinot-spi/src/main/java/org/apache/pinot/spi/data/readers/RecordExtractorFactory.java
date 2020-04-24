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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.utils.SchemaFieldExtractorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory class to provide the RecordExtractor
 */
public final class RecordExtractorFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordExtractorFactory.class);

  private static final Map<String, String> RECORD_EXTRACTOR_CONFIG_MAP = new HashMap<>();
  private static final String CSV_RECORD_EXTRACTOR = "org.apache.pinot.plugin.inputformat.csv.CSVRecordExtractor";
  private static final String THRIFT_RECORD_EXTRACTOR =
      "org.apache.pinot.plugin.inputformat.thrift.ThriftRecordExtractor";
  private static final String CSV_RECORD_EXTRACTOR_CONFIG =
      "org.apache.pinot.plugin.inputformat.csv.CSVRecordExtractorConfig";
  private static final String THRIFT_RECORD_EXTRACTOR_CONFIG =
      "org.apache.pinot.plugin.inputformat.thrift.ThriftRecordExtractorConfig";

  private static final String RECORD_EXTRACTOR_CONFIG_KEY = "recordExtractorClass";

  private RecordExtractorFactory() {

  }

  private static void register(String recordExtractorClass, String recordExtractorConfigClass) {
    RECORD_EXTRACTOR_CONFIG_MAP.put(recordExtractorClass, recordExtractorConfigClass);
  }

  static {
    register(CSV_RECORD_EXTRACTOR, CSV_RECORD_EXTRACTOR_CONFIG);
    register(THRIFT_RECORD_EXTRACTOR, THRIFT_RECORD_EXTRACTOR_CONFIG);
  }

  /**
   * Constructs the RecordExtractor for a RecordReader
   */
  public static RecordExtractor getRecordExtractor(RecordReader recordReader, RecordReaderConfig readerConfig,
      Schema schema)
      throws Exception {
    Set<String> sourceFields = SchemaFieldExtractorUtils.extract(schema);
    String recordExtractorClassName = recordReader.getRecordExtractorClassName();
    RecordExtractor recordExtractor = PluginManager.get().createInstance(recordExtractorClassName);
    RecordExtractorConfig recordExtractorConfig = getRecordExtractorConfig(recordExtractorClassName, readerConfig);
    recordExtractor.init(sourceFields, recordExtractorConfig);
    return recordExtractor;
  }

  /**
   * Constructs the RecordExtractor for a StreamMessageDecoder
   */
  public static RecordExtractor getRecordExtractor(StreamMessageDecoder decoder, Map<String, String> decoderProps,
      Schema schema) {
    RecordExtractor recordExtractor = null;
    try {
      String recordExtractorClassName = null;
      if (decoderProps != null) {
        recordExtractorClassName = decoderProps.get(RECORD_EXTRACTOR_CONFIG_KEY);
        if (recordExtractorClassName != null) {
          try {
            recordExtractor = PluginManager.get().createInstance(recordExtractorClassName);
          } catch (Exception e) {
            LOGGER.info("Could not create RecordExtractor using class name {}", recordExtractorClassName);
          }
        }
      }
      if (recordExtractor == null) {
        recordExtractorClassName = decoder.getRecordExtractorClassName();
        recordExtractor = PluginManager.get().createInstance(recordExtractorClassName);
      }
      RecordExtractorConfig recordExtractorConfig = getRecordExtractorConfig(recordExtractorClassName, decoderProps);
      Set<String> sourceFields = SchemaFieldExtractorUtils.extract(schema);
      recordExtractor.init(sourceFields, recordExtractorConfig);
    } catch (Exception e) {
      ExceptionUtils.rethrow(e);
    }
    return recordExtractor;
  }

  private static RecordExtractorConfig getRecordExtractorConfig(String recordExtractorClass,
      RecordReaderConfig readerConfig)
      throws Exception {
    String recordExtractorConfigClass = RECORD_EXTRACTOR_CONFIG_MAP.get(recordExtractorClass);
    if (recordExtractorConfigClass != null) {
      RecordExtractorConfig recordExtractorConfig = PluginManager.get().createInstance(recordExtractorConfigClass);
      recordExtractorConfig.init(readerConfig);
      return recordExtractorConfig;
    }
    return null;
  }

  private static RecordExtractorConfig getRecordExtractorConfig(String recordExtractorClass,
      Map<String, String> decoderProps)
      throws Exception {
    String recordExtractorConfigClass = RECORD_EXTRACTOR_CONFIG_MAP.get(recordExtractorClass);
    if (recordExtractorConfigClass != null) {
      RecordExtractorConfig recordExtractorConfig = PluginManager.get().createInstance(recordExtractorConfigClass);
      recordExtractorConfig.init(decoderProps);
      return recordExtractorConfig;
    }
    return null;
  }
}
