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
package org.apache.pinot.core.data.readers;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RecordReaderFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordReaderFactory.class);
  private static final Map<FileFormat, String> DEFAULT_RECORD_READER_CLASS_MAP = new HashMap<>();

  private static final String DEFAULT_AVRO_RECORD_READER_CLASS = "org.apache.pinot.avro.data.readers.AvroRecordReader";
  private static final String DEFAULT_CSV_RECORD_READER_CLASS = "org.apache.pinot.csv.data.readers.CSVRecordReader";
  private static final String DEFAULT_JSON_RECORD_READER_CLASS = "org.apache.pinot.json.data.readers.JSONRecordReader";
  private static final String DEFAULT_THRIFT_RECORD_READER_CLASS =
      "org.apache.pinot.thrift.data.readers.ThriftRecordReader";
  private static final String DEFAULT_ORC_RECORD_READER_CLASS = "org.apache.pinot.orc.data.readers.ORCRecordReader";
  private static final String DEFAULT_PARQUET_RECORD_READER_CLASS =
      "org.apache.pinot.parquet.data.readers.ParquetRecordReader";

  static {
    DEFAULT_RECORD_READER_CLASS_MAP.put(FileFormat.AVRO, DEFAULT_AVRO_RECORD_READER_CLASS);
    DEFAULT_RECORD_READER_CLASS_MAP.put(FileFormat.GZIPPED_AVRO, DEFAULT_AVRO_RECORD_READER_CLASS);
    DEFAULT_RECORD_READER_CLASS_MAP.put(FileFormat.CSV, DEFAULT_CSV_RECORD_READER_CLASS);
    DEFAULT_RECORD_READER_CLASS_MAP.put(FileFormat.JSON, DEFAULT_JSON_RECORD_READER_CLASS);
    DEFAULT_RECORD_READER_CLASS_MAP.put(FileFormat.THRIFT, DEFAULT_THRIFT_RECORD_READER_CLASS);
    DEFAULT_RECORD_READER_CLASS_MAP.put(FileFormat.ORC, DEFAULT_ORC_RECORD_READER_CLASS);
    DEFAULT_RECORD_READER_CLASS_MAP.put(FileFormat.PARQUET, DEFAULT_PARQUET_RECORD_READER_CLASS);
  }

  private RecordReaderFactory() {
  }

  public static RecordReader getRecordReader(String recordReaderClassName, File dataFile, Schema schema,
      RecordReaderConfig config)
      throws Exception {
    RecordReader recordReader = (RecordReader) Class.forName(recordReaderClassName).newInstance();
    recordReader.init(dataFile, schema, config);
    return recordReader;
  }

  public static RecordReader getRecordReader(FileFormat fileFormat, File dataFile, Schema schema,
      RecordReaderConfig config)
      throws Exception {
    if (DEFAULT_RECORD_READER_CLASS_MAP.containsKey(fileFormat)) {
      return getRecordReader(DEFAULT_RECORD_READER_CLASS_MAP.get(fileFormat), dataFile, schema, config);
    }
    throw new UnsupportedOperationException("No supported RecordReader found for file format - '" + fileFormat + "'");
  }

  public static RecordReader getRecordReader(SegmentGeneratorConfig segmentGeneratorConfig)
      throws Exception {
    File dataFile = new File(segmentGeneratorConfig.getInputFilePath());
    Preconditions.checkState(dataFile.exists(), "Input file: " + dataFile.getAbsolutePath() + " does not exist");

    Schema schema = segmentGeneratorConfig.getSchema();
    FileFormat fileFormat = segmentGeneratorConfig.getFormat();
    String recordReaderPath = segmentGeneratorConfig.getRecordReaderPath();

    // Allow for instantiation general record readers from a record reader path passed into segment generator config
    // If this is set, this will override the file format
    if (recordReaderPath != null) {
      if (fileFormat != FileFormat.OTHER) {
        // NOTE: we currently have default file format set to AVRO inside segment generator config, do not want to break
        // this behavior for clients.
        LOGGER
            .warn("Using class: {} to read segment, ignoring configured file format: {}", recordReaderPath, fileFormat);
      }
      return getRecordReader(recordReaderPath, dataFile, schema, segmentGeneratorConfig.getReaderConfig());
    }

    switch (fileFormat) {
      case AVRO:
      case GZIPPED_AVRO:
      case CSV:
      case JSON:
      case THRIFT:
        return getRecordReader(fileFormat, dataFile, schema, segmentGeneratorConfig.getReaderConfig());
      // NOTE: PinotSegmentRecordReader does not support time conversion (field spec must match)
      case PINOT:
        return new PinotSegmentRecordReader(dataFile, schema, segmentGeneratorConfig.getColumnSortOrder());
      default:
        throw new UnsupportedOperationException("Unsupported input file format: " + fileFormat);
    }
  }
}
