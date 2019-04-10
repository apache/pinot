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
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RecordReaderFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordReaderFactory.class);

  private RecordReaderFactory() {
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
      RecordReader recordReader = (RecordReader) Class.forName(recordReaderPath).newInstance();
      recordReader.init(segmentGeneratorConfig);
      return recordReader;
    }

    switch (fileFormat) {
      case AVRO:
      case GZIPPED_AVRO:
        return new AvroRecordReader(dataFile, schema);
      case CSV:
        return new CSVRecordReader(dataFile, schema, (CSVRecordReaderConfig) segmentGeneratorConfig.getReaderConfig());
      case JSON:
        return new JSONRecordReader(dataFile, schema);
      case THRIFT:
        return new ThriftRecordReader(dataFile, schema,
            (ThriftRecordReaderConfig) segmentGeneratorConfig.getReaderConfig());
      // NOTE: PinotSegmentRecordReader does not support time conversion (field spec must match)
      case PINOT:
        return new PinotSegmentRecordReader(dataFile, schema, segmentGeneratorConfig.getColumnSortOrder());
      default:
        throw new UnsupportedOperationException("Unsupported input file format: " + fileFormat);
    }
  }
}
