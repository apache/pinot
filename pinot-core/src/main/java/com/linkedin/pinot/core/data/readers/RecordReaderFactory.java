/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.readers;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import java.io.File;


public class RecordReaderFactory {
  private RecordReaderFactory() {
  }

  public static RecordReader getRecordReader(SegmentGeneratorConfig segmentGeneratorConfig) throws Exception {
    File dataFile = new File(segmentGeneratorConfig.getInputFilePath());
    Preconditions.checkState(dataFile.exists(), "Input file: " + dataFile.getAbsolutePath() + " does not exist");

    Schema schema = segmentGeneratorConfig.getSchema();
    FileFormat fileFormat = segmentGeneratorConfig.getFormat();
    switch (fileFormat) {
      case AVRO:
      case GZIPPED_AVRO:
        return new AvroRecordReader(dataFile, schema);
      case CSV:
        return new CSVRecordReader(dataFile, schema, (CSVRecordReaderConfig) segmentGeneratorConfig.getReaderConfig());
      case JSON:
        return new JSONRecordReader(dataFile, schema);
      case PINOT:
        return new PinotSegmentRecordReader(dataFile, schema);
      case THRIFT:
        return new ThriftRecordReader(dataFile, schema,(ThriftRecordReaderConfig)segmentGeneratorConfig.getReaderConfig());
      default:
        throw new UnsupportedOperationException("Unsupported input file format: " + fileFormat);
    }
  }
}
