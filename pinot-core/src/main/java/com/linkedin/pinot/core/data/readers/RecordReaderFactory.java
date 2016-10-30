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

import com.linkedin.pinot.core.data.extractors.FieldExtractor;
import com.linkedin.pinot.core.data.extractors.FieldExtractorFactory;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import java.io.File;


public class RecordReaderFactory {
  public static RecordReader get(final SegmentGeneratorConfig segmentCreationSpec) throws Exception {
    if (segmentCreationSpec.getFormat() == null) {
      throw new UnsupportedOperationException("No input format property!");
    }

    if (segmentCreationSpec.getFormat() == FileFormat.AVRO || segmentCreationSpec.getFormat() == FileFormat.GZIPPED_AVRO) {
      return new AvroRecordReader(FieldExtractorFactory.getPlainFieldExtractor(segmentCreationSpec), segmentCreationSpec.getInputFilePath());

    } else if (segmentCreationSpec.getFormat() == FileFormat.CSV) {
      return new CSVRecordReader(segmentCreationSpec.getInputFilePath(), segmentCreationSpec.getReaderConfig(),
          segmentCreationSpec.getSchema());

    } else if (segmentCreationSpec.getFormat() == FileFormat.JSON) {
      return new JSONRecordReader(segmentCreationSpec.getInputFilePath(), segmentCreationSpec.getSchema());
    } else if (segmentCreationSpec.getFormat() == FileFormat.PINOT) {
      return new PinotSegmentRecordReader(new File(segmentCreationSpec.getInputFilePath()));
    }

    throw new UnsupportedOperationException("Unsupported input format: " + segmentCreationSpec.getFormat());
  }

  public static RecordReader get(FileFormat format, String fileName, FieldExtractor extractor) throws Exception {
    if (format == FileFormat.AVRO || format == FileFormat.GZIPPED_AVRO) {
      return new AvroRecordReader(extractor, fileName);
    }
    return null;
  }
}
